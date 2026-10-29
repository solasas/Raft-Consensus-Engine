package transport

import (
	"fmt"
	"sort"
	"time"
)

// Chaos is the control plane for a Switch: named fault-injection operations
// that edit the link matrix. Node code never sees it; it only sees the effects.
//
// Operations touch several matrix entries one after another, so a message
// racing with a call may observe a half-applied change. That is fine for
// fault injection, which is inherently racy with traffic.
type Chaos struct{ sw *Switch }

func NewChaos(sw *Switch) *Chaos { return &Chaos{sw: sw} }

// ---- hard partitions ----

// Partition splits the cluster into the given groups. Nodes in different groups
// cannot exchange messages in either direction; nodes in the same group can.
// Members not named in any group are each isolated on their own. It replaces
// the current set of blocked links.
func (c *Chaos) Partition(groups ...[]string) {
	group := map[string]int{}
	for i, g := range groups {
		for _, id := range g {
			group[id] = i
		}
	}
	members := c.sw.Members()
	next := len(groups)
	for _, m := range members {
		if _, ok := group[m]; !ok {
			group[m] = next
			next++
		}
	}
	for _, a := range members {
		for _, b := range members {
			if a == b {
				continue
			}
			blocked := group[a] != group[b]
			c.sw.UpdateLink(a, b, func(p *LinkPolicy) { p.Blocked = blocked })
		}
	}
}

// Isolate cuts node id off from everyone, in both directions.
func (c *Chaos) Isolate(id string) { c.setNodeBlocked(id, true) }

// Reconnect restores every link touching node id.
func (c *Chaos) Reconnect(id string) { c.setNodeBlocked(id, false) }

func (c *Chaos) setNodeBlocked(id string, blocked bool) {
	for _, m := range c.sw.Members() {
		if m == id {
			continue
		}
		c.sw.UpdateLink(id, m, func(p *LinkPolicy) { p.Blocked = blocked })
		c.sw.UpdateLink(m, id, func(p *LinkPolicy) { p.Blocked = blocked })
	}
}

// BlockOneWay stops traffic flowing from -> to while leaving to -> from open.
//
// Because an RPC is a round trip, BlockOneWay("B","A") means: A's requests
// reach B and are executed there, but B's replies never come back, and B's own
// requests to A are lost. A sees timeouts yet B's state still changes.
func (c *Chaos) BlockOneWay(from, to string) {
	c.sw.UpdateLink(from, to, func(p *LinkPolicy) { p.Blocked = true })
}

// UnblockOneWay reopens a link closed by BlockOneWay.
func (c *Chaos) UnblockOneWay(from, to string) {
	c.sw.UpdateLink(from, to, func(p *LinkPolicy) { p.Blocked = false })
}

// Heal lifts every hard partition. Latency and packet loss settings stay.
func (c *Chaos) Heal() { c.sw.ClearBlocked() }

// Reset returns the network to perfect: no partitions, delays or loss.
func (c *Chaos) Reset() { c.sw.Reset() }

// ---- latency ----

// SetLatency adds uniform [min,max] delay to the directed link from -> to.
func (c *Chaos) SetLatency(from, to string, min, max time.Duration) error {
	if err := checkLatency(min, max); err != nil {
		return err
	}
	c.sw.UpdateLink(from, to, func(p *LinkPolicy) { p.MinLatency, p.MaxLatency = min, max })
	return nil
}

// SetNodeLatency adds delay to every link into and out of node id, so any
// round trip involving it pays the cost on both legs.
func (c *Chaos) SetNodeLatency(id string, min, max time.Duration) error {
	if err := checkLatency(min, max); err != nil {
		return err
	}
	for _, m := range c.sw.Members() {
		if m == id {
			continue
		}
		c.sw.UpdateLink(id, m, func(p *LinkPolicy) { p.MinLatency, p.MaxLatency = min, max })
		c.sw.UpdateLink(m, id, func(p *LinkPolicy) { p.MinLatency, p.MaxLatency = min, max })
	}
	return nil
}

// SetGlobalLatency adds delay to every link, including ones not yet touched.
func (c *Chaos) SetGlobalLatency(min, max time.Duration) error {
	if err := checkLatency(min, max); err != nil {
		return err
	}
	c.sw.UpdateAll(func(p *LinkPolicy) { p.MinLatency, p.MaxLatency = min, max })
	return nil
}

// ---- packet loss ----

// SetDropRate loses the given fraction of messages on the directed link.
func (c *Chaos) SetDropRate(from, to string, rate float64) error {
	if err := checkRate(rate); err != nil {
		return err
	}
	c.sw.UpdateLink(from, to, func(p *LinkPolicy) { p.DropRate = rate })
	return nil
}

// SetNodeDropRate loses the given fraction of messages on every link into and
// out of node id.
func (c *Chaos) SetNodeDropRate(id string, rate float64) error {
	if err := checkRate(rate); err != nil {
		return err
	}
	for _, m := range c.sw.Members() {
		if m == id {
			continue
		}
		c.sw.UpdateLink(id, m, func(p *LinkPolicy) { p.DropRate = rate })
		c.sw.UpdateLink(m, id, func(p *LinkPolicy) { p.DropRate = rate })
	}
	return nil
}

// SetGlobalDropRate loses the given fraction of messages on every link.
func (c *Chaos) SetGlobalDropRate(rate float64) error {
	if err := checkRate(rate); err != nil {
		return err
	}
	c.sw.UpdateAll(func(p *LinkPolicy) { p.DropRate = rate })
	return nil
}

// ---- introspection ----

// Faults lists every link that is currently misbehaving, one line each and in a
// stable order, for test logs: "n1->n3: blocked", "default: drop=15%".
func (c *Chaos) Faults() []string {
	var out []string
	if d := c.sw.Default(); d != (LinkPolicy{}) {
		out = append(out, "default: "+d.String())
	}
	snap := c.sw.Snapshot()
	links := make([]Link, 0, len(snap))
	for l, p := range snap {
		if p != c.sw.Default() && p != (LinkPolicy{}) {
			links = append(links, l)
		}
	}
	sort.Slice(links, func(i, j int) bool {
		if links[i].From != links[j].From {
			return links[i].From < links[j].From
		}
		return links[i].To < links[j].To
	})
	for _, l := range links {
		out = append(out, fmt.Sprintf("%s->%s: %s", l.From, l.To, snap[l]))
	}
	return out
}

func checkLatency(min, max time.Duration) error {
	if min < 0 || max < min {
		return fmt.Errorf("transport: invalid latency range [%v, %v]", min, max)
	}
	return nil
}

func checkRate(rate float64) error {
	if rate < 0 || rate > 1 {
		return fmt.Errorf("transport: drop rate %v outside [0, 1]", rate)
	}
	return nil
}
