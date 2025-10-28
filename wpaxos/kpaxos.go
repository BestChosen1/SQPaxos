package wpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

type kpaxos struct {
	//interface 不用*   struct用*
	paxi.Node
	key paxi.Key
	*paxos.Paxos
	paxi.Policy
	// B_max map[paxi.Key]paxi.Ballot
}

// func Q1(q *paxi.Quorum) bool {
// 	// if *Fz == 0 {
// 	// 	return q.GridRow()
// 	// }
// 	return q.FGridQ1(*Fz, *Fd)
// }

// func Q2(q *paxi.Quorum) bool {
// 	// if *Fz == 0 {
// 	// 	return q.GridColumn()
// 	// }
// 	return q.FGridQ2(*Fz, *Fd)
// }

// Multi-Paxos
func Q12(q *paxi.Quorum) bool {
	return q.SGridQ1()
}

// // DPaxos and SQPaxos
// func Q1(q *paxi.Quorum) bool {
// 	return q.SGridQ1()
// }

// func Q2(q *paxi.Quorum) bool {
// 	// if *Fz == 0 {
// 	// 	return q.GridColumn()
// 	// }
// 	return q.SGridQ2(*Fz, *Fd)
// }



func newKPaxos(key paxi.Key, node paxi.Node) *kpaxos {
	k := &kpaxos{}
	k.Node = node
	k.key = key
	k.Policy = paxi.NewPolicy()
//	k.B_max = make(map[paxi.Key]paxi.Ballot)

	quorum := func(p *paxos.Paxos) {
		p.Q1 = Q12
		p.Q2 = Q12
	}
	k.Paxos = paxos.NewPaxos(k, quorum)

	// zone := int(key)%paxi.GetConfig().Z() + 1
	// id := paxi.NewID(zone, 1)
	// k.Paxos.SetBallot(paxi.NewBallot(1, id))
	// if node.ID() == id {
	// 	k.Paxos.SetActive(true)
	// }
	return k
}

// Broadcast overrides Socket interface in Node
func (k *kpaxos) Broadcast(m interface{}) {
	switch m := m.(type) {
	case paxos.P1a:
		k.Node.Broadcast(Prepare{k.key, m})
	case paxos.P2a:
		k.Node.Broadcast(Accept{k.key, m})
	case paxos.P3:
		k.Node.Broadcast(Commit{k.key, m})
	default:
		k.Node.Broadcast(m)
	}
}


// MulticastNode overrides Socket interface in Node
func (k *kpaxos) MulticastNode(quorum []paxi.ID, m interface{}) {
	switch m := m.(type) {
	case paxos.P1a:
		k.Node.MulticastNode(quorum, Prepare{k.key, m})
	case paxos.P2a:
		k.Node.MulticastNode(quorum, Accept{k.key, m})
	case paxos.P3:
		k.Node.MulticastNode(quorum, Commit{k.key, m})
	default:
		k.Node.MulticastNode(quorum, m)
	}
}

// // MulticastQuorum overrides Socket interface in Node
// func (k *kpaxos) MulticastQuorum(quorum int, m interface{}) {
// 	switch m := m.(type) {
// 	case paxos.P1a:
// 		k.Node.MulticastQuorum(quorum, Prepare{k.key, m})
// 	case paxos.P2a:
// 		k.Node.MulticastQuorum(quorum, Accept{k.key, m})
// 	case paxos.P3:
// 		k.Node.MulticastQuorum(quorum, Commit{k.key, m})
// 	default:
// 		k.Node.MulticastQuorum(quorum, m)
// 	}
// }


// Send overrides Socket interface in Node
func (k *kpaxos) Send(to paxi.ID, m interface{}) {
	switch m := m.(type) {
	case paxos.P1b:
		k.Node.Send(to, Promise{k.key, m})
	case paxos.P2b:
		k.Node.Send(to, Accepted{k.key, m})
	default:
		k.Node.Send(to, m)
	}
}



