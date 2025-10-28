package wpaxos

import (
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

//adaptive是能stable leader 模式
var adaptive = flag.Bool("adaptive", true, "stable leader, if true paxos forward request to current leader")
//fz是能容忍故障的zone数
var Fz = flag.Int("Fz", 0, "F_z fault tolerent zones")
//fd是每个区域中能容忍故障的node数
var Fd = flag.Int("Fd", 1, "F_d fault tolerent nodes per zone")
// // 每个区域单次处理的最大请求数
// var nums = flag.Int("nums", 1000, "max requests operated per zone once")


// Replica is WPaxos replica node
type Replica struct {
	paxi.Node
	paxi map[paxi.Key]*kpaxos
	
	// n_request int
}

// NewReplica creates new Replica instance
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.paxi = make(map[paxi.Key]*kpaxos)
	// r.n_request = 0

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(paxi.Transaction{}, r.handleTransaction)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)
	r.Register(LeaderChange{}, r.handleLeaderChange)
	return r
}

//初始化key对应的KPaxos
func (r *Replica) init(key paxi.Key) {
	//不存在key对应的kpaxos
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = newKPaxos(key, r.Node)
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	key := m.Command.Key
	r.init(key)

	p := r.paxi[key]
	p.HandleRequest(m)
	// p.SetOpz(m.Command.ClientID.Zone())
	// if *adaptive {
	// 	if p.IsLeader() || p.Ballot() == 0 {
	// 		p.HandleRequest(m)
	// 		// // r.n_request++;
	// 		// // to := p.Hit(m.NodeID)
	// 		// // p.SetOpz(to.Zone())
	// 		// if(r.n_request > *nums){
	// 		// 	Max_Z := 0
	// 		// 	Max_N := 0
	// 		// 	for zone,num := range p.Opz() {
	// 		// 		if num > Max_N{
	// 		// 			Max_N = num
	// 		// 			Max_Z = zone
	// 		// 		}
	// 		// 	}
	// 		// 	aim := p.Hit(paxi.NewID(Max_Z,1))
	// 		// 	if aim != "" && aim.Zone() != r.ID().Zone() {
	// 		// 		p.Send(aim, LeaderChange{
	// 		// 			Key:    key,
	// 		// 			To:     aim,
	// 		// 			From:   r.ID(),
	// 		// 			Ballot: p.Ballot(),
	// 		// 		})
	// 		// 	}
	// 		// //to是m对应的leadership转移的目标nodeID
	// 		// to := p.Hit(m.NodeID)
	// 		// if to != "" && to.Zone() != r.ID().Zone() {
	// 		// 	p.Send(to, LeaderChange{
	// 		// 		Key:    key,
	// 		// 		To:     to,
	// 		// 		From:   r.ID(),
	// 		// 		Ballot: p.Ballot(),
	// 		// 	})
	// 		// }
	// 		} else {
	// 				go r.Forward(p.Leader(), m)
	// 		}
	// 	} else {
	// 			p.HandleRequest(m)
	// 	}
	// }
}

func (r *Replica) handleTransaction(m paxi.Transaction) {
	// TODO
}

func (r *Replica) handlePrepare(m Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].HandleP1b(m.P1b)
	// log.Debugf("Number of keys: %d", r.keys())
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].HandleP2b(m.P2b)
}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP3(m.P3)
}

func (r *Replica) handleLeaderChange(m LeaderChange) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.From, m, r.ID())
	p := r.paxi[m.Key]
	if m.Ballot == p.Ballot() && m.To == r.ID() {
		// log.Debugf("Replica %s : change leader of key %d\n", r.ID(), m.Key)
		p.P1a()
	}
}

// func (r *Replica) handleUpdateBallot(m UpdateBallot){
// 	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
// 	r.init(m.Key)
// 	p := r.paxi[m.Key];
// 	if p.B_max[m.key] < m.Ballot {
// 		p.B_max[m.key] = m.Ballot
// 	}
// }

func (r *Replica) keys() int {
	sum := 0
	for _, p := range r.paxi {
		if p.IsLeader() {
			sum++
		}
	}
	return sum
}
