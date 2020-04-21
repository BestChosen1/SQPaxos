package wqpaxos

import (
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/paxos"
)
var adaptive = flag.Bool("adaptive", true, "stable leader, if true paxos forward request to current leader")
//fz是能容忍故障的zone数
var fz = flag.Int("fz", 0, "f_z fault tolerent zones")
var fn = flag.Int("fn",0,"f_n fault tolerent nodes per zone")
var nums = flag.Int("opt", 0, "slot number per region operated")

type Replica struct{
	paxi.Node
	paxi map[paxi.key]*wqpaxos
	n_request int
}

func NewReplica(id Paxi.ID) *Replica{
	r :=new(Replica)
	r.Node = paxi.NewNode(id)
	r.paxi = make(map[paxi.key]*wqpaxos)
	
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(paxi.Transaction{}, r.handleTransaction)
	r.Register(Prepare{}, r.handlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(Commit{}, r.handleCommit)
	r.Register(LeaderChange{}, r.handleLeaderChange)
	r.Register(UpdateBallot{},r.handleUpdateBallot)
	return r
}

//初始化key对应的KPaxos
func (r *Replica) init(key paxi.Key) {
	//存在key对应的的kpaxos
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = newWQPaxos(key, r.Node)
	}
}

func(r *Replica) handleRequest(m paxi.Request){
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	key := m.Command.Key
	r.init(key)

	p := r.paxi[key]
	if *adaptive {
		if p.IsLeader() || p.Ballot() == 0 {
			p.Handle_Request(m)
			n_request++
			to := p.Hit(m.NodeID)
			p.opz[to.Zone()]++
			if(n_request > *nums){
				Max_Z := 0
				Max_N := 0
				for zone,num := range p.opz{
					if num > Max_N{
						Max_N = num
						Max_Z = zone
					}
				}
				aim := p.Hit(NewID(Max_Z,1))
				if aim != "" && aim.Zone() != r.ID().Zone() {
				p.Send(aim, LeaderChange{
					Key:    key,
					To:     aim,
					From:   r.ID(),
					Ballot: p.Ballot(),
				})
			}
			}
		}else {
				go r.Forward(p.Leader(), m)
		}	 
	} else {
		p.Handle_Request(m)
	}
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
	r.paxi[m.Key].handleP1b(m.P1b)
	// log.Debugf("Number of keys: %d", r.keys())
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	r.paxi[m.Key].HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxi[m.Key].handleP2b(m.P2b)
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
func (r *Replica) handleUpdateBallot(m UpdateBallot){
	
}

func (r *Replica) keys() int {
	sum := 0
	for _, p := range r.paxi {
		if p.IsLeader() {
			sum++
		}
	}
	return sum
}
