package paxos_group

import (
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/paxos"
)

var groups = flag.Int("groups", 5, "Number of Paxos groups")

type Replica struct {
	paxi.Node
	//r.paxi[gid] = paxos.NewPaxos(r.Node)
	paxi map[int]*paxos.Paxos

	gid int // current working group id
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.paxi = make(map[int]*paxos.Paxos)	//r.paxi[gid] = paxos.NewPaxos(r.Node)

	r.Register(paxi.Request{}, r.handleReqeust)

	return r
}

// static paxos groups
//index根据key计算相应的静态Paxos分组
func index(key paxi.Key) int {
	return int(key) % *groups
}

//用paxi获取gid对应的paxos
func (r *Replica) paxos(gid int) *paxos.Paxos {
	r.gid = gid
	if _, exists := r.paxi[gid]; !exists {
		r.paxi[gid] = paxos.NewPaxos(r.Node)
	}
	return r.paxi[gid]
}

func (r *Replica) handleReqeust(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	//获取key
	k := m.Command.Key
	//由key计算gid
	gid := index(k)
	//由gid得到相应的paxos
	p := r.paxos(gid)
	//若当前paxos是leader或者为本地消息，则直接处理request m
	if p.IsLeader() || p.Ballot() == 0 {
		p.HandleRequest(m)
	} else {
		//否则等待p获取到leader，处理m
		go r.Forward(p.Leader(), m)
	}
}

//副本r 处理prepare message
func (r *Replica) handlePrepare(m Prepare) {
	//副本m.ballot.ID()(leader) 发送请求m到 r.ID(处理request m)
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.paxos(m.GroupID).HandleP1a(m.P1a)
}

func (r *Replica) handlePromise(m Promise) {
	//
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxos(m.GroupID).HandleP1b(m.P1b)
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.paxos(m.GroupID).HandleP2a(m.P2a)
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	r.paxos(m.GroupID).HandleP2b(m.P2b)
}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, r.ID())
	r.paxos(m.GroupID).HandleP3(m.P3)
}

// Broadcast overrides Socket interface in Node
//leader广播message
func (r *Replica) Broadcast(msg interface{}) {
	switch m := msg.(type) {
	case paxos.P1a:
		r.Node.MulticastZone(r.ID().Zone(), Prepare{r.gid, m})
	case paxos.P2a:
		r.Node.MulticastZone(r.ID().Zone(), Accept{r.gid, m})
	case paxos.P3:
		r.Node.MulticastZone(r.ID().Zone(), Commit{r.gid, m})
	default:
		r.Node.Broadcast(msg)
	}
}

// Send overrides Socket interface in Node
//发送回复信息
func (r *Replica) Send(to paxi.ID, msg interface{}) {
	switch m := msg.(type) {
	case paxos.P1b:
		r.Node.Send(to, Promise{r.gid, m})
	case paxos.P2b:
		r.Node.Send(to, Accepted{r.gid, m})
	default:
		r.Node.Send(to, msg)
	}
}
