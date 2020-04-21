package paxos

import (
	"flag"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var ephemeralLeader = flag.Bool("ephemeral_leader", false, "unstable leader, if true paxos replica try to become leader instead of forward requests to current leader")
var read = flag.String("read", "", "read from \"leader\", \"quorum\" or \"any\" replica")

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Paxos
}

// NewReplica generates new Paxos replica 输入参数id是 zone.node
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	//本次Paxos实例的发起结点Id==r.Node
	r.Node = paxi.NewNode(id)
	r.Paxos = NewPaxos(r)
	
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}



func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	//request m是read操作
	if m.Command.IsRead() && *read != "" {
		//返回值v和bool类型的状态
		v, inProgress := r.readInProgress(m)
		//回复m的reply信息
		reply := paxi.Reply{
			Command:    m.Command,
			Value:      v,
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		//reply属性信息
		reply.Properties[HTTPHeaderSlot] = strconv.Itoa(r.Paxos.slot)
		reply.Properties[HTTPHeaderBallot] = r.Paxos.ballot.String()
		reply.Properties[HTTPHeaderExecute] = strconv.Itoa(r.Paxos.execute - 1)
		reply.Properties[HTTPHeaderInProgress] = strconv.FormatBool(inProgress)
		m.Reply(reply)
		return
	}
	//是leader
	if *ephemeralLeader || r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
		//paxos处理request m
		r.Paxos.HandleRequest(m)
	} else {
		//不是leader 放入forward中等待执行
		go r.Forward(r.Paxos.Leader(), m)
	}
}

func (r *Replica) readInProgress(m paxi.Request) (paxi.Value, bool) {
	// TODO
	// (1) last slot is read?
	// (2) entry in log over writen
	// (3) value is not overwriten command 覆盖

	// is in progress
	//读取操作，从后往前读log，匹配当前key值的entry
	for i := r.Paxos.slot; i >= r.Paxos.execute; i-- {
		entry, exist := r.Paxos.log[i]
		if exist && entry.command.Key == m.Command.Key {
			return entry.command.Value, true
		}
	}

	// not in progress key 没有request m
	return r.Node.Execute(m.Command), false
}
