package wqpaxos

import (
	"strconv"
	"time"
	
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

type wqpaxos struct{
	paxi.Node
	key paxi.key
	*paxos.Paxos
	paxi.Policy
	B_max map[paxi.key]paxi.Ballot
	opz map[int]int
}

func Q1(q *paxi.Quorum) bool{
	return q.SGridQ1()
}

func Q2(q *paxi.Quorum) bool{
	return q.SGridQ2(*fz,*fn)
}

func NewWQPaxos(key paxi.Key,node paxi.Node) *wqpaxos{
	p := &wqpaxos{}
	p.Node = node
	p.key = key
	p.Policy = paxi.NewPolicy()
	p.opz = make(map[int]int)	//记录每个区域的操作频率
	
	quorum := func(p *paxos.Paxos){
		p.Q1 = Q1
		p.Q2 = Q2
	}
	p.Paxos = paxos.NewPaxos(p, quorum)
	return p
}

//结点ID从0开始
func (p *wqpaxos) SQ2(b paxi.Ballot,Fz int,Fn int) paxi.IDs{
	var smallQ2 []paxi.ID
	//Q2使用的区域集合
	var zone []int
	
	b_N := b.N()
	startZoneID := b.ID().zone()
	startNodeID :=(1+(b_N-1)*(Fz+1))%config.npz(startZoneID)
	
	//添加Fz+1个区域
	for i :=1; i <= Fz+1;i++{
		zone = append(zone,startZoneID % config.z)
		startZoneID++
	}
	
	//在zone中的每个区域选择Fn+1个结点
	for _,z := range zone{
		nodeID := startNodeID
		for j := 1;j <= Fn+1;j++{
			nodeID = nodeID % config.npz(z)
			smallQ2 = append(samllQ2,newID(z,nodeID))
			nodeID++
		}
	}
	return smallQ2
}

func(p *wqpaxos) SQ1(pre paxi.Ballot,b paxi.Ballot,Fz int) paxi.IDs{
	var smallQ1 []paxi.ID
	var zone []int
	
	pre_N := pre.N()
	pre_startZoneID := pre.ID().zone()
	pre_startNodeID :=(1+(pre_N-1)*(Fz+1))%config.npz(pre_startZoneID)
	
	b_N := b.N()
	startZoneID := b.ID().zone()
	startNodeID := (1+(b_N-1)*(config.npz(startZoneID)/2+1)) % config.npz(startZoneID)
	
	//添加之前提案使用过的Q2
	for i := 1; i <= Fz+1;i++{
		zone = append(zone,pre_startZoneID % config.z)
		pre_startZoneID++
	}
	for _,z := range zone{
		nodeID :=pre_startNodeID
		for j := 1;j <= config.npz(z)/2+1 ;j++{
			nodeID = nodeID % config.npz(z)
			smallQ1 = append(samllQ1,newID(z,nodeID))
			nodeID++
		}
	}
	
	//添加其余config.z-Fz个区域
	if startZoneID - pre_startZoneID < Fz+1{
		startZoneID = pre_startZoneID
	}
	
	for i := 1;i <=config.z/2 - Fz;i++{
		zone = append(zone,startZoneID % config.z)
		nodeID :=startNodeID
		for j := 1;j <= config.npz(z)/2+1 ;j++{
			nodeID = nodeID % config.npz(z)
			smallQ1 = append(samllQ1,newID(startZoneID,nodeID))
			nodeID++
		}
		startZoneID++
	}
	
	return smallQ1
	
}

func (p *wqpaxos) Handle_Request(r paxi.Request) {
	// log.Debugf("Replica %s received %v\n", p.ID(), r)
	//如果当前node不是leader，从p1开始
	if !p.active {
		//将请求r放入request 数组中
		p.requests = append(p.requests, &r)
		// current phase 1 pending
		//从phase 1开始
		if p.ballot.ID() != p.ID() {
			p.p1a()
		}
	} else {
		//从p2开始执行
		p.p2a(&r)
	}
}

func (p *wqpaxos) p1a() {
	if p.active {
		return
	}
	//生成ballot number
	p.ballot.Next(p.ID())
	//形成quorum
	p.quorum.Reset()
	//把ID放入回复消息的quorum中
	p.quorum.ACK(p.ID())
	//广播消息
	Q1 := p.SQ1(p.B_max,p.ballot,*fz)
	p.MulticastNode(Q1, P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *wqpaxos) p2a(r *paxi.Request) {
	//当前最高的slot number +1
	p.slot++
	//设置第slot个log的内容entry
	p.log[p.slot] = &entry{
		ballot:    p.ballot,
		//request r的command
		command:   r.Command,
		request:   r,
		//生成新的quorum
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
	}
	p.log[p.slot].quorum.ACK(p.ID())
	//生成p2a信息
	m := P2a{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	}
	Q2 := p.SQ2(p.ballot,*fz,*fn)
	p.MulticastNode(Q2, m)
	
}

// HandleP1b handles P1b message处理p1b消息
func (p *wqpaxos) handleP1b(m P1b) {
	// m是过时信息,old message m的ballot比已知的ballot小，或者p仍然是当前leader
	if m.Ballot < p.ballot || p.active {
		// log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	//更新m的log
	p.update(m.Log)

	// m是拒绝信息 reject message 如果m的ballot比p已知的最高ballot号高，则拒绝
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false // not necessary
		// forward pending requests to new leader
		p.forward()
		// p.P1a()
	}

	//m是接受信息 ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			p.active = true
			// propose any uncommitted entries
			for i := p.execute; i <= p.slot; i++ {
				// TODO nil gap?
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				Q2 := p.SQ2(p.ballot,*fz,*fn)
				p.MulticastNode(Q2, P2a{
					Ballot:  p.ballot,
					Slot:    i,
					Command: p.log[i].command,
				})
			}
			// propose new commands
			for _, req := range p.requests {
				p.p2a(req)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}
// HandleP2b handles P2b message
func (p *wqpaxos) handleP2b(m P2b) {
	// old message
	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
	}

	// ack message
	// the current slot might still be committed with q2
	// if no q2 can be formed, this slot will be retried when received p2a or p3
	if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
		p.log[m.Slot].quorum.ACK(m.ID)
		if p.Q2(p.log[m.Slot].quorum) {
			p.log[m.Slot].commit = true
			p.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: p.log[m.Slot].command,
			})
			if m.Ballot >B_max[p.key] {
				B_max[p.key] = m.Ballot
				p.Broadcast(UpdateBallot{
						Key: 	p.key
						Ballot:	m.Ballot
					})
			}

			if p.ReplyWhenCommit {
				r := p.log[m.Slot].request
				r.Reply(paxi.Reply{
					Command:   r.Command,
					Timestamp: r.Timestamp,
				})
			} else {
				p.exec()
			}
		}
	}
}

//问题怎么确定函数输入参数Q1，Q2
func (p *wqpaxos) MulticastNode(quorum []paxi.ID,m interface{}){
	switch m := m.(type) {
	case paxos.P1a:
		p.Node.MulticastNode(quorum,Prepare{p.key, m})
	case paxos.P2a:
		p.Node.MulticastNode(quorum,Accept{p.key, m})
	default:
		p.Node.Broadcast(m)
	}
}
func (p *wqpaxos) Broadcast(m interface{}) {
	switch m := m.(type) {
		p.Node.Broadcast(Commit{k.key, m})
	default:
		p.Node.Broadcast(m)
	}
}
func (p *wqpaxos) Send(to paxi.ID, m interface{}) {
	switch m := m.(type) {
	case paxos.P1b:
		p.Node.Send(to, Promise{p.key, m})
	case paxos.P2b:
		p.Node.Send(to, Accepted{p.key, m})
	default:
		p.Node.Send(to, m)
	}
}
