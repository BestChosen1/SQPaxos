package wqpaxos

import (
	"strconv"
	"time"
	
	"github.com/ailidani/paxi"
	// "github.com/ailidani/paxi/log"
)

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

type wqpaxos struct{
	paxi.Node
	key paxi.Key
	//*paxos.Paxos
	paxi.Policy
	opz map[int]int
	
	//??
	config []paxi.ID

	B_max map[paxi.Key]paxi.Ballot

	//log集合：key是int类型，value是entry类型的
	log     map[int]*entry // log ordered by slot
	//下一个要执行的slot number（小于 ballot）
	execute int            // next execute slot number
	//发起Paxos的node是否是leader
	active  bool           // active leader 是否是leader
	//最高的ballot number
	ballot  paxi.Ballot    // highest ballot number
	//最高的slot number
	slot    int            // highest slot number

	quorum   *paxi.Quorum    // phase 1 quorum
	//阶段1挂起的请求
	requests []*paxi.Request // phase 1 pending requests

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	//答复提交
	ReplyWhenCommit bool
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
	p.B_max = make(map[paxi.Key]paxi.Ballot)


	p.log = make(map[int]*entry, paxi.GetConfig().BufferSize)//map初始大小为paxi.GetConfig().BufferSize
	p.slot = -1
	p.quorum = paxi.NewQuorum()
	p.requests = make([]*paxi.Request, 0)
	p.Q1 = Q1
	p.Q2 = Q2
	p.ReplyWhenCommit = false
	
	return p
}

// IsLeader indecates if this node is current leader
func (p *wqpaxos) IsLeader() bool {
	//p.active表示是否为active leader或者p.ballot.id()表示leaderID
	return p.active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot 
// p.ballot.ID 表示当前ballot 的leaderID
func (p *wqpaxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
//返回当前最高number的ballot内容
func (p *wqpaxos) Ballot() paxi.Ballot {
	return p.ballot
}

// SetActive sets current paxos instance as active leader
//设置当前paxos的node为leader
func (p *wqpaxos) SetActive(active bool) {
	p.active = active
}

// SetBallot sets a new ballot number
//设置当前最高ballot number p.ballot为b
func (p *wqpaxos) SetBallot(b paxi.Ballot) {
	p.ballot = b
}

// HandleRequest handles request and starts phase 1 or phase 2
func (p *wqpaxos) HandleRequest(r paxi.Request) {
	//log.Debugf("Replica %s received %v \n", p.ID(), r)
	//如果当前node不是leader，从p1开始
	if !p.active {
		//将请求r放入request 数组中
		p.requests = append(p.requests, &r)
		// current phase 1 pending
		//从phase 1开始
		if p.ballot.ID() != p.ID() {
			p.P1a()
		}
	} else {
		//从p2开始执行
		p.P2a(&r)
	}
}

//结点ID从0开始
func (p *wqpaxos) SQ2(b paxi.Ballot,Fz int,Fn int) []paxi.ID{
	var Q2 []paxi.ID
	//Q2使用的区域集合
	var zone []int
	
	config := paxi.GetConfig()
	npz := config.NPZ()
	
	b_N := b.N()
	startZoneID := b.ID().Zone()
	// startNodeID :=(1+(b_N-1)*(Fn+1)) % npz[startZoneID]
	
	//添加Fz+1个区域
	for i :=1; i <= Fz+1;i++{
		zone = append(zone,startZoneID)
		startZoneID++
		if startZoneID > config.Z(){
			startZoneID = startZoneID % config.Z()
		}
	}
	
	//在zone中的每个区域选择Fn+1个结点
	for _,z := range zone{
		startNodeID := (1+(b_N-1)*(Fn+1)) % npz[z]
		if startNodeID == 0 {
			startNodeID = npz[z]
		}
		nodeID := startNodeID
		for j := 1;j <= Fn+1;j++{
			Q2 = append(Q2,paxi.NewID(z,nodeID))
			nodeID++
			if nodeID > npz[z]{
				nodeID = nodeID % npz[z]
			}
		}
	}

	return Q2
}

func(p *wqpaxos) SQ1(pre paxi.Ballot,b paxi.Ballot,Fz int, Fn int) []paxi.ID{
	var Q1 []paxi.ID
	var zone []int
	
	config := paxi.GetConfig()
	npz := config.NPZ()
	
	b_N := b.N()
	startZoneID := b.ID().Zone()
	//startNodeID := (1+(b_N-1)*(npz[startZoneID]/2+1)) % npz[startZoneID]
	
	pre_N := pre.N()
	pre_startZoneID := pre.ID().Zone()
	// pre_startNodeID :=(1+(pre_N-1)*(Fn+1))% npz[pre_startZoneID]
	

	//添加之前提案使用过的Q2
	for i := 1; i <= Fz+1; i++ {
	  zone = append(zone,pre_startZoneID)
		pre_startZoneID++
		if pre_startZoneID > config.Z(){
			pre_startZoneID = pre_startZoneID % config.Z()
		}
	}
	for _ , z := range zone {
		pre_startNodeID := (1+(pre_N-1)*(Fn+1)) % npz[z]
		if pre_startNodeID == 0 {
			pre_startNodeID = npz[z]
		}
		nodeID := pre_startNodeID
		for j := 1; j <= npz[z]/2+1 ;j++{
			Q1 = append(Q1,paxi.NewID(z,nodeID))
			nodeID++
			if nodeID > npz[z]{
				nodeID = nodeID % npz[z]
			}
		}
	}

	
	
	// // 添加其余config.z/2-Fz个区域
	// if startZoneID - pre_startZoneID < Fz+1{
	// 	startZoneID = pre_startZoneID
	// }
	 
	
	for i := 1; i <= config.Z()/2 - Fz; i++{
		if startZoneID == pre.ID().Zone() {
				startZoneID = startZoneID + Fz + 1
				if startZoneID > config.Z() {
						startZoneID = startZoneID % config.Z()
				}
		}
		zone = append(zone,startZoneID)
		startNodeID := (1+(b_N-1)*(npz[startZoneID]/2+1)) % npz[startZoneID]
		if startNodeID == 0 {
			startNodeID = npz[startZoneID]
		}
		nodeID := startNodeID
		for j := 1;j <= npz[startZoneID]/2+1 ;j++{
			Q1 = append(Q1,paxi.NewID(startZoneID,nodeID))
			nodeID++
			if nodeID > npz[startZoneID]{
				nodeID =  nodeID % npz[startZoneID]
			}
		}
		startZoneID++
		if startZoneID > config.Z() {
			startZoneID = startZoneID % config.Z()
		}
	}

	return Q1
	
}

func (p *wqpaxos) P1a() {
	if p.active{
		return
	}
	//生成ballot number
	p.ballot.Next(p.ID())
	//形成quorum
	p.quorum.Reset()
	//把ID放入回复消息的quorum中
	p.quorum.ACK(p.ID())

	if p.B_max[p.key] == 0{
		p.B_max[p.key] = p.ballot
	}
	Q1 := p.SQ1(p.B_max[p.key], p.ballot, *fz, *fn)
	p.MulticastNode(Q1, Prepare{Key:p.key, Ballot: p.ballot})
}

// P2a starts phase 2 accept
// 第二阶段不生成新的ballot，新的ballot只用在第一阶段获取领导权
func (p *wqpaxos) P2a(r *paxi.Request) {
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
	m := Accept{
		Key:	p.key,
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	}
	Q2 := p.SQ2(p.ballot,*fz,*fn)
	p.MulticastNode(Q2, m)
	
}

// 副本 HandleP1a handles P1a message
func (p *wqpaxos) HandleP1a(m Prepare) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	//如果m的ballot比paxos中最大的ballot p.ballot还大,则进行以下步骤
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		//停止p操作
		p.active = false
		// TODO use BackOff time or forward
		// forward pending requests to new leader
		p.forward()
		// if len(p.requests) > 0 {
		// 	defer p.P1a()
		// }
	}

	//l是之前接受过的ballot信息集合
	//交接Ballot Slot Requests(Commands) Log 
	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		//如果p.log[s]为空或者log已经提交 跳过
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		//否则放入l[s]中
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}

	//回复p1b消息：ballot 、id、log
	p.Send(m.Ballot.ID(), Promise{
		Key:	p.key,
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

//更新
func (p *wqpaxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		//返回值e是entry
		if  e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
			}
		} else {
			p.log[s] = &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  false,
			}
		}
	}
}

// HandleP1b handles P1b message处理p1b消息
func (p *wqpaxos) HandleP1b(m Promise) {
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
				p.MulticastNode(Q2, Accept{
					Key:	p.key,
					Ballot:  p.ballot,
					Slot:    i,
					Command: p.log[i].command,
				})
			}
			// propose new commands
			for _, req := range p.requests {
				p.P2a(req)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}

// HandleP2a handles P2a message
func (p *wqpaxos) HandleP2a(m Accept) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	p.opz[m.Command.ClientID.Zone()]++
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)
		// update entry 
		//返回值e是entry
		if e, exists := p.log[m.Slot]; exists {
			//更新entry
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !e.command.Equal(m.Command) && e.request != nil {
					p.Forward(m.Ballot.ID(), *e.request)
					// p.Retry(*e.request)
					e.request = nil
				}
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command,
				commit:  false,
			}
		}
	}
	p.Send(m.Ballot.ID(), Accepted{
		Key:	p.key,
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

// HandleP2b handles P2b message
func (p *wqpaxos) HandleP2b(m Accepted) {
	// old message
	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	// reject message
	// node updates its ballot number and falls back to acceptor
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
			
			if m.Ballot > p.B_max[m.Key] {
				p.B_max[m.Key] = m.Ballot
			}

			p.Broadcast(Commit{
				Key:	 p.key,	
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: p.log[m.Slot].command,
				MaxBallot: p.B_max[m.Key],
			})
		
			// p.Broadcast(UpdateBallot{
			// 	Key: 	m.Key,
			// 	Ballot:	p.B_max[m.Key],
			// })

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

// HandleP3 handles phase 3 commit message
func (p *wqpaxos) HandleP3(m Commit) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	p.slot = paxi.Max(p.slot, m.Slot)

	e, exist := p.log[m.Slot]
	if exist {
		if !e.command.Equal(m.Command) && e.request != nil {
			// p.Retry(*e.request)
			p.Forward(m.Ballot.ID(), *e.request)
			e.request = nil
		}
	} else {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true

	if m.MaxBallot > p.B_max[m.Key] {
		p.B_max[m.Key] = m.MaxBallot
	}

	if p.ReplyWhenCommit {
		if e.request != nil {
			e.request.Reply(paxi.Reply{
				Command:   e.request.Command,
				Timestamp: e.request.Timestamp,
			})
		}
	} else {
		p.exec()
	}
}


//问题怎么确定函数输入参数Q1，Q2
func (p *wqpaxos) MulticastNode(quorum []paxi.ID,m interface{}){
	switch m := m.(type) {
	case Prepare:
		p.Node.MulticastNode(quorum, m)
	case Accept:
		p.Node.MulticastNode(quorum, m)
	default:
		p.Node.Broadcast(m)
	}
}
func (p *wqpaxos) Broadcast(m interface{}) {
	switch m := m.(type) {
	case Commit:	
		p.Node.Broadcast(m)
	default:
		p.Node.Broadcast(m)
	}
}
func (p *wqpaxos) Send(to paxi.ID, m interface{}) {
	switch m := m.(type) {
	case Promise:
		p.Node.Send(to, m)
	case Accepted:
		p.Node.Send(to, m)
	default:
		p.Node.Send(to, m)
	}
}
func (p *wqpaxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		// log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		value := p.Execute(e.command)
		if e.request != nil {
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
}

func (p *wqpaxos) forward() {
	for _, m := range p.requests {
		p.Forward(p.ballot.ID(), *m)
	}
	p.requests = make([]*paxi.Request, 0)
}
