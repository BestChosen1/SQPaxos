package paxi

import (
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// Socket integrates all networking interface and fault injections
type Socket interface {

	// Send put message to outbound queue
	//将消息发送到outbound 队列中
	Send(to ID, m interface{})

	// MulticastZone send msg to all nodes in the same site
	//将消息广播到zone中
	MulticastZone(zone int, m interface{})

	// MulticastQuorum sends msg to random number of nodes
	//将msg发送到随机数量的node中
	MulticastQuorum(quorum int, m interface{})
	
	//将msg发送至特定ID集合
	MulticastNode(quorum IDs, m interface{})

	// Broadcast send to all peers
	//全网广播
	Broadcast(m interface{})

	// Recv receives a message
	//接收message
	Recv() interface{}

	Close()

	// Fault injection
	//故障中断
	Drop(id ID, t int)             // drops every message send to ID last for t seconds 将发送到ID的每封邮件保留t秒
	Slow(id ID, d int, t int)      // delays every message send to ID for d ms and last for t seconds 将每个发送到ID的消息延迟d ms并持续t秒
	Flaky(id ID, p float64, t int) // drop message by chance p for t seconds 偶然丢弃消息p，持续t秒
	Crash(t int)                   // node crash for t seconds 节点崩溃t秒
}

type socket struct {
	id        ID
	addresses map[ID]string
	nodes     map[ID]Transport

	crash bool
	drop  map[ID]bool
	slow  map[ID]int
	flaky map[ID]float64

	lock sync.RWMutex // locking map nodes
}

// NewSocket return Socket interface instance given self ID, node list, transport and codec name
func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := &socket{
		id:        id,
		addresses: addrs,
		nodes:     make(map[ID]Transport),
		crash:     false,
		drop:      make(map[ID]bool),
		slow:      make(map[ID]int),
		flaky:     make(map[ID]float64),
	}
	//id 赋值
	socket.nodes[id] = NewTransport(addrs[id])
	//ID上放监听器
	socket.nodes[id].Listen()

	return socket
}

func (s *socket) Send(to ID, m interface{}) {
	
	//结点s.id将消息m发送至to结点
	log.Debugf("node %s send message %+v to %v", s.id, m, to)

	//s是否故障
	if s.crash {
		return
	}

	//目标结点to是否故障，需要停滞接收的message t秒
	if s.drop[to] {
		return
	}

	//flaky故障
	if p, ok := s.flaky[to]; ok && p > 0 {
		if rand.Float64() < p {
			return
		}
	}

	//申请读锁
	s.lock.RLock()
	//transport=s.node[to]
	t, exists := s.nodes[to]
	//释放读锁
	s.lock.RUnlock()
	if !exists {
		//获取目标结点to的地址
		s.lock.RLock()
		address, ok := s.addresses[to]
		s.lock.Unlock()
		if !ok {
			log.Errorf("socket does not have address of node %s", to)
			return
		}
		//为目标结点to的地址新建transport
		t = NewTransport(address)
		err := Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
		if err != nil {
			panic(err)
		}
		s.lock.Lock()
		s.nodes[to] = t
		s.lock.Unlock()
	}

	//slow方法实现：个timer时间后发送信息m
	if delay, ok := s.slow[to]; ok && delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		go func() {
			<-timer.C
			t.Send(m)
		}()
		
		return
	}

	t.Send(m)
}

//获取s.id结点收到的信息m
func (s *socket) Recv() interface{} {
	s.lock.RLock()
	t := s.nodes[s.id]
	s.lock.RUnlock()
	for {
		m := t.Recv()
		if !s.crash {
			return m
		}
	}
}

//广播消息到指定zone
func (s *socket) MulticastZone(zone int, m interface{}) {
	//log.Debugf("node %s broadcasting message %+v in zone %d", s.id, m, zone)
	for id := range s.addresses {
		//不用给本节点s.id发送消息
		if id == s.id {
			continue
		}
		if id.Zone() == zone {
			s.Send(id, m)
		}
	}
}

//给quorum个结点发送message
func (s *socket) MulticastQuorum(quorum int, m interface{}) {
	//log.Debugf("node %s multicasting message %+v for %d nodes", s.id, m, quorum)
	i := 0
	//s.addresses中随机选择quorum个结点发送消息m
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
		i++
		if i == quorum {
			break
		}
	}
}

//**发送消息至特定结点集
func(s *socket) MulticastNode(quorum IDs,m interface{}){
	for id := quorum {
		if id == s.id{
			continue
		}
		s.Send(id, m)
	}
}

//给所有s.addresses中的结点发送消息
func (s *socket) Broadcast(m interface{}) {
	//log.Debugf("node %s broadcasting message %+v", s.id, m)
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
	}
}

//关闭s中的所有结点
func (s *socket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}

//Drop方法：ID结点node等待timer时间，s.drop[id]是bool型
func (s *socket) Drop(id ID, t int) {
	s.drop[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.drop[id] = false
	}()
}

//Slow方法：s.slow[id]是int型，ID结点等待timer时间
func (s *socket) Slow(id ID, delay int, t int) {
	s.slow[id] = delay
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.slow[id] = 0
	}()
}

//flaky方法：s.flaky[id]是float型，ID结点等待timer时间
func (s *socket) Flaky(id ID, p float64, t int) {
	s.flaky[id] = p
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.flaky[id] = 0
	}()
}

//Crash方法：s.crash是bool型，未指定的结点待timer时间
func (s *socket) Crash(t int) {
	s.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			s.crash = false
		}()
	}
}
