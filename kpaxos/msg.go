package kpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Commit{})
	gob.Register(LeaderChange{})
}

/**************************
 * Inter-Replica Messages *
 **************************/

// Prepare phase 1a
type Prepare struct {
	//prepare message内容：key、P1a（Ballot）
	//"P1a {b=%v}", m.Ballot
	Key paxi.Key
	paxos.P1a
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {key=%v, %v}", p.Key, p.P1a)
}

// Promise phase 1b
type Promise struct {
	//promise message内容：key、"P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log
	//m.ID：发送p1b的node ID；m.Log：未提交的日志内容
	Key paxi.Key
	paxos.P1b
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {key=%v, %v}", p.Key, p.P1b)
}

// Accept phase 2a
type Accept struct {
	//accept message内容：key、"P2a {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command
	//m.slot:log slot;m.Command:要发送的command
	Key paxi.Key
	paxos.P2a
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {key=%d, %v}", a.Key, a.P2a)
}

// Accepted phase 2b
type Accepted struct {
	//"P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot
	Key paxi.Key
	paxos.P2b
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {key=%v, %v}", a.Key, a.P2b)
}

// Commit phase 3
type Commit struct {
	//"P3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command
	Key paxi.Key
	paxos.P3
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {key=%d, %v}", c.Key, c.P3)
}

// LeaderChange switch leader
type LeaderChange struct {
	Key    paxi.Key
	To     paxi.ID
	From   paxi.ID
	Ballot paxi.Ballot
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {key=%d, from=%s, to=%s, bal=%d}", l.Key, l.From, l.To, l.Ballot)
}
