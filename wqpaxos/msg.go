package wqpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Commit{})
	gob.Register(LeaderChange{})
	// gob.Register(UpdateBallot{})
}

// Prepare phase 1a
type Prepare struct {
	Key paxi.Key
	Ballot paxi.Ballot
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {key=%v, %v}", p.Key, p.Ballot)
}

// CommandBallot conbines each command with its ballot number
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

//字符串化commandBallot
func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Command, cb.Ballot)
}

// Promise phase 1b
type Promise struct {
	Key paxi.Key
	//p1b message的 ballot
	Ballot paxi.Ballot
	//发送p1b的node ID
	ID     paxi.ID               // from node id
	//已接受的command和 ballot
	Log    map[int]CommandBallot // uncommitted logs
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {key=%v,b=%v id=%s log=%v}", p.Key, p.Ballot, p.ID, p.Log)
}

// Accept phase 2a
type Accept struct {
	Key paxi.Key
	//p2b message的Ballot
	Ballot  paxi.Ballot
	//slot是log slot
	Slot    int
	//要发送的command
	Command paxi.Command
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {key=%d, b=%v s=%d cmd=%v}", a.Key, a.Ballot, a.Slot, a.Command)
}

// Accepted phase 2b
type Accepted struct {
	Key paxi.Key
	//accepted   message的ballot 
	Ballot paxi.Ballot
	//发送accepted message的 nodeID
	ID     paxi.ID // from node id
	//log slot
	Slot   int
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {key=%v, b=%v id=%s s=%d}", a.Key, a.Ballot, a.ID, a.Slot)
}

// Commit phase 3
type Commit struct {
	Key       paxi.Key
	Ballot    paxi.Ballot
	Slot      int
	Command   paxi.Command
	MaxBallot paxi.Ballot
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {key=%d, b=%v s=%d cmd=%v, maxb=%v}", c.Key, c.Ballot, c.Slot, c.Command, c.MaxBallot)
}

// LeaderChange switch leader
//key的Leader转换
type LeaderChange struct {
	Key    paxi.Key
	To     paxi.ID
	From   paxi.ID
	Ballot paxi.Ballot
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {key=%d, from=%s, to=%s, bal=%v}", l.Key, l.From, l.To, l.Ballot)
}

// //更新最高的有效ballot
// type UpdateBallot struct{
// 	Key    paxi.Key
// 	Ballot paxi.Ballot
// }

// func(u UpdateBallot) String() string{
// 	return fmt.Sprintf("UpdateBallot {key = %d, ballot = %d", u.Key, u.Ballot)
// }
