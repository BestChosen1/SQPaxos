package paxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot
}

//字符串化p1a
func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
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

// P1b promise message
type P1b struct {
	//p1b message的 ballot
	Ballot paxi.Ballot
	//发送p1b的node ID
	ID     paxi.ID               // from node id
	//已接受的command和 ballot
	Log    map[int]CommandBallot // uncommitted logs
}

//字符串化p1b
func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a accept message
type P2a struct {
	//p2b message的Ballot
	Ballot  paxi.Ballot
	//slot是log slot
	Slot    int
	//要发送的command
	Command paxi.Command
}


func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	//accepted   message的ballot 
	Ballot paxi.Ballot
	//发送accepted message的 nodeID
	ID     paxi.ID // from node id
	//log slot
	Slot   int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}
