package wpaxos
//跟KPaxos的msg.go一样
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


//phase 1a
type Prepare struct {
	Key paxi.Key
	paxos.P1a
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {key=%v, %v}", p.Key, p.P1a)
}

//phase 1b
type Promise struct {
	Key paxi.Key
	paxos.P1b
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {key=%v, %v}", p.Key, p.P1b)
}

// phase 2a
type Accept struct {
	Key paxi.Key
	paxos.P2a
}

func (p Accept) String() string {
	return fmt.Sprintf("Propose {key=%d, %v}", p.Key, p.P2a)
}

// phase 2b
type Accepted struct {
	Key paxi.Key
	paxos.P2b
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {key=%v, %v}", a.Key, a.P2b)
}

// phase 3
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


// //更新最高的有效ballot
// type UpdateBallot struct{
// 	Key    paxi.Key
// 	Ballot paxi.Ballot
// }

// func(u UpdateBallot) String() string{
// 	return fmt.Sprintf("UpdateBallot {key = %d, ballot = %d", u.Key, u.Ballot)
// }
