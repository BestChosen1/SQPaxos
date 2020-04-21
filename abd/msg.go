package abd

import (
	"encoding/gob"

	"github.com/ailidani/paxi"
)

func init() {
	//Register记录方法参数的下层具体值的类型和其名称。该名称将用来识别发送或接受接口类型值时下层的具体类型。
	gob.Register(Get{})
	gob.Register(GetReply{})
	gob.Register(Set{})
	gob.Register(SetReply{})
}

//第一阶段由proposer发送的message内容
// Get message
type Get struct {
	ID  paxi.ID
	//commandID
	CID int
	Key paxi.Key
}

//第一阶段由acceptor发送到回复信息
// GetReply message returns value and version
type GetReply struct {
	ID      paxi.ID
	CID     int
	Key     paxi.Key
	Value   paxi.Value
	Version int
}

//第二阶段由proposer发起的accepte message内容
// Set message
type Set struct {
	ID      paxi.ID
	CID     int
	Key     paxi.Key
	Value   paxi.Value
	Version int
}

//第二阶段由acceptor发送的同意或者拒绝信息
// SetReply acknowledges a set operation, whether succeed or not
type SetReply struct {
	ID  paxi.ID
	CID int
	Key paxi.Key
}
