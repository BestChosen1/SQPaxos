package paxi

import (
	"encoding/gob"
	"fmt"
)

func init() {
	//Register(value interface{})   记录value下层具体值的类型和其名称,该名称将用来识别发送或接受接口类型值时下层的具体类型
	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(Transaction{})
	gob.Register(TransactionReply{})
	gob.Register(Register{})
	gob.Register(Config{})
}

/***************************
 * Client-Replica Messages *
 ***************************/

// Request is client reqeust with http response channel
//request是有HTTP channel的客户端请求
type Request struct {
	Command    Command
	Properties map[string]string
	Timestamp  int64
	NodeID     ID         // forward by node
	c          chan Reply // reply channel created by request receiver
}

// Reply replies to current client session 
//reply方法：将参数reply放到request的 回复channel c中
func (r *Request) Reply(reply Reply) {
	r.c <- reply
}

//request字符串化：返回request的command内容和 request的node ID
func (r Request) String() string {
	return fmt.Sprintf("Request {cmd=%v nid=%v}", r.Command, r.NodeID)
}

// Reply includes all info that might replies to back the client for the coresponding reqeust
//Reply结构体包括：服务给client的所有可能的信息内容 
type Reply struct {
	Command    Command
	Value      Value
	Properties map[string]string
	Timestamp  int64
	Err        error
}

//Reply信息字符串化：返回command、value、properties（配置）
func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v value=%x prop=%v}", r.Command, r.Value, r.Properties)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
//READ 结构体：一种特殊的request，可以在不经过副本协议的基础上直接读取key的值value
type Read struct {
	CommandID int
	Key       Key
}

//READ结构体的字符串化：返回命令ID和 read.key
func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
//ReadReply结构体:command ID , value
type ReadReply struct {
	CommandID int
	Value     Value
}

//ReadReply的字符串化：返回commandID和value
func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%x}", r.CommandID, r.Value)
}

// Transaction contains arbitrary number of commands in one request
// TODO read-only or write-only transactions
//Transaction结构体：command组，时间戳，transactionReply通道c
type Transaction struct {
	Commands  []Command
	Timestamp int64

	c chan TransactionReply
}

// Reply replies to current client session
//Reply方法：将TransactionReply类型的参数r放入transAction的回复通道t.c中
func (t *Transaction) Reply(r TransactionReply) {
	t.c <- r
}

//Transaction字符串化：返回结构体中的command
func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {cmds=%v}", t.Commands)
}

// TransactionReply is the result of transaction struct
//transactionReply结构体：bool类型的OK， command数组，时间戳，错误
type TransactionReply struct {
	OK        bool
	Commands  []Command
	Timestamp int64
	Err       error
}

/**************************
 *     Config Related     *
 **************************/

// Register message type is used to regitster self (node or client) with master node
//Register结构体：Register消息类型用于向master节点注册self（节点或客户机），包括：bool类型的client，ID ，地址
type Register struct {
	Client bool
	ID     ID
	Addr   string
}
