package blockchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"math"

	"github.com/ailidani/paxi/log"
)

//前缀是哈希值中作为工作证明的前导零点
// PREFIX is the leading zeros in hash value as proof of work
var PREFIX = []byte{0, 0, 0, 0}

func init() {
	gob.Register(Block{})
}

//Block中包含一些区块链中的数据
// Block contains some amount of data in blockchain
type Block struct {
	Index uint64
	Nonce uint64	//?
	Data  []byte	//存储的数据
	Prev  []byte // previous block hash 前一个区块的hash值
	Hash  []byte // current block hash	当前区块链的hash值

	next *Block	//后继区块链
}

//产生下一个区块链
func (b *Block) Next(data []byte) *Block {
	next := &Block{
		Index: b.Index + 1,
		Data:  data,
		Prev:  b.Hash,
	}
	next.mine()
	b.next = next
	return b.next
}

//
func (b *Block) mine() {
	//
	log.Debugf("start mining block %d", b.Index)
	h := sha256.New()
	h.Write(b.bytes())
	for i := uint64(0); i <= math.MaxUint64; i++ {
		t := h
		err := binary.Write(t, binary.LittleEndian, i)
		if err != nil {
			log.Error("binary write failed: ", err)
			break
		}
		thash := t.Sum(nil)
		if bytes.HasPrefix(thash, PREFIX) {
			b.Nonce = i
			b.Hash = thash
			log.Debugf("block %d nonce found %d", b.Index, b.Nonce)
			return
		}
	}
	log.Errorf("Cannot find nonce for block %d", b.Index)
}

func (b *Block) bytes() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, b.Index)
	if err != nil {
		log.Error("binary write failed: ", err)
		return nil
	}
	buf.Write(b.Data)
	buf.Write(b.Prev)
	return buf.Bytes()
}

// Genesis gets the genesis block which has byte[1024] data byte[256] prev hash
func Genesis() *Block {
	data := make([]byte, 1024)
	prev := make([]byte, 256)
	b := &Block{
		Index: 0,
		Data:  data,
		Prev:  prev,
	}
	b.mine()
	return b
}
