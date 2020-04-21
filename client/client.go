package main

import (
	"encoding/binary"
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/chain"
	"github.com/ailidani/paxi/paxos"
)

//String用指定的名称、默认值、使用信息注册一个string类型flag。返回一个保存了该flag的值的指针\
//id:客户端连接的 node id
var id = flag.String("id", "", "node id this client connects to")
//algorithm:客户端API类型
var algorithm = flag.String("algorithm", "", "Client API type [paxos, chain]")
//Bool用指定的名称、默认值、使用信息注册一个bool类型flag。返回一个保存了该flag的值的指针。
//load:load key是否加入db中
var load = flag.Bool("load", false, "Load K keys into DB")
//mater:master 地址
var master = flag.String("master", "", "Master address.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	paxi.Client
}

func (d *db) Init() error {
	return nil
}

func (d *db) Stop() error {
	return nil
}

func (d *db) Read(k int) (int, error) {
	key := paxi.Key(k)
	//v 是byte 类型
	v, err := d.Get(key)
	if len(v) == 0 {
		return 0, nil
	}
	//字节转int
	x, _ := binary.Uvarint(v)
	return int(x), err
}

func (d *db) Write(k, v int) error {
	key := paxi.Key(k)
	value := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(value, uint64(v))
	err := d.Put(key, value)
	return err
}

func main() {
	paxi.Init()

	//连接master
	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	//新建DB，并选择DB的client模式（paxos或者chain）
	d := new(db)
	switch *algorithm {
	case "paxos":
		d.Client = paxos.NewClient(paxi.ID(*id))
	case "chain":
		d.Client = chain.NewClient()
	default:
		d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	}

	//设置DB的benchmark
	b := paxi.NewBenchmark(d)
	//加载key或者运行
	if *load {
		b.Load()
	} else {
		b.Run()
	}
}
