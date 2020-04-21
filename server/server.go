package main

import (
	"flag"	//解析包
	"sync" //同步包

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/abd"
	"github.com/ailidani/paxi/blockchain"
	"github.com/ailidani/paxi/chain"
	"github.com/ailidani/paxi/dynamo"
	"github.com/ailidani/paxi/epaxos"
	"github.com/ailidani/paxi/hpaxos"
	"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/m2paxos"
	"github.com/ailidani/paxi/paxos"
	"github.com/ailidani/paxi/paxos_group"
	"github.com/ailidani/paxi/sdpaxos"
	"github.com/ailidani/paxi/vpaxos"
	"github.com/ailidani/paxi/wankeeper"
	"github.com/ailidani/paxi/wpaxos"
	"github.com/ailidani/paxi/wqpaxos"
)

//flag.String函数：String用指定的名称、默认值、使用信息注册一个string类型flag
var algorithm = flag.String("algorithm", "paxos", "Distributed algorithm")
var id = flag.String("id", "", "ID in format of Zone.Node.")
//模拟模式
var simulation = flag.Bool("sim", false, "simulation mode")

var master = flag.String("master", "", "Master address.")

func replica(id paxi.ID) {
	if *master != "" {
		//连接到master
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("node %v starting...", id)

	switch *algorithm {

	case "paxos":
		paxos.NewReplica(id).Run()

	case "epaxos":
		epaxos.NewReplica(id).Run()

	case "sdpaxos":
		sdpaxos.NewReplica(id).Run()

	case "wpaxos":
		wpaxos.NewReplica(id).Run()

	case "abd":
		abd.NewReplica(id).Run()

	case "chain":
		chain.NewReplica(id).Run()

	case "vpaxos":
		vpaxos.NewReplica(id).Run()

	case "wankeeper":
		wankeeper.NewReplica(id).Run()

	case "kpaxos":
		kpaxos.NewReplica(id).Run()

	case "paxos_groups":
		paxos_group.NewReplica(id).Run()

	case "dynamo":
		dynamo.NewReplica(id).Run()

	case "blockchain":
		blockchain.NewMiner(id).Run()

	case "m2paxos":
		m2paxos.NewReplica(id).Run()

	case "hpaxos":
		hpaxos.NewReplica(id).Run()
		
	case "wqpaxos":
		wqpaxos.NewReplica(id).Run()
	default:
		panic("Unknown algorithm")
	}
}

func main() {
	paxi.Init()

	if *simulation {
		//WaitGroup用于等待一组线程的结束。父线程调用Add方法来设定应等待的线程的数量,每个被等待的线程在结束时应调用Done方法。同时，主线程里可以调用Wait方法阻塞至所有线程结束
		var wg sync.WaitGroup
		//父线程调用Add方法来设定应等待的线程的数量
		wg.Add(1)
		paxi.Simulation()
		for id := range paxi.GetConfig().Addrs {
			n := id
			go replica(n)
		}
		//主线程里可以调用Wait方法阻塞至所有线程结束
		wg.Wait()
	} else {
		replica(paxi.ID(*id))
	}
}
