


## What is Paxi?

**Paxi** is the framework that implements WPaxos and other Paxos protocol variants. Paxi provides most of the elements that any Paxos implementation or replication protocol needs, including network communication, state machine of a key-value store, client API and multiple types of quorum systems.

*Warning*: Paxi project is still under heavy development, with more features and protocols to include. Paxi API may change too.



# How to build

1. Install [Go](https://golang.org/dl/).
2. Use `go get` command or [Download](https://github.com/wpaxos/paxi/archive/master.zip) Paxi source code from GitHub page.
```
go get github.com/ailidani/paxi
```

3. Compile everything from `paxi/bin` folder.
```
cd github.com/ailidani/paxi/bin
./build.sh
```

After compile, Golang will generate 3 executable files under `bin` folder.
* `server` is one replica instance.
* `client` is a simple benchmark that generates read/write reqeust to servers.
* `cmd` is a command line tool to test Get/Set requests.


# How to run

Each executable file expects some parameters which can be seen by `-help` flag, e.g. `./server -help`.

1. Generate the [configuration file](https://github.com/ailidani/paxi/blob/master/bin/config.json) according to the example, then start server with `-config FILE_PATH` option, default to "config.json" when omit.

2. Start 9 servers with different ids in format of "ZONE_ID.NODE_ID".
```
./server -id 1.1 -algorithm=paxos &
./server -id 1.2 -algorithm=paxos &
./server -id 1.3 -algorithm=paxos &
./server -id 2.1 -algorithm=paxos &
./server -id 2.2 -algorithm=paxos &
./server -id 2.3 -algorithm=paxos &
./server -id 3.1 -algorithm=paxos &
./server -id 3.2 -algorithm=paxos &
./server -id 3.3 -algorithm=paxos &
```

3. Start benchmarking client that connects to server ID 1.1 and benchmark parameters specified in [config.json](https://github.com/ailidani/paxi/blob/master/bin/config.json).
```
./client -id 1.1 -bconfig benchmark.json
```
When flag `id` is absent, client will randomly select any server for each operation.

The algorithms can also be running in **simulation** mode, where all nodes are running in one process and transport layer is replaced by Go channels. Check [`simulation.sh`](https://github.com/ailidani/paxi/blob/master/bin/simulation.sh) script on how to run.


# How to implement algorithms in Paxi

Replication algorithm in Paxi follows the message passing model, where several message types and their handle function are registered. We use [Paxos](https://github.com/ailidani/paxi/tree/master/paxos) as an example for our step-by-step tutorial.

1. Define messages, register with gob in `init()` function if using gob codec. As show in [`msg.go`](https://github.com/ailidani/paxi/blob/master/paxos/msg.go).

2. Define a `Replica` structure embeded with `paxi.Node` interface.
```go
type Replica struct {
	paxi.Node
	*Paxos
}
```

Define handle function for each message type. For example, to handle client `Request`
```go
func (r *Replica) handleRequest(m paxi.Request) {
	if *adaptive {
		if r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
			r.Paxos.HandleRequest(m)
		} else {
			go r.Forward(r.Paxos.Leader(), m)
		}
	} else {
		r.Paxos.HandleRequest(m)
	}

}
```

3. Register the messages with their handle function using `Node.Register(interface{}, func())` interface in `Replica` constructor.

Replica use `Send(to ID, msg interface{})`, `Broadcast(msg interface{})` functions in Node.Socket to send messages.

For data-store related functions check `db.go` file.

For quorum types check `quorum.go` file.

Client uses a simple RESTful API to submit requests. GET method with URL "http://ip:port/key" will read the value of given key. POST method with URL "http://ip:port/key" and body as the value, will write the value to key.
