


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
