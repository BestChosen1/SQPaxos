package paxi

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// DB is general interface implemented by client to call client library
//DB是客户端实现的调用客户端库的通用接口
type DB interface {
	Init() error
	Read(key int) (int, error)
	Write(key, value int) error
	Stop() error
}

// Bconfig holds all benchmark configuration
//Bconfig保存所有基准配置
type Bconfig struct {
	T                    int     // total number of running time in seconds 运行总时间（秒）
	N                    int     // total number of requests 请求数量
	K                    int     // key sapce key空间
	W                    float64 // write ratio 写率
	Throttle             int     // requests per second throttle, unused if 0. 每秒请求数限制,0表示未使用
	Concurrency          int     // number of simulated clients. 模拟客户端数
	Distribution         string  // distribution
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark 在基准测试结束时运行线性化能力检查程序
	// rounds       int    // repeat in many rounds sequentially

	// conflict distribution 冲突分布
	Conflicts int // percentage of conflicting keys 冲突密钥的百分比
	Min       int // min key 最小键

	// normal distribution 正态分布
	Mu    float64 // mu of normal distribution 正态分布μ
	Sigma float64 // sigma of normal distribution 正态分布sigma
	Move  bool    // moving average (mu) of normal distribution正态分布的移动平均数
	Speed int     // moving speed in milliseconds intervals per key 移动速度，以毫秒为单位，每键间隔

	// zipfian distribution 齐普法分布
	ZipfianS float64 // zipfian s parameter  齐普法s参数
	ZipfianV float64 // zipfian v parameter  齐普法v参数

	// exponential distribution 指数分布
	Lambda float64 // rate parameter 速率参数
}

// DefaultBConfig returns a default benchmark config
func DefaultBConfig() Bconfig {
	return Bconfig{
		T:                    60,	//运行总时间60s
		N:                    0,	//请求数量为0
		K:                    1000,	//key 空间
		W:                    0.5,	//写率
		Throttle:             0,	//未使用每秒请求限制
		Concurrency:          1,	//模拟的客户端数量
		Distribution:         "uniform",//分布式模式
		LinearizabilityCheck: true,	//线性化检查
		Conflicts:            100,
		Min:                  0,
		Mu:                   0,
		Sigma:                60,
		Move:                 false,
		Speed:                500,
		ZipfianS:             2,
		ZipfianV:             1,
		Lambda:               0.01,
	}
}

// Benchmark is benchmarking tool that generates workload and collects operation history and latency
//benchmark结构体：
type Benchmark struct {
	db DB // read/write operation interface
	Bconfig
	*History

	rate      *Limiter	//由throttle(每秒限制请求数)生成
	latency   []time.Duration // latency per operation
	startTime time.Time
	zipf      *rand.Zipf
	counter   int

	wait sync.WaitGroup // waiting for all generated keys to complete  等待所有生成的密钥完成
}

// NewBenchmark returns new Benchmark object given implementation of DB interface
func NewBenchmark(db DB) *Benchmark {
	b := new(Benchmark)
	b.db = db
	b.Bconfig = config.Benchmark
	b.History = NewHistory()
	if b.Throttle > 0 {
		//b.rate为每秒限制请求数b.Throttle生成
		b.rate = NewLimiter(b.Throttle)
	}
	//func (r *Rand) Seed(seed int64):使用给定的seed来初始化生成器到一个确定的状态
	rand.Seed(time.Now().UTC().UnixNano())
	//func New(src Source) *Rand:返回一个使用src生产的随机数来生成其他各种分布的随机数值的*Rand。
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	//func NewZipf(r *Rand, s float64, v float64, imax uint64) *Zipf:NewZipf返回一个[0, imax]范围内的齐普夫随机数生成器。
	//齐普夫分布：值k出现的几率p(k)正比于(v+k)**(-s)，其中s>1且k>=0且v>=1。
	b.zipf = rand.NewZipf(r, b.ZipfianS, b.ZipfianV, uint64(b.K))
	return b
}

// Load will create all K keys to DB:将创建数据库的所有K个密钥
func (b *Benchmark) Load() {
	b.W = 1.0		//全写操作
	b.Throttle = 0	//每秒不限制操作数

	b.db.Init()		//初始化db
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	b.startTime = time.Now()
	//运行concurrency个key的过程
	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}
	for i := b.Min; i < b.Min+b.K; i++ {
		b.wait.Add(1)
		keys <- i
	}
	//时间差
	t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	b.wait.Wait()
	stat := Statistic(b.latency)

	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)
}

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	var stop chan bool
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }
		stop = Schedule(move, time.Duration(b.Speed)*time.Millisecond)
		defer close(stop)
	}

	b.latency = make([]time.Duration, 0)
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}

	b.db.Init()
	b.startTime = time.Now()
	if b.T > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.T))
	loop:
		for {
			select {
			case <-timer.C:
				break loop
			default:
				b.wait.Add(1)
				keys <- b.next()
			}
		}
	} else {
		for i := 0; i < b.N; i++ {
			b.wait.Add(1)
			keys <- b.next()
		}
		b.wait.Wait()
	}
	t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	stat.WriteFile("latency")
	b.History.WriteFile("history")

	if b.LinearizabilityCheck {
		n := b.History.Linearizable()
		if n == 0 {
			log.Info("The execution is linearizable.")
		} else {
			log.Info("The execution is NOT linearizable.")
			log.Infof("Total anomaly read operations are %d", n)
			log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
		}
	}
}

// generates key based on distribution；
//根据不同的分布式策略生key
func (b *Benchmark) next() int {
	var key int
	switch b.Distribution {
	case "order":
		b.counter = (b.counter + 1) % b.K
		key = b.counter + b.Min

	case "uniform":
		key = rand.Intn(b.K) + b.Min

	case "conflict":
		if rand.Intn(100) < b.Conflicts {
			key = 0
		} else {
			b.counter = (b.counter + 1) % b.K
			key = b.counter + b.Min
		}

	case "normal":
		key = int(rand.NormFloat64()*b.Sigma + b.Mu)
		for key < 0 {
			key += b.K
		}
		for key > b.K {
			key -= b.K
		}

	case "zipfan":
		key = int(b.zipf.Uint64())

	case "exponential":
		key = int(rand.ExpFloat64() / b.Lambda)

	default:
		log.Fatalf("unknown distribution %s", b.Distribution)
	}

	if b.Throttle > 0 {
		b.rate.Wait()
	}

	return key
}

//测验读操作或写操作
func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	var s time.Time
	var e time.Time
	var v int
	var err error
	for k := range keys {
		op := new(operation)
		//rand.Float64() 返回一个取值范围在[0.0, 1.0)的伪随机float64值
		if rand.Float64() < b.W {
			v = rand.Int()
			s = time.Now()
			err = b.db.Write(k, v)
			e = time.Now()
			op.input = v
		} else {
			s = time.Now()
			v, err = b.db.Read(k)
			e = time.Now()
			op.output = v
		}
		op.start = s.Sub(b.startTime).Nanoseconds()
		if err == nil {
			op.end = e.Sub(b.startTime).Nanoseconds()
			result <- e.Sub(s)
		} else {
			op.end = math.MaxInt64
			log.Error(err)
		}
		b.History.AddOperation(k, op)
	}
}

//所有延迟，等待执行
func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
		b.wait.Done()
	}
}
