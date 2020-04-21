package paxi

import (
	//gob包在发送器和接收器之间交换binary值，一般用于传递远程程序调用的参数和结果
	"encoding/gob"
	"fmt"
	"net"
	"time"

	"github.com/ailidani/paxi/log"
)

//二者选最大值
// Max of two int
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// VMax of a vector 数组最大值
func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

//重试attempt次后 函数f的错误信息
// Retry function f sleep time between attempts
func Retry(f func() error, attempts int, sleep time.Duration) error {
	var err error
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return nil
		}

		if i >= attempts-1 {
			break
		}

		// exponential delay
		time.Sleep(sleep * time.Duration(i+1))
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

//重复调用f函数
// Schedule repeatedly call function with intervals
//time.Duration:两个时间之间经过的时间，以纳秒为单位。chan是通道，chan bool是指可以发送bool类型的通道
func Schedule(f func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			f()
			select {
				//case语句：若为time.After语句，则无动作
				//隔delay时间在调用一次
			case <-time.After(delay):
			//若为stop，停止调用，则返回
			case <-stop:
				return
			}
		}
	}()

	return stop
}

//连接master，
// ConnectToMaster connects to master node and set global Config
func ConnectToMaster(addr string, client bool, id ID) {
	//net.Dial是在指定网络上连接指定地址。
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		//打印错误信息
		log.Fatal(err)
	}
	//decoder是一个描述数据的接口，用自己的方案解码要发送的数据
	dec := gob.NewDecoder(conn)
	//encoder是一个描述数据的接口，用自己的方案来将数据编码，供decoder接收并解码
	enc := gob.NewEncoder(conn)
	msg := &Register{
		ID:     id,
		Client: client,
		Addr:   "",
	}
	enc.Encode(msg)
	err = dec.Decode(&config)
	if err != nil {
		log.Fatal(err)
	}
}
