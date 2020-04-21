package paxi

import (
	"sync"
	"time"
)

// Limiter limits operation rate when used with Wait function
//limiter：当使用等待功能时，限制工作速率
type Limiter struct {
	sync.Mutex
	//最新时间
	last     time.Time
	//休眠
	sleep    time.Duration
	//间隔时间
	interval time.Duration
	//松弛
	slack    time.Duration
}

// NewLimiter creates a new rate limiter, where rate is operations per second
func NewLimiter(rate int) *Limiter {
	return &Limiter{
		//秒除次数
		interval: time.Second / time.Duration(rate),
		//松弛：10倍的interval
		slack:    -10 * time.Second / time.Duration(rate),
	}
}

// Wait blocks for the limit
func (l *Limiter) Wait() {
	l.Lock()
	//本方法结束后释放锁
	defer l.Unlock()

	//获取当前时间
	now := time.Now()

	//l.last是0
	if l.last.IsZero() {
		//l.last为当前时间
		l.last = now
		return
	}

	//l.sleep= l.sleep+l.interval-现在和刚才last的时间差
	l.sleep += l.interval - now.Sub(l.last)

	if l.sleep < l.slack {
		l.sleep = l.slack
	}

	if l.sleep > 0 {
		time.Sleep(l.sleep)
		l.last = now.Add(l.sleep)
		l.sleep = 0
	} else {
		l.last = now
	}
}
