package paxi

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"time"
)

// Stat stores the statistics data for benchmarking results
//统计数据储存基准结果的统计数据 
type Stat struct {
	Data   []float64
	Size   int
	Mean   float64
	Min    float64
	Max    float64
	Median float64
	P95    float64
	P99    float64
	P999   float64
}

// WriteFile writes stat to new file in path
//在path指定的文件中写stat
func (s Stat) WriteFile(path string) error {
	//path路径下创建文件
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	//writefile方法结束后关闭文件
	defer file.Close()
	
	//为file建立writebuffer
	w := bufio.NewWriter(file)
	for _, line := range s.Data {
		//写入data到buffer w中
		fmt.Fprintln(w, line)
	}
	//刷新
	return w.Flush()
}

func (s Stat) String() string {
	//字符串化输出
	return fmt.Sprintf("size = %d\nmean = %f\nmin = %f\nmax = %f\nmedian = %f\np95 = %f\np99 = %f\np999 = %f\n", s.Size, s.Mean, s.Min, s.Max, s.Median, s.P95, s.P99, s.P999)
}

//初始化stat
// Statistic function creates Stat object from raw latency data
func Statistic(latency []time.Duration) Stat {
	ms := make([]float64, 0)
	//生成ms列表
	for _, l := range latency {
		ms = append(ms, float64(l.Nanoseconds())/1000000.0)
	}
	//排序
	sort.Float64s(ms)
	sum := 0.0
	for _, m := range ms {
		sum += m
	}
	size := len(ms)
	//返回stat
	return Stat{
		Data:   ms,
		Size:   size,
		Mean:   sum / float64(size),
		Min:    ms[0],
		Max:    ms[size-1],
		Median: ms[int(0.5*float64(size))],
		P95:    ms[int(0.95*float64(size))],
		P99:    ms[int(0.99*float64(size))],
		P999:   ms[int(0.999*float64(size))],
	}
}
