package paxi

import "fmt"

type operation struct {
	input  interface{}
	output interface{}
	// timestamps
	start int64
	end   int64
}

//判断a操作是否在b操作之前
func (a operation) happenBefore(b operation) bool {
	return a.end < b.start
}

//判断a操作与b操作是否并行（a不在b之前,b也不在a之前）
func (a operation) concurrent(b operation) bool {
	return !a.happenBefore(b) && !b.happenBefore(a)
}

//a b等价：四种操作是否相同
func (a operation) equal(b operation) bool {
	return a.input == b.input && a.output == b.output && a.start == b.start && a.end == b.end
}

//字符化
func (a operation) String() string {
	return fmt.Sprintf("{input=%v, output=%v, start=%d, end=%d}", a.input, a.output, a.start, a.end)
}

//安照开始时间排序
// sort operations by invocation time
type byTime []*operation

func (a byTime) Len() int           { return len(a) }
func (a byTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTime) Less(i, j int) bool { return a[i].start < a[j].start }
