package paxi

import (
	"sort"

	"github.com/ailidani/paxi/lib"
)

// A simple linearizability checker based on https://pdos.csail.mit.edu/6.824/papers/fb-consistency.pdf
//checker是图结构
type checker struct {
	*lib.Graph
}

func newChecker() *checker {
	return &checker{
		Graph: lib.NewGraph(),
	}
}

//operation：input,output,start,end
//添加operation就是增加结点,同时增加结点之间的边
func (c *checker) add(o *operation) {
	if c.Graph.Has(o) {
		// already in graph from lookahead
		return
	}
	//operations是点vertice
	c.Graph.Add(o)
	for v := range c.Graph.Vertices() {
		//序列化operation：增加在o之前的操作v与o 之间的边
		if v.(*operation).happenBefore(*o) {
			//c.AddEdge(o, v)
			c.AddEdge(v, o)
		}
	}
}

//移除operations，移除点
func (c *checker) remove(o *operation) {
	c.Remove(o)
}

//clear：重新分配一个新的图
func (c *checker) clear() {
	c.Graph = lib.NewGraph()
}

//match():read操作的output是某个操作的input，返回该操作
// match finds the first matching write operation to the given read operation
func (c *checker) match(read *operation) *operation {
	//for _, v := range c.Graph.BFSReverse(read) {
	for v := range c.Graph.Vertices() {
		if read.output == v.(*operation).input {
			return v.(*operation)
		}
	}
	return nil
}

//合并merge():匹配写入继承边读取
// matched write inherits edges read
func (c *checker) merge(read, write *operation) {
	//遍历指向read操作的operations s
	//operations与读有边，增加到write的边
	for s := range c.To(read) {
		//如果s不是写操作，增加写操作
		if s.(*operation) != write {
			//增加s.operations到write的边
			c.Graph.AddEdge(s.(*operation), write)
		}
	}

	// refine response time of merged vertex 
	if read.end < write.end {
		write.end = read.end
	}
	//写代替读，移除读
	c.Graph.Remove(read)
}

//线性化：
func (c *checker) linearizable(history []*operation) []*operation {
	c.clear()
	sort.Sort(byTime(history))
	//结果集
	anomaly := make([]*operation, 0)
	for i, o := range history {
		c.add(o)
		// o is read operation
		if o.input == nil {
			// look ahead for concurrent writes
			for j := i + 1; j < len(history) && o.concurrent(*history[j]); j++ {
				// next operation is write
				if history[j].output == nil {
					c.add(history[j])
				}
			}

			match := c.match(o)
			if match != nil {
				c.merge(o, match)
			}

			cycle := c.Graph.Cycle()
			if cycle != nil {
				anomaly = append(anomaly, o)
				for _, u := range cycle {
					for _, v := range cycle {
						if c.Graph.From(u).Has(v) && u.(*operation).start > v.(*operation).end {
							c.Graph.RemoveEdge(u, v)
						}
					}
				}
			}
		}
	}
	return anomaly
}
