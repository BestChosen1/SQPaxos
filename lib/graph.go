package lib

import "container/list"

type Graph struct {
	//顶点集合
	vertices Set
	// from(map名）  map[key类型]值类型  key是接口类型，value是set集合
	from     map[interface{}]Set
	to       map[interface{}]Set
}

func NewGraph() *Graph {
	return &Graph{
		vertices: NewSet(),
		//make函数创建map
		from:     make(map[interface{}]Set),
		to:       make(map[interface{}]Set),
	}
}

func (g *Graph) Size() int {
	//图的大小是顶点数
	return len(g.vertices)
}

//查结点
func (g *Graph) Has(v interface{}) bool {
	return g.vertices.Has(v)
}

//增加结点，加入form set ，加入 to set
func (g *Graph) Add(v interface{}) {
	if !g.Has(v) {
		g.vertices.Add(v)
		g.from[v] = NewSet()
		g.to[v] = NewSet()
	}
}

//移除v，
func (g *Graph) Remove(v interface{}) {
	if !g.Has(v) {
		return
	}
	g.vertices.Remove(v)
//移除边
	for u := range g.vertices {
		g.from[u].Remove(v)
		g.to[u].Remove(v)
	}
	//from、to的map中删除v delete（map_name,key)
	delete(g.from, v)
	delete(g.to, v)
}

//加边：form 集合中加to，to集合中加from
func (g *Graph) AddEdge(from, to interface{}) {
	if from == to {
		panic("graph: adding self edge")
	}
	if !g.Has(from) {
		g.Add(from)
	}
	if !g.Has(to) {
		g.Add(to)
	}
	g.from[from].Add(to)
	g.to[to].Add(from)
}

//移除边
func (g *Graph) RemoveEdge(from, to interface{}) {
	if !g.Has(from) || !g.Has(to) {
		return
	}
	g.from[from].Remove(to)
	g.to[to].Remove(from)
}

//返回结点集合
func (g *Graph) Vertices() Set {
	return g.vertices
}

//从v出发的边的所有结点集合
// From returns all vertices in graph that can be reached directly from v
func (g *Graph) From(v interface{}) Set {
	return g.from[v]
}

//到v 的结点结合
// To returns all vertices that can reach to v
func (g *Graph) To(v interface{}) Set {
	return g.to[v]
}

//广度优先搜索
// BFS returns breadth first search vertices from a given source
func (g *Graph) BFS(v interface{}) []interface{} {
	result := make([]interface{}, 0)
	visited := make(map[interface{}]bool)
	queue := list.New()

	visited[v] = true
	queue.PushBack(v)

	for queue.Len() > 0 {
		s := queue.Front()
		result = append(result, s.Value)
		queue.Remove(s)

		for t := range g.from[s.Value] {
			if !visited[t] {
				visited[t] = true
				queue.PushBack(t)
			}
		}
	}

	return result
}

//深度优先搜索
// DFS returns depth first search vertices from a given source
func (g *Graph) DFS(v interface{}) []interface{} {
	result := make([]interface{}, 0)
	visited := NewSet()
	stack := NewStack()

	stack.Push(v)

	for !stack.Empty() {
		s := stack.Pop()
		if !visited.Has(s) {
			visited.Add(s)
			result = append(result, s)
		}

		for i := range g.from[s] {
			if !visited.Has(i) {
				stack.Push(i)
			}
		}
	}

	return result
}

func (g *Graph) BFSReverse(v interface{}) []interface{} {
	vertices := make([]interface{}, 0)
	visited := make(map[interface{}]bool)
	queue := list.New()

	visited[v] = true
	queue.PushBack(v)

	for queue.Len() > 0 {
		s := queue.Front()
		vertices = append(vertices, s.Value)
		queue.Remove(s)

		for t := range g.to[s.Value] {
			if !visited[t] {
				visited[t] = true
				queue.PushBack(t)
			}
		}
	}

	return vertices
}

//复制g到t
// Transpose return transpose graph
func (g *Graph) Transpose() *Graph {
	t := NewGraph()
	t.vertices = g.vertices.Clone()

	for v := range g.vertices {
		t.from[v] = g.to[v].Clone()
		t.to[v] = g.from[v].Clone()
	}

	return t
}

type color int

const (
	white color = iota // unvisited nodes
	gray               // visiting nodes
	black              // visited nodes
)

func (g *Graph) visit(v interface{}, colors map[interface{}]color) bool {
	colors[v] = gray
	//访问所有从v出发到u的结点
	for _, u := range g.from[v].Slice() {
		//v==u
		if colors[u] == gray {
			return true
		}
		//u是未访问结点，迭代访问u
		if colors[u] == white && g.visit(u, colors) {
			return true
		}
	}
	colors[v] = black
	return false
}


// Cyclic returns true if the graph contains a cycle
func (g *Graph) Cyclic() bool {
	colors := make(map[interface{}]color)
	for v := range g.vertices {
		colors[v] = white
	}

	for v := range g.vertices {
		if colors[v] == white {
			if g.visit(v, colors) {
				return true
			}
		}
	}
	return false
}

// Cycle returns the first cycle with vertices
func (g *Graph) Cycle() []interface{} {
	colors := make(map[interface{}]color)
	for v := range g.vertices {
		colors[v] = white
	}

	for v := range g.vertices {
		if colors[v] == white {
			if g.visit(v, colors) {
				cycle := make([]interface{}, 0)
				for v, color := range colors {
					if color == gray {
						cycle = append(cycle, v)
					}
				}
				return cycle
			}
		}
	}
	return nil
}
