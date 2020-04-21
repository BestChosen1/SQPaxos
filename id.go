package paxi

import (
	"strconv"
	"strings"

	"github.com/ailidani/paxi/log"
)

// ID represents a generic identifier in format of Zone.Node
type ID string

// NewID returns a new ID type given two int number of zone and node
//NewID方法：根据输入参数zone和node，返回string类型的ID
func NewID(zone, node int) ID {
	if zone < 0 {
		zone = -zone
	}
	if node < 0 {
		node = -node
	}
	// return ID(fmt.Sprintf("%d.%d", zone, node))
	//strconv.Itoa(i int) string:将int转换成string类型 
	return ID(strconv.Itoa(zone) + "." + strconv.Itoa(node))
}

// Zone returns Zond ID component
//zone（）方法：返回int类型的zone编号
func (i ID) Zone() int {
	if !strings.Contains(string(i), ".") {
		log.Warningf("id %s does not contain \".\"\n", i)
		return 0
	}
	s := strings.Split(string(i), ".")[0]
	//func ParseUint(s string, base int, bitSize int) (n uint64, err error):ParseUint类似ParseInt但不接受正负号，用于无符号整型
	zone, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Zone %s to int\n", s)
		return 0
	}
	return int(zone)
}

// Node returns Node ID component
//node（）:返回int类型的node编号
func (i ID) Node() int {
	var s string
	if !strings.Contains(string(i), ".") {
		log.Warningf("id %s does not contain \".\"\n", i)
		s = string(i)
	} else {
		s = strings.Split(string(i), ".")[1]
	}
	node, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Node %s to int\n", s)
		return 0
	}
	return int(node)
}

//IDs:ID数组
type IDs []ID

func (a IDs) Len() int      { return len(a) }
func (a IDs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
//less方法：比较两个输入的大小，不同zone：i的zone好比j小返回true，相同zone，比较node，i<j 返回true
func (a IDs) Less(i, j int) bool {
	if a[i].Zone() < a[j].Zone() {
		return true
	} else if a[i].Zone() > a[j].Zone() {
		return false
	} else {
		return a[i].Node() < a[j].Node()
	}
}
