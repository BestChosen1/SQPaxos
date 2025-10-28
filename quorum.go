package paxi

// Quorum records each acknowledgement and check for different types of quorum satisfied
type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
	nacks map[ID]bool
}

// NewQuorum returns a new Quorum
func NewQuorum() *Quorum {
	q := &Quorum{
		size:  0,
		acks:  make(map[ID]bool),
		zones: make(map[int]int),
	}
	return q
}

// ACK adds id to quorum ack records. id回复同意信息后，quorum.size++;
func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
	}
}

// NACK adds id to quorum nack records
func (q *Quorum) NACK(id ID) {
	if !q.nacks[id] {
		q.nacks[id] = true
	}
}

func (q *Quorum) GetAcks() map[ID]bool {
	return q.acks
}

func (q *Quorum) HaveID(ids []ID) bool {
	for _ , id := range ids {
		if !q.acks[id] {
			return false
		}
	}
	return true
}

// ADD increase ack size by one 
//quorum的大小加1
func (q *Quorum) ADD() {
	q.size++
}

// Size returns current ack size
//返回quorum大小
func (q *Quorum) Size() int {
	return q.size
}

// Reset resets the quorum to empty
//重置quorum的所有参数
func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[ID]bool)
	q.zones = make(map[int]int)
	q.nacks = make(map[ID]bool)
}

//quorum的大小是否等于配置大小（全回复）
func (q *Quorum) All() bool {
	return q.size == config.n
}

//quorum的大小是否大于 majority
// Majority quorum satisfied
func (q *Quorum) Majority() bool {
	return q.size > config.n/2
}

//quorum的大小是否是fast quorum
// FastQuorum from fast paxos
func (q *Quorum) FastQuorum() bool {
	return q.size >= config.n*3/4
}

// AllZones returns true if there is at one ack from each zone
//是否收到了所有zone的ack，每个zone至少一个结点回复
func (q *Quorum) AllZones() bool {
	return len(q.zones) == config.z
}

// ZoneMajority returns true if majority quorum satisfied in any zone
//任意一个区域满足majority
func (q *Quorum) ZoneMajority() bool {
	for z, n := range q.zones {
		if n > config.npz[z]/2 {
			return true
		}
	}
	return false
}

// GridRow == AllZones  一个结点一个zone 网格行=所有zone
func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

// GridColumn == all nodes in one zone 所有结点在一个zone中
func (q *Quorum) GridColumn() bool {
	for z, n := range q.zones {
		if n == config.npz[z] {
			return true
		}
	}
	return false
}

// // FGridQ1 is flexible grid quorum for phase 1  Fpaxos第一阶段使用的quorum
// func (q *Quorum) FGridQ1(Fz int) bool {
// 	zone := 0
// 	for z, n := range q.zones {
// 		if n > config.npz[z]/2 {
// 			//
// 			zone++
// 		}
// 	}
// 	return zone >= config.z-Fz
// }

// FGridQ1 is flexible grid quorum for phase 1  Fpaxos第一阶段使用的quorum
func (q *Quorum) FGridQ1(Fz int, Fd int) bool {
	zone := 0
	for z, n := range q.zones {
		if n >= config.npz[z]-Fd {
			//
			zone++
		}
	}
	return zone >= config.z-Fz
}

// // FGridQ2 is flexible grid quorum for phase 2
// func (q *Quorum) FGridQ2(Fz int) bool {
// 	zone := 0
// 	//没动
// 	for z, n := range q.zones {
// 		if n > config.npz[z]/2 {
// 			zone++
// 		}
// 	}
// 	return zone >= Fz+1
// }

// FGridQ2 is flexible grid quorum for phase 2
func (q *Quorum) FGridQ2(Fz int, Fd int) bool {
	zone := 0
	//没动
	for _, n := range q.zones {
		if n >= Fd+1 {
			zone++
		}
	}
	return zone >= Fz+1
}

func (q *Quorum)SGridQ1() bool {
	zone := 0
	for z, n := range q.zones{
		if n > config.npz[z]/2{
			zone++
		}
	}
	return zone > config.z/2
}

func (q *Quorum) SGridQ2(Fz, Fn int) bool{
	zone := 0
	for _, n := range q.zones{
		if n >= Fn+1{
			zone++
		}
	}
	return zone >= Fz +1
}


// // Q1 returns true if config.Quorum type is satisfied
// func (q *Quorum) Q1() bool {
// 	switch config.Quorum {
// 	case "majority":
// 		return q.Majority()
// 	case "grid":
// 		return q.GridRow()
// 	case "fgrid":
// 		return q.FGridQ1()
// 	case "group":
// 		return q.ZoneMajority()
// 	case "count":
// 		return q.size >= config.n-config.F
// 	default:
// 		log.Error("Unknown quorum type")
// 		return false
// 	}
// }

// // Q2 returns true if config.Quorum type is satisfied
// func (q *Quorum) Q2() bool {
// 	switch config.Quorum {
// 	case "majority":
// 		return q.Majority()
// 	case "grid":
// 		return q.GridColumn()
// 	case "fgrid":
// 		return q.FGridQ2()
// 	case "group":
// 		return q.ZoneMajority()
// 	case "count":
// 		return q.size > config.F
// 	default:
// 		log.Error("Unknown quorum type")
// 		return false
// 	}
// }
