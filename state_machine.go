package paxi

// StateMachine defines a deterministic state machine
//状态机
type StateMachine interface {
	// Execute is the state-transition function
	// returns current state value if state unchanged or previous state value
	Execute(interface{}) interface{}
}

//状态 
type State interface {
	Hash() uint64
}
