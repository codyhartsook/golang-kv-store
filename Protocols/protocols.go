package P2P

import (
	"fmt"
	netutil "kv-store/Network"
)

// simple protocol to propogate a message from the start node to the last
// node. 
type Chain struct {
	msg string
	action string
	timeout int
	num_packets int
}

//
func NewChainMessager() *Chain {
	chain := new(Chain)

	return chain
}

// Define a start node by choosing the smallest ring hash, then send a message
// to the next phyical node in the ring.
func (proto *Chain) ChainMsg(oracle Orchestrator, udp netutil.UDP) error {

	if len(oracle.view) == 1 {
		return nil
	}
	
	proto.msg = "Are you up?"
	proto.action = "signal"
	proto.num_packets = 1
	proto.timeout = 60

	// determine which node we are in the ring
	index := oracle.GetNodeRingIndex()

	fmt.Println("my index:", index)

	// is this is node0, send packet to node1
	if index == 0 {
		initAddr := oracle.view[index+1]

		// send message to this node
		udp.Send(initAddr, proto.msg, proto.action, oracle.Context)
		fmt.Println("Sending chain message to first node", initAddr)
	}

	udp.RecvFrom() // wait until this node gets a message

	// if we are the last node, reset index 
	if index == (len(oracle.view)-1) {
		index = -1
	}

	// if we are not the first node, send message to next node in chain
	if index != 0 {
		addr := oracle.view[index+1]
		fmt.Println("Sending chain message to next node", addr)
		udp.Send(addr, proto.msg, proto.action, oracle.Context)
	}

	return nil
}
