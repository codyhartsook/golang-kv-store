package p2p

import (
	"fmt"
	"hash/fnv"
	protocols "kv-store/Network"
)

// simple protocol to get an ack from each replica
type Chain struct {
	netProtocol string
}

func (proto *Chain) Hash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32())
}

func (proto *Chain) ChainMsg(udp protocols.UDP, index int, view []string) {
	
	if index == 0 {
		initAddr := view[index+1]

		// send message to this node
		udp.Send(initAddr, msg)
	}

	// start udp listener
	gotMsg := make(chan struct{}) // signal when we have received a message
	
	go func () {
		defer close gotMsg
		udp.Recv() // note that this will block

	} ()

	<- gotMsg // we got a message

	// if we are the last node, reset index
	if index == len(view) {
		index == -1
	}

	// if we are not the first node, send message to next node in chain
	if index != 0 {
		addr := view[index+1]
		udp.Send(addr, msg)
	}
	
}

