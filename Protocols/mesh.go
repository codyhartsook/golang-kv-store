package p2p

import (
	"fmt"
	"hash/fnv"
	protocols "kv-store/Network"
)

// simple protocol to get an ack from each replica
type Chain struct {
	msg string
	ack string
	timeout int
	num_packets int
}

func (proto *Chain) Hash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32())
}

func (proto *Chain) ChainMsg(udp protocols.UDP, index int, view []string) error {

	if len(view) == 1 {
		return nil
	}
	
	proto.msg = "Are you up?"
	proto.ack = "Yep!"
	proto.num_packets = 1
	proto.timeout = 10

	// is this is node0, send packet to node1
	if index == 0 {
		initAddr := view[index+1]

		// send message to this node
		udp.Send(initAddr, proto.msg)
		fmt.Println("Sending message to first node ", initAddr)
	}
	
	// wait to receive packet from node in chain
	// note that this will block until all messages have been received or a timeotu occures
	cerr, conn := udp.Recv(proto.num_packets, proto.timeout)

	if cerr != nil {
		fmt.Println("error getting request from peer, check that all nodes are up:", cerr)
		return cerr
	}

	//r_addr := conn.RemoteAddr().(*net.UDPAddr)
	//udp.SendResponse(conn, r_addr, proto.ack)

	conn.Close()

	// if we are the last node, reset index 
	if index == len(view) {
		index = -1
	}

	// if we are not the first node, send message to next node in chain
	if index != 0 {
		addr := view[index+1]
		udp.Send(addr, proto.msg)
	}

	return nil
}

