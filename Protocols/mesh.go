package p2p

import (
	"fmt"
	protocols "kv-store/Network"
)

// simple protocol to get an ack from each replica
type PingPeer struct {
	netProtocol string
}

func (ping *PingPeer) SendMsg(udp protocols.UDP) {
	fmt.Println("sending message to each node")
	udp.Send()
}

