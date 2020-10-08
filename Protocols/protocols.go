package P2P

import (
	"fmt"
	"time"
	consensus "kv-store/Consensus"
)

// simple protocol to propogate a message from the start node to the last
// node. 
type Protocol struct {
	msg string
	action string
	timeout int
	num_packets int
	state string
	gossip_interval_seconds float32
	addr string
}

//
func NewProtocol(node_addr string) *Protocol {
	p := new(Protocol)
	p.gossip_interval_seconds = 2.0
	p.addr = node_addr

	return p
}

// Algorithm: Send message to each node in parrallel
func (proto *Protocol) Broadcast(view []string, con consensus.ConsensusEngine) error {
	proto.msg = "Broadcasting"
	proto.action = "broadcast"

	for _, node := range view {
		go con.Send(node, proto.msg, proto.action)
	}
}

/* Gossip protocol:
	Select a frequency to gossip

	At the start of each iteration, choose a peer at random and send vector clock and
	database contents id 

	if peer has different id and greater vector clock, update datastore
*/
func (proto *Protocol) StartGossip(oracle Orchestrator, con consensus.ConsensusEngine) error {
	// choose a shard replica at random
	// Run the gossip protocol
	go doEvery(proto.gossip_interval_seconds*time.Millisecond, proto.SendGossip())
}

// scheduling function
func doEvery(d time.Duration, f func(time.Time)) {
	for x := range time.Tick(d) {
		f(x)
	}
}

func (proto *Protocol) SendGossip(oracle Orchestrator, con consensus.ConsensusEngine) error {
	// choose random peer within shard
	// send message to peer with vc and db id

	// choose a shard replica at random
	nodes := oracle.ShardReplicas(proto.addr)

	rand.Seed(time.Now())
	peer := nodes[rand.Intn(len(nodes))]
}

func (proto *Protocol) RecvGossip(oracle Orchestrator, con consensus.ConsensusEngine) error {

	// if db id differs:
		// compare vc to resolve

	// resolve vcs
}

// Define a start node by choosing the smallest ring hash, then send a message
// to the next phyical node in the ring.
func (proto *Protocol) ChainMsg(oracle Orchestrator, con consensus.ConsensusEngine) error {

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
		con.Send(initAddr, proto.msg, proto.action)
		fmt.Println("Sending chain message to first node", initAddr)
	}

	con.RecvFrom() // wait until this node gets a message

	// if we are the last node, reset index 
	if index == (len(oracle.view)-1) {
		index = -1
	}

	// if we are not the first node, send message to next node in chain
	if index != 0 {
		addr := oracle.view[index+1]
		fmt.Println("Sending chain message to next node", addr)
		con.Send(addr, proto.msg, proto.action)
	}

	return nil
}
