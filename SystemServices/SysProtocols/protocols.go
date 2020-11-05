package protocols

import (
	"fmt"
	msg "kv-store/Messages"
	consensus "kv-store/SystemServices/Consensus"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var gossiping = &sync.Mutex{} // used as a lock for gossiping protocol

// Protocol -> simple protocol to propogate a message from the start node to the last
// node.
type Protocol struct {
	msg              string
	action           string
	timeout          int
	numPackets       int
	state            string
	gossipIntervalMs float32
	shardReplicas    []string
	addr             string
	notSeen          []string
}

// NewProtocol ->
func NewProtocol(nodeAddr string, shardReplicas []string) *Protocol {
	p := new(Protocol)
	p.addr = nodeAddr
	p.shardReplicas = shardReplicas

	return p
}

// Broadcast -> Send message to each node in parrallel
func (proto *Protocol) Broadcast(view []string, con consensus.ConEngine) {
	proto.msg = "Broadcasting"
	proto.action = "broadcast"

	for _, node := range view {
		thisMsg := msg.Msg{
			SrcAddr: proto.addr,
			Payload: proto.msg,
			ID:      "",
			Action:  proto.action,
		}
		go con.SendWithoutEvent(node, thisMsg)
	}
}

// scheduling function
func (proto *Protocol) doEvery(d time.Duration, f func(consensus.ConEngine), con consensus.ConEngine) {
	for x := range time.Tick(d) {
		f(con)

		if len(proto.notSeen) == 0 {
			fmt.Println("round done.", x)
			break
		}
	}
}

// InitGossipProtocol -> Every 2 seconds start a gossip round
func (proto *Protocol) InitGossipProtocol(con consensus.ConEngine) {
	gossipRounds := 2000
	interval := time.Duration(gossipRounds) * time.Millisecond

	// start a gossip round every 2 seconds
	for x := range time.Tick(interval) {
		rand.Seed(time.Now().UTC().UnixNano())

		i := rand.Intn(len(proto.shardReplicas))
		startNode := proto.shardReplicas[i]
		ip := strings.Split(startNode, ":")[0]

		fmt.Println(ip, proto.addr)

		if ip == proto.addr {
			fmt.Println("Gossip round starting", x)

			proto.startGossipRound(con)
		}
	}
}

// StartGossipRound -> choose a shard replica at random then runt he gossip protocol
func (proto *Protocol) startGossipRound(con consensus.ConEngine) {

	proto.notSeen = make([]string, len(proto.shardReplicas))
	copy(proto.notSeen, proto.shardReplicas)

	for i, node := range proto.shardReplicas {
		ip := strings.Split(node, ":")[0]
		if ip == proto.addr {
			proto.notSeen = proto.deleteIndex(i)
		}
	}

	proto.gossipIntervalMs = 100 // 200 milisecond delay between gossips
	rand.Seed(time.Now().UTC().UnixNano())
	interval := time.Duration(proto.gossipIntervalMs) * time.Millisecond

	proto.doEvery(interval, proto.sendGossip, con)
}

// SendGossip -> must put a lock on gossiping so only one node at a time can gossip with us
func (proto *Protocol) sendGossip(con consensus.ConEngine) {

	// choose a shard replica at random
	peer := proto.chooseNode()

	// get lock
	gossiping.Lock()

	thisMsg := msg.Msg{
		SrcAddr: proto.addr,
		Payload: "database contents ID",
		ID:      "",
		Action:  "gossip",
	}

	// send message to peer with vc and db id
	fmt.Println("sending gossip to", peer)
	con.SendWithoutEvent(peer, thisMsg)

	// state transfer

	// release lock
	gossiping.Unlock()
}

// RecvGossip -> must put a lock on gossiping so only one node at a time can gossip with us
func (proto *Protocol) RecvGossip(Msg msg.Msg, con consensus.ConEngine) {

	// get lock
	gossiping.Lock()
	// if db id differs:
	// compare vc to resolve

	fmt.Println("gossiping with", Msg.SrcAddr)

	// resolve vcs
	needToUpdate := con.ValidDeliveryLocal(Msg.SrcAddr, Msg.Context)

	fmt.Println("Do we need to update our datastore:", needToUpdate)

	con.PrintVC()

	// release lock
	gossiping.Unlock()
}

// chooseNode ->
func (proto *Protocol) chooseNode() string {

	if len(proto.notSeen) == 0 {
		return "end of nodes"
	}

	i := rand.Intn(len(proto.notSeen))

	peer := proto.notSeen[i]
	proto.notSeen = proto.deleteIndex(i)

	return peer
}

// deleteIndex ->
func (proto Protocol) deleteIndex(i int) []string {
	if len(proto.notSeen) == 0 {
		fmt.Println("panic")
	}
	proto.notSeen[i] = proto.notSeen[len(proto.notSeen)-1]
	proto.notSeen[len(proto.notSeen)-1] = "p"            // Erase last element (write zero value).
	proto.notSeen = proto.notSeen[:len(proto.notSeen)-1] // Truncate slice.
	return proto.notSeen
}

// ChainMsg -> Define a start node by choosing the smallest ring hash, then send a message
// to the next phyical node in the ring.
func (proto *Protocol) ChainMsg(oracle Orchestrator, con consensus.ConEngine) error {

	if len(oracle.view) == 1 {
		return nil
	}

	proto.msg = "Are you up?"
	proto.action = "signal"
	proto.numPackets = 1
	proto.timeout = 60

	// determine which node we are in the ring
	index := oracle.GetShardID(proto.addr)

	fmt.Println("my index:", index)

	// if this is node0, send packet to node1
	if index == 0 {
		initAddr := oracle.view[index+1]

		// send message to this node
		thisMsg := msg.Msg{
			SrcAddr: proto.addr,
			Payload: proto.msg,
			ID:      "",
			Action:  proto.action,
		}
		con.SendWithoutEvent(initAddr, thisMsg)
		fmt.Println("Sending chain message to first node", initAddr)
	}

	con.RecvFrom() // wait until this node gets a message

	// if we are the last node, reset index
	if index == (len(oracle.view) - 1) {
		index = -1
	}

	// if we are not the first node, send message to next node in chain
	if index != 0 {
		addr := oracle.view[index+1]
		fmt.Println("Sending chain message to next node", addr)
		thisMsg := msg.Msg{
			SrcAddr: proto.addr,
			Payload: proto.msg,
			ID:      "",
			Action:  proto.action,
		}
		con.SendWithoutEvent(addr, thisMsg)
	}

	return nil
}