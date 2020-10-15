package P2P

import (
	"fmt"
	"time"
	"sync"
	"strings"
	"math/rand"
	consensus "kv-store/Consensus"
)

var gossiping = &sync.Mutex{} // used as a lock for gossiping protocol

// simple protocol to propogate a message from the start node to the last
// node. 
type Protocol struct {
	msg string
	action string
	timeout int
	num_packets int
	state string
	gossip_interval_ms float32
	shard_replicas []string
	addr string
	not_seen []string
}

//
func NewProtocol(node_addr string, shard_replicas []string) *Protocol {
	p := new(Protocol)
	p.addr = node_addr
	p.shard_replicas = shard_replicas

	return p
}

// Algorithm: Send message to each node in parrallel
func (proto *Protocol) Broadcast(view []string, con consensus.ConsensusEngine) {
	proto.msg = "Broadcasting"
	proto.action = "broadcast"

	for _, node := range view {
		go con.SendWithoutEvent(node, proto.msg, proto.action)
	}
}

// scheduling function
func (proto *Protocol) doEvery(d time.Duration, f func(consensus.ConsensusEngine), con consensus.ConsensusEngine) {
	for x := range time.Tick(d) {
		f(con)

		if len(proto.not_seen) == 0 {
			fmt.Println("round done.", x)
			break
		}
	}
}

/* Gossip protocol:
	Select a frequency to gossip

	At the start of each iteration, choose a peer at random and send vector clock and
	database contents id 

	if peer has different id and greater vector clock, update datastore
*/
func (proto *Protocol) RunGossipProtocol(con consensus.ConsensusEngine) {
	gossip_rounds := 2000
	interval := time.Duration(gossip_rounds)*time.Millisecond

	// start a gossip round every 2 seconds
	for x := range time.Tick(interval) {
		rand.Seed(time.Now().UTC().UnixNano())

		i := rand.Intn(len(proto.shard_replicas))
		start_node := proto.shard_replicas[i]
		ip := strings.Split(start_node, ":")[0]

		//fmt.Println("starter", ip)

		if ip == proto.addr {
			fmt.Println("Gossip round starting", x)
			
			proto.StartGossipRound(con)
		}
	}
}

func (proto *Protocol) StartGossipRound(con consensus.ConsensusEngine) {
	// choose a shard replica at random
	// Run the gossip protocol
	proto.not_seen = make([]string, len(proto.shard_replicas))
	copy(proto.not_seen, proto.shard_replicas)

	for i, node := range proto.shard_replicas {
		ip := strings.Split(node, ":")[0]
		if ip == proto.addr {
			proto.not_seen = proto.delete_index(i)
		}
	}

	proto.gossip_interval_ms = 100 // 200 milisecond delay between gossips
	rand.Seed(time.Now().UTC().UnixNano())
	interval := time.Duration(proto.gossip_interval_ms)*time.Millisecond

	proto.doEvery(interval, proto.SendGossip, con)
}

// must put a lock on gossiping so only one node at a time can gossip with us
func (proto *Protocol) SendGossip(con consensus.ConsensusEngine) {

	// choose a shard replica at random
	peer := proto.ChooseNode()

	// get lock
	gossiping.Lock()

	// send message to peer with vc and db id
	fmt.Println("sending gossip to", peer)
	msg := "database contents ID"
	action := "gossip"
	con.SendWithoutEvent(peer, msg, action)

	// state transfer

	// release lock
	gossiping.Unlock()
}

// must put a lock on gossiping so only one node at a time can gossip with us
func (proto *Protocol) RecvGossip(peer string, msg string, context map[string]int, con consensus.ConsensusEngine) {

	// get lock
	gossiping.Lock()
	// if db id differs:
		// compare vc to resolve

	fmt.Println("gossiping with", peer)


	// resolve vcs
	need_to_update := con.ValidDeliveryLocal(peer, context)

	fmt.Println("Do we need to update our datastore:", need_to_update)

	fmt.Println(context)
	con.PrintVC()

	// release lock
	gossiping.Unlock()
}

func (proto *Protocol) ChooseNode() string {

	if len(proto.not_seen) == 0 {
		return "end of nodes"
	}

	i := rand.Intn(len(proto.not_seen))
	
	peer := proto.not_seen[i] 
	proto.not_seen = proto.delete_index(i)

	return peer
}

func (proto Protocol) delete_index(i int) []string {
	if len(proto.not_seen) == 0 {
		fmt.Println("panic")
	}
	proto.not_seen[i] = proto.not_seen[len(proto.not_seen)-1]
	proto.not_seen[len(proto.not_seen)-1] = "p"               // Erase last element (write zero value).
	proto.not_seen = proto.not_seen[:len(proto.not_seen)-1]   // Truncate slice.
	return proto.not_seen
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

	// if this is node0, send packet to node1
	if index == 0 {
		initAddr := oracle.view[index+1]

		// send message to this node
		con.SendWithoutEvent(initAddr, proto.msg, proto.action)
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
		con.SendWithoutEvent(addr, proto.msg, proto.action)
	}

	return nil
}
