package protocols

import (
	"fmt"
	db "kv-store/Database"
	log "kv-store/Logging"
	msg "kv-store/Messages"
	consensus "kv-store/SystemServices/Consensus"
	"math/rand"
	"strconv"
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
	gossipIntervalMs float32
	shardReplicas    []string
	addr             string
	notSeen          []string
	dbRef            db.DB
}

// NewProtocol -> Construct a new system level protocol
func NewProtocol(nodeAddr string, shardReplicas []string, DB db.DB) *Protocol {
	p := new(Protocol)
	p.addr = nodeAddr
	p.shardReplicas = shardReplicas
	p.dbRef = DB

	logger = *log.New(nil) // create logger
	go logger.Start()

	return p
}

// Broadcast -> Send message to each node in parrallel
func (proto *Protocol) Broadcast(view []string, con consensus.ConEngine) {
	proto.msg = "Broadcasting"
	proto.action = "broadcast"

	for _, node := range view {
		thisMsg := msg.Msg{
			SrcAddr: proto.addr,
			Payload: []byte(proto.msg),
			ID:      "",
			Action:  proto.action,
		}
		go con.SendWithoutEvent(node, thisMsg)
	}
}

// scheduling function
func (proto *Protocol) doEvery(d time.Duration, f func(consensus.ConEngine), con consensus.ConEngine) {
	for range time.Tick(d) {

		f(con) // call given function

		if len(proto.notSeen) == 0 {
			logger.Write("Gossip round done.")
			break
		}
	}
}

// InitGossipProtocol -> Every 2 seconds start a gossip round
func (proto *Protocol) InitGossipProtocol(con consensus.ConEngine) {
	gossipRounds := 2000 // 2 seconds between rounds
	interval := time.Duration(gossipRounds) * time.Millisecond

	// start a gossip round every time interval
	for range time.Tick(interval) {
		rand.Seed(time.Now().UTC().UnixNano())

		i := rand.Intn(len(proto.shardReplicas))
		startNode := proto.shardReplicas[i]
		ip := strings.Split(startNode, ":")[0]

		if ip == proto.addr {
			logger.Write("Gossip round starting")

			go proto.startGossipRound(con)
		}
	}
}

// StartGossipRound -> choose a shard replica at random then runt he gossip protocol
func (proto *Protocol) startGossipRound(con consensus.ConEngine) {

	proto.notSeen = make([]string, len(proto.shardReplicas))
	copy(proto.notSeen, proto.shardReplicas)

	// update which nodes we still need to reach
	for i, node := range proto.shardReplicas {
		ip := strings.Split(node, ":")[0]
		if ip == proto.addr {
			proto.notSeen = proto.deleteIndex(i)
		}
	}

	proto.gossipIntervalMs = 100 // 100 milisecond delay between gossips
	rand.Seed(time.Now().UTC().UnixNano())
	interval := time.Duration(proto.gossipIntervalMs) * time.Millisecond

	proto.doEvery(interval, proto.sendGossip, con)
}

// SendGossip -> must put a lock on gossiping so only one node at a time can gossip with us
func (proto *Protocol) sendGossip(con consensus.ConEngine) {

	// choose a shard replica at random
	peer := proto.chooseNode()

	// get lock then release when function returns
	gossiping.Lock()
	defer gossiping.Unlock()

	p, err := proto.dbRef.ToByteArray()

	if err != nil {
		logger.Write(err.Error())
	}

	thisMsg := msg.Msg{
		SrcAddr: proto.addr,
		Payload: p,
		ID:      "",
		Action:  "gossip",
	}

	// send message to peer with vc and db id
	logger.Write("sending gossip to " + peer)
	con.SendWithoutEvent(peer, thisMsg)
}

// RecvGossip -> must put a lock on gossiping so only one node at a time can gossip with us
func (proto *Protocol) RecvGossip(Msg msg.Msg, con consensus.ConEngine, myDB db.DB) {

	// get lock then release when function returns
	gossiping.Lock()
	defer gossiping.Unlock()

	logger.Write("gossiping with " + Msg.SrcAddr)

	// resolve vcs
	needToUpdate := con.ValidDeliveryLocal(Msg.SrcAddr, Msg.Context)
	logger.Write("checking if we need to update our database" + strconv.FormatBool(needToUpdate))

	if needToUpdate {
		// compare and update
		p, err := myDB.ByteArrayToMap(Msg.Payload)
		if err != nil {
			logger.Write(err.Error())
		}

		myDB.MergeDB(p)
	}
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
			Payload: []byte(proto.msg),
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
			Payload: []byte(proto.msg),
			ID:      "",
			Action:  proto.action,
		}
		con.SendWithoutEvent(addr, thisMsg)
	}

	return nil
}
