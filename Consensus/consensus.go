package P2P

import (
	netutil "kv-store/Network"
	"fmt"
	"strings"
)

var messages chan netutil.Msg // buffered channel of messages

type ConsensusEngine struct {
	vector_clock map[string]int
	message_capacity int
	quorum_req float32
	consensus_req int
	seen map[string]int
	addr string
	Net netutil.UDP
	Msg netutil.Msg
}

func NewConsensusEngine(ip string, port int, replicas int, view []string) *ConsensusEngine {
	c := new(ConsensusEngine)
	c.vector_clock = make(map[string]int)
	c.addr = ip

	for _, node := range view {
		key := strings.Split(node, ":")[0]
		c.vector_clock[key] = 0
	}


	c.quorum_req = 0.66
    c.consensus_req = int(float32(replicas) * c.quorum_req + 0.55)
    fmt.Println("Using quorum requirement of", c.consensus_req, "replicas")

    // create a udp utility
    udp := new(netutil.UDP)
    udp.Init(ip, port, 1024)
    c.Net = *udp

	return c
}

// Provide a wrapper for any networking functions needed to send and recv messages.
// This enables vector clocks to be appended to the payload.
func (c *ConsensusEngine) Send(raw_addr string, msg string, action string) {

	// update my clock
	c.Increment(c.addr)

	c.Net.Send(raw_addr, msg, action, c.vector_clock)
}

func (c *ConsensusEngine) SendWithoutEvent(raw_addr string, msg string, action string) {
	c.Net.Send(raw_addr, msg, action, c.vector_clock)
}

// wraper for the netutil function, is a blocking call
func (c *ConsensusEngine) RecvFrom() {
	c.Net.RecvFrom()
}

// wrapper for the netutil function
func (c *ConsensusEngine) Signal() {
	c.Net.Signal()
}

func (c *ConsensusEngine) Encode(src string, msg string, action string) netutil.Msg {
	return c.Net.Encode(src, msg, action, c.vector_clock)
}

func (c *ConsensusEngine) PrintVC() {
	fmt.Println(c.vector_clock)
}

// Provides a messaging construct for to implement Quorum replication among shards
func (c *ConsensusEngine) NewEventStream() {
	c.message_capacity = c.consensus_req
	c.seen = make(map[string]int)
	messages = make(chan netutil.Msg, c.message_capacity)
}

// Publish new message to channel until we reach bound of buffer
func (c *ConsensusEngine) Deliver(new_msg netutil.Msg) {

	if c.message_capacity <= 0 {
		return
	}

	// dont consider repeat messages from any node
	_, ok := c.seen[new_msg.SrcAddr]
	if ok {
		return
	}

	select {
	    case messages <- new_msg:
	    	// Put this nodes into map of seen responses
	    	c.seen[new_msg.SrcAddr] = 1
	    default:
	        close(messages)
    }

    c.message_capacity-- // decrement how many more messages we need to see

    if c.message_capacity == 0 {
		close(messages)
	} 
}

// Consume all messages in channel and compare causal context
func (c *ConsensusEngine) OrderEvents() netutil.Msg {
	
	var Nil map[string]int
	var highest_priority_msg = netutil.Msg{SrcAddr:"", Message:"", Action:"", Context:Nil}

	for msg := range messages {
		
		// if the value of the two messages are the same, dont check vectors
		if c.IdenticalValue(highest_priority_msg.Message, msg.Message) {
			continue
		}
		
		if c.ValidDelivery(msg.SrcAddr, msg.Context, highest_priority_msg.SrcAddr, highest_priority_msg.Context) {
			highest_priority_msg = msg
		}
	}

	fmt.Println("\n chosen:", highest_priority_msg)
	return highest_priority_msg
}

// if the message value is the same we dont need to check vector clocks
// compare strings for equality
func (c *ConsensusEngine) IdenticalValue(m1 string, m2 string) bool {
	if m1 != m2 {
		return false
	} 

	return true
}

/*
	The following functions implement causal broadcast by utilizing vector clocks.
*/

// Update the vector clock for this node
func (c *ConsensusEngine) Increment(src_node string) {
	_, ok := c.vector_clock[src_node]

	if ok {
		c.vector_clock[src_node]++
	} else {
		//c.vector_clock[node] = 0
		fmt.Println("Cant Increment index of node not in view!")
		fmt.Println(src_node, "\n", c.vector_clock)
	}
}


// Determine if the given vector clock causaly happened before my vector clock
func (c *ConsensusEngine) CheckIndices(new_vc map[string]int, prev_vc map[string]int) (bool, bool) {

    // check each key, val to compare the greater history
    nul := true
    for index, value := range prev_vc {

    	if value != 0 {
    		nul = false
    	}
        
    	val, ok := new_vc[index]

		if ok == false && value != 0 {
			return false, nul
		} 

    	if ok && val < value {
    		return false, nul
    	}
    }
 
    return true, nul
}

// compare two vector clocks and determine if the new clock -> old clock
func (c *ConsensusEngine) ValidDelivery(new_node string, new_vc map[string]int, pev_node string, prev_vc map[string]int) bool {
	/* 
		Causal broadcast algorithm: delivery valid iff
			1. message from nodeA: c.vector_clock[nodeA] == remote_vc[nodeA] + 1

			2. for each element c.vecor_clock <= remote_vc

	*/

	if len(new_vc) < len(prev_vc) {
        return false
    }

    valid, empty := c.CheckIndices(new_vc, prev_vc) // compare element wise
    
    if valid && empty { // check if our vector clock is empty
    	return true
    }

    val, ok := prev_vc[new_node]

    if ok {
    	if val != (new_vc[new_node] + 1) { // compare first condition of causal broadcast
    		return false
    	}
    }
    
    return valid
}

func (c *ConsensusEngine) ValidDeliveryLocal(new_node string, new_vc map[string]int) bool {
	return c.ValidDelivery(new_node, new_vc, c.addr, c.vector_clock)
}

