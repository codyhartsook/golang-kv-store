package consensus

import (
	"fmt"
	log "kv-store/Logging"
	msg "kv-store/Messages"
	netutil "kv-store/SystemServices/Network"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

var logger log.AsyncLog

//var message chan msg.Msg // buffered channel of messages

// ConEngine ->
type ConEngine struct {
	vectorClock     map[string]int
	messageCapacity int
	streams         map[string]chan msg.Msg
	quorumReq       float64
	consensusReq    int
	addr            string
	Net             netutil.UDP
	Msg             msg.Msg
}

// NewConEngine -> Construct a new consensus manager
func NewConEngine(ip string, port int, replicas int, view []string) *ConEngine {
	c := new(ConEngine)
	c.vectorClock = make(map[string]int)
	c.streams = make(map[string]chan msg.Msg)
	c.addr = ip
	logger = *log.New(nil) // create logger
	go logger.Start()

	for _, node := range view {
		key := strings.Split(node, ":")[0]
		c.vectorClock[key] = 0
	}

	c.quorumReq = 0.66
	c.consensusReq = int(math.Round(((float64(replicas) * c.quorumReq) + 0.5)))
	fmt.Printf("Using quorum requirement of %d replicas with a view of %d replicas\n", c.consensusReq, replicas)

	// create a udp utility
	udp := new(netutil.UDP)
	udp.Init(ip, port, 1024)
	c.Net = *udp

	return c
}

// Send -> Provide a wrapper for any networking functions needed to send and recv messages.
// This enables vector clocks to be appended to the payload.
func (c *ConEngine) Send(addr string, Msg msg.Msg) {
	Msg = c.Encode(Msg)

	// update my clock
	c.Increment(c.addr)
	c.Net.Send(addr, Msg)
}

// SendWithoutEvent -> Dont update the vector clock
func (c *ConEngine) SendWithoutEvent(addr string, Msg msg.Msg) {
	c.Net.Send(addr, Msg)
}

// RecvFrom -> wraper for the netutil function, is a blocking call
func (c *ConEngine) RecvFrom() {
	c.Net.RecvFrom()
}

// Signal -> wrapper for the netutil function
func (c *ConEngine) Signal() {
	c.Net.Signal()
}

// Encode ->
func (c *ConEngine) Encode(Msg msg.Msg) msg.Msg {
	Msg.Context = c.vectorClock
	return Msg
}

// PrintVC ->
func (c *ConEngine) PrintVC() {
	fmt.Println(c.vectorClock)
}

func (c *ConEngine) generateID() string {
	t := strconv.FormatInt(time.Now().UnixNano(), 10)
	id := (t + c.addr)
	return id
}

// NewEventStream -> Provides a messaging construct to implement Quorum replication among shards
func (c *ConEngine) NewEventStream() string {
	c.messageCapacity = c.consensusReq

	id := c.generateID()
	message := make(chan msg.Msg, c.messageCapacity)

	// gain exclusive access before we add to map of channels
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	c.streams[id] = message

	return id
}

// Deliver -> Publish new message to channel until we reach bound of buffer
func (c *ConEngine) Deliver(newMsg msg.Msg) error {

	thisChan, ok := c.streams[newMsg.ID]

	if !ok {
		logger.Write("ID provided in message does not exist in map of channles " + newMsg.ID)
		return fmt.Errorf("ID provided in message does not exist in map of channels %s", newMsg.ID)
	}

	select {
	// Put this node into map of seen responses
	case thisChan <- newMsg:
		logger.Write("Message from " + newMsg.SrcAddr + " was accepted")
	default:
		logger.Write("Channel capacity is full")
	}

	return nil
}

// OrderEvents -> Consume all messages in channel and compare causal context
func (c *ConEngine) OrderEvents(id string) (msg.Msg, error) {

	var Nil map[string]int
	var highestPriorityMsg = msg.Msg{SrcAddr: "", Payload: "", ID: "", Action: "", Context: Nil}

	messages, ok := c.streams[id]
	if !ok {
		return highestPriorityMsg, fmt.Errorf("Stream id %s not in map of streams", id)
	}

	seen := 0
	for thisMsg := range messages {

		logger.Write("Consuming message and comparing clocks, msg src: " + thisMsg.SrcAddr)

		// if the value of the two messages are the same, dont check vectors
		if !c.IdenticalValue(highestPriorityMsg.Payload, thisMsg.Payload) {
			if c.ValidDelivery(thisMsg.SrcAddr, thisMsg.Context, highestPriorityMsg.SrcAddr, highestPriorityMsg.Context) {
				highestPriorityMsg = thisMsg
			}
		}

		seen++
		if seen >= c.messageCapacity {
			close(messages)
			break
		}
	}

	// gain exclusive access to our map of channels before we delete
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	delete(c.streams, id)
	return highestPriorityMsg, nil
}

// IdenticalValue -> if the message value is the same we dont need to
// check vector clocks
func (c *ConEngine) IdenticalValue(m1 string, m2 string) bool {
	if m1 != m2 {
		return false
	}
	return true
}

/*
	The following functions implement causal broadcast by utilizing vector clocks.
*/

// Increment -> Update the vector clock for this node
func (c *ConEngine) Increment(srcNode string) error {
	_, ok := c.vectorClock[srcNode]

	if ok {
		c.vectorClock[srcNode]++
	} else {
		//c.vectorClock[node] = 0
		return fmt.Errorf("Cant Increment index of node not in view %q, %q", srcNode, c.vectorClock)
	}

	return nil
}

// CheckIndices -> Determine if the given vector clock causaly happened before my vector clock
func (c *ConEngine) CheckIndices(newVC map[string]int, prevVC map[string]int) (bool, bool) {

	// check each key, val to compare the greater history
	nul := true
	for index, value := range prevVC {

		if value != 0 {
			nul = false
		}

		val, ok := newVC[index]

		if ok == false && value != 0 {
			return false, nul
		}

		if ok && val < value {
			return false, nul
		}
	}

	return true, nul
}

// ValidDelivery -> compare two vector clocks and determine if the new clock -> old clock
func (c *ConEngine) ValidDelivery(newNode string, newVC map[string]int, pevNode string, prevVC map[string]int) bool {
	/*
		Causal broadcast algorithm: delivery valid iff
			1. message from nodeA: c.vectorClock[nodeA] == remote_vc[nodeA] + 1

			2. for each element c.vecor_clock <= remote_vc

	*/

	if len(newVC) < len(prevVC) {
		return false
	}

	valid, empty := c.CheckIndices(newVC, prevVC) // compare element wise

	if valid && empty { // check if our vector clock is empty
		return true
	}

	val, ok := prevVC[newNode]

	if ok {
		if val != (newVC[newNode] + 1) { // compare first condition of causal broadcast
			return false
		}
	}

	return valid
}

// ValidDeliveryLocal ->
func (c *ConEngine) ValidDeliveryLocal(newNode string, newVC map[string]int) bool {
	return c.ValidDelivery(newNode, newVC, c.addr, c.vectorClock)
}
