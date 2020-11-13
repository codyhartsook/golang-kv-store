package consensus

import (
	"bytes"
	"fmt"
	"io"
	log "kv-store/Logging"
	msg "kv-store/Messages"
	netutil "kv-store/SystemServices/Network"
	"strconv"
	"strings"
	"sync"
	"time"
)

var logger log.AsyncLog // define our logging suite

// ConEngine -> Provides an interface to contstruct causaly consistent reads and writes.
type ConEngine struct {
	vectorClock map[string]int
	streams     map[string]chan msg.Msg
	quorumReq   int
	addr        string
	netutil.UDP
}

// NewConEngine -> Construct a new consensus manager
func (c *ConEngine) NewConEngine(ip string, port int, replicas int, view []string) {
	c.vectorClock = make(map[string]int)
	c.streams = make(map[string]chan msg.Msg)
	c.addr = ip

	logger = *log.New(nil) // create logger
	go logger.Start()

	// initialize vector clock
	for _, node := range view {
		key := strings.Split(node, ":")[0]
		c.vectorClock[key] = 0
	}

	// we require a majority of replicas to respond
	c.quorumReq = int(replicas/2) + 1
	fmt.Printf("Using quorum requirement of %d replicas with a view of %d replicas\n", c.quorumReq, replicas)

	// create a udp utility
	c.UDP.Init(ip, port, 1024)
}

// Send -> Provide a wrapper for any networking functions needed to send and recv messages.
// This enables vector clocks to be appended to the payload.
func (c *ConEngine) Send(addr string, Msg msg.Msg) {
	Msg = c.Encode(Msg)

	// update my clock
	c.Increment(c.addr)
	c.Send(addr, Msg)
}

// SendWithoutEvent -> Dont update the vector clock
func (c *ConEngine) SendWithoutEvent(addr string, Msg msg.Msg) {
	c.Send(addr, Msg)
}

// RecvFrom -> wraper for the netutil function, is a blocking call
func (c *ConEngine) RecvFrom() {
	c.RecvFrom()
}

// Signal -> wrapper for the netutil function
func (c *ConEngine) Signal() {
	c.Signal()
}

// Encode -> Add our vector clock to the message
func (c *ConEngine) Encode(Msg msg.Msg) msg.Msg {
	Msg.Context = c.vectorClock
	return Msg
}

// PrintVC ->
func (c *ConEngine) PrintVC() {
	fmt.Println(c.vectorClock)
}

// generateID -> return a unique id for this node at this time
func (c *ConEngine) generateID() string {
	t := strconv.FormatInt(time.Now().UnixNano(), 10)
	id := (t + c.addr)
	return id
}

/*
	The following functions implement causal broadcast by utilizing vector clocks.

	Causal broadcast algorithm: delivery valid iff
		1. message from nodeA: c.vectorClock[nodeA] == remote_vc[nodeA] + 1

		2. for each element c.vecor_clock <= remote_vc

*/

// NewEventStream -> Given a key get request, contact the correct replicas for the
// value associated to that key. We can define how relationship between availability
// and consistency by determining how many replicas we need to hear from before we
// return to the client with the retrived value.
func (c *ConEngine) NewEventStream() string {

	id := c.generateID()
	message := make(chan msg.Msg, c.quorumReq)

	// generate a unique channel id
	// gain exclusive access before we add to map of channels
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	c.streams[id] = message

	return id
}

// Deliver -> This function indicates we have received a response from a replica during
// a get request. Send a message into the channel associated with this get request.
func (c *ConEngine) Deliver(newMsg msg.Msg) error {

	thisChan, ok := c.streams[newMsg.ID]

	if !ok {
		return fmt.Errorf("ID provided in message does not exist in map of channels %s", newMsg.ID)
	}

	select {
	case thisChan <- newMsg:
		logger.Write("Message from " + newMsg.SrcAddr + " was accepted")
	default:
		logger.Write("Channel capacity is full")
	}

	return nil
}

// OrderEvents -> Consume all messages in channel and compare causal context
// before returning the most up to date read. Each message in this channel
// is from a separate shard replica.
func (c *ConEngine) OrderEvents(id string) (msg.Msg, error) {

	var Nil map[string]int
	var p io.Reader
	var highestPriorityMsg = msg.Msg{SrcAddr: "", Payload: p, ID: "", Action: "", Context: Nil}

	messages, ok := c.streams[id]
	if !ok {
		return highestPriorityMsg, fmt.Errorf("Stream id %s not in map of streams", id)
	}

	// read all responses from the specified number of replicas
	seen := 0
	for thisMsg := range messages {

		logger.Write("Consuming message and comparing clocks, msg src: " + thisMsg.SrcAddr)

		// if the value of the two messages are the same, dont check vectors
		if !c.IdenticalValue(highestPriorityMsg.Payload, thisMsg.Payload) {
			if c.ValidDelivery(thisMsg.SrcAddr, thisMsg.Context, highestPriorityMsg.SrcAddr, highestPriorityMsg.Context) {
				// update which read we should return
				highestPriorityMsg = thisMsg
			}
		}

		seen++
		if seen >= c.quorumReq {
			close(messages)
			break
		}
	}

	// gain exclusive access to our map of channels before we delete this read stream id
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	delete(c.streams, id)

	return highestPriorityMsg, nil
}

// IdenticalValue -> if the message value is the same we dont need to
// check vector clocks
func (c *ConEngine) IdenticalValue(m1, m2 io.Reader) bool {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	buf1.ReadFrom(m1)
	buf2.ReadFrom(m2)

	if buf1.String() != buf2.String() {
		return false
	}
	return true
}

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

// CheckIndices -> Determine if the first vector clock causaly happened before the second vector clock
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
