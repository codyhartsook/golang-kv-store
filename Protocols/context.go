package P2P

import (
	netutil "kv-store/Network"
	"fmt"
)

var messages chan netutil.Msg

type CausalEngine struct {}

func NewCausalEngine(num_messages int) *CausalEngine {
	c := new(CausalEngine)

	messages = make(chan netutil.Msg, num_messages)
	return c
}

// Publish new message to channel until we reach bound of buffer
func (c *CausalEngine) Publish(new_msg netutil.Msg) {
	select {
	    case messages <- new_msg: // Put new message in the channel unless it is full
	    default:
	        fmt.Println("Channel full. Discarding Message")
    }
}

// Consume all messages in channel and compare causal context
func (c *CausalEngine) OrderEvents() netutil.Msg {
	var Nil []int
	var highest_priority_msg = netutil.Msg{SrcAddr:"", Message:"", Action:"", Context:Nil}

	for msg := range messages {
		if c.ValidRead(msg.Context, highest_priority_msg.Context) {
			highest_priority_msg = msg
		}
	}

	return highest_priority_msg
}

// compare two vector clocks and determine if the new clock -> old clock
func (c *CausalEngine) ValidRead(vc_new []int, vc_old []int) bool {
	if len(vc_new) > len(vc_old) {
		return true
	} else if len(vc_new) < len(vc_old) {
		return false
	}

	return true
}

func (c *CausalEngine) ValidWrite() bool {
	return true
}

