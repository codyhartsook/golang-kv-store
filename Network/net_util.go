package Network

import (
	"fmt"
	"net"
	"strings"
	"errors"
	"os"
	"bytes"
    "encoding/gob"
)

// create a network interface that defines general networking functions
type Network interface {
    Send()
    Recv()
    FormatAddr()
    ServerDaemon()
}

type UDP struct {
    send_port int
    recv_port int
    buffer int
    timeout int
    actions map[string]interface{}
}

type TCP struct {
    port int
    buffer int
    timeout int
}

type Msg struct {
	Message string
    Action string
    Context []int
}

// used to implement a recv from blocking call
var wait chan struct{}

//
func (udp *UDP) Init(s_port int, r_port int, buffer int) {
	udp.send_port = s_port
	udp.recv_port = r_port
	udp.buffer = buffer
	
	m := map[string]interface{} {
    	"signal": udp.Signal,
    	"send": udp.Send,
  	}
  	udp.actions = m
}

//
func(udp *UDP) FormatAddr(addr string) string {
	if strings.Contains(addr, ":") {
		host := strings.Split(addr, ":")[0]

		return host
	}
	return addr
} 

//
func (udp *UDP) RecvFrom() {
	wait = make(chan struct{})

	// wait until we have been signaled
	<-wait
}

//
func (udp *UDP) Send(raw_addr string, msg string, action string, context []int) error {
	Msg := Msg{Message:msg, Action:action, Context:context} // create new message 

	var buffer bytes.Buffer
	en := gob.NewEncoder(&buffer)

	if err := en.Encode(Msg); err != nil {
	  panic(err)
	}

    host := udp.FormatAddr(raw_addr)

    addr := net.UDPAddr{
        Port: udp.send_port,
        IP: net.ParseIP(host),
    }

    conn, err := net.DialUDP("udp", nil, &addr) // bind udp socket

    defer func() {
		if err := conn.Close(); err != nil {
			fmt.Println("failed while closing connection:", err)
		}
	}()

    if err != nil {
		fmt.Printf("failed to connect: %v\n", err)
		errN := errors.New("Failed to connect")
		return errN
	}

	// send time request
	_, err = conn.Write(buffer.Bytes())
	return err
}

// This function listens to clients as a go routine and hands off
// any requests to the request handler.
func (udp *UDP) ServerDaemon() error {
	p := make([]byte, udp.buffer)
	oob := make([]byte, udp.buffer)
	buffer := bytes.NewBuffer(p)

	// listen to all addresses
    addr := net.UDPAddr{
        Port: udp.recv_port,
        IP: net.ParseIP("0.0.0.0"),
    }

    conn, err := net.ListenUDP("udp", &addr)
    defer conn.Close()

    if err != nil {
        fmt.Println("failed to create socket:", err)
        return errors.New("Failed to create socket")
    }

    // run indefinately
    for {

	    _, _, _, _, rerr := conn.ReadMsgUDP(buffer.Bytes(), oob)

	    if rerr != nil {
	    	fmt.Println("ReadMsgUDP error", rerr)
	    }

	    go udp.MessageHandler(*buffer, conn) // determine what to do with this packet
	}

	return nil
}

//
func (udp *UDP) MessageHandler(buffer bytes.Buffer, conn *net.UDPConn) error {
	var msg_decode Msg

	d := gob.NewDecoder(&buffer)
	
	if err := d.Decode(&msg_decode); err != nil {
	  panic(err)
	}
 
   fmt.Println("Decoded Struct \n", msg_decode.Message,"\n", msg_decode.Action)

   // loop through actions map
   for k, v := range udp.actions {
        switch k {

        case "signal":
            v.(func())()
        }
    }

   return nil
}

// Will raise the signal chan releasing any functions waiting for the signal
func (udp *UDP) Signal() {
	close(wait)
}

//
func (udp *UDP) SendResponse(conn *net.UDPConn, addr *net.UDPAddr, msg string) {
    _, err := conn.WriteToUDP([]byte(msg), addr)
    if err != nil {
        fmt.Printf("Couldn't send response %v", err)
    }
}

//
func (udp *UDP) errorHandler(err error, action string) {
    if err != nil { 
        fmt.Println(err)

        if action == "terminate" {
        	os.Exit(1)
        } 
    }
}


