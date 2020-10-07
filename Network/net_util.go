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
	Addr string
    Send_port int
    Recv_port int
    Buffer int
    timeout int
}

type TCP struct {
    Port int
    Buffer int
    Timeout int
}

type Msg struct {
	SrcAddr string
	Message string
    Action string
    Context []int
}

// used to implement a recv from blocking call
var wait chan struct{}

//
func (udp *UDP) Init(s_addr string, s_port int, r_port int, buffer int) {
	udp.Addr = s_addr
	udp.Send_port = s_port
	udp.Recv_port = r_port
	udp.Buffer = buffer
}

//
func(udp *UDP) FormatAddr(addr string) string {
	if strings.Contains(addr, ":") {
		host := strings.Split(addr, ":")[0]

		return host
	}
	return addr
} 

func (udp *UDP) FormatMsg(msg string, action string, context []int) []byte {
	Msg := Msg{SrcAddr:udp.Addr, Message:msg, Action:action, Context:context} // create new message 

	var buffer bytes.Buffer
	en := gob.NewEncoder(&buffer)

	if err := en.Encode(Msg); err != nil {
	  panic(err)
	}

	return buffer.Bytes()
}

//
func (udp *UDP) RecvFrom() {
	wait = make(chan struct{})

	// wait until we have been signaled
	<-wait
}

//
func (udp *UDP) Send(raw_addr string, msg string, action string, context []int) error {

    host := udp.FormatAddr(raw_addr)
    payload := udp.FormatMsg(msg, action, context)

    addr := net.UDPAddr{
        Port: udp.Send_port,
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
	_, err = conn.Write(payload)
	return err
}

// Will raise the signal chan releasing any functions waiting for the signal
func (udp *UDP) Signal() {
	fmt.Println("closing channel")
	close(wait)
}

//
func (udp *UDP) SendResponse(conn *net.UDPConn, host_addr string, msg string, action string, context []int) {
	payload := udp.FormatMsg(msg, action, context)

    addr, _ := net.ResolveUDPAddr("udp", host_addr)

    fmt.Println(addr)

    fmt.Println(host_addr)

    _, err := conn.WriteToUDP(payload, addr)
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


