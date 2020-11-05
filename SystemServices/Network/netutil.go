package network

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	msg "kv-store/Messages"
	"net"
	"strings"
)

// Network -> create a network interface that defines general networking functions
type Network interface {
	Send()
	Recv()
	formatAddr()
	ServerDaemon()
}

// UDP ->
type UDP struct {
	Addr    string
	Port    int
	Buffer  int
	timeout int
}

// TCP ->
type TCP struct {
	Port    int
	Buffer  int
	Timeout int
}

// used to implement a recv from blocking call
var wait chan struct{}

// Init ->
func (udp *UDP) Init(sAddr string, port int, buffer int) {
	udp.Addr = sAddr
	udp.Port = port
	udp.Buffer = buffer
}

// Decode ->
func (udp *UDP) Decode(buffer bytes.Buffer) msg.Msg {
	var msgDecode msg.Msg

	d := gob.NewDecoder(&buffer)

	if err := d.Decode(&msgDecode); err != nil {
		panic(err)
	}

	return msgDecode
}

func (udp *UDP) formatAddr(addr string) string {
	if strings.Contains(addr, ":") {
		host := strings.Split(addr, ":")[0]

		return host
	}
	return addr
}

// Encode ->
func (udp *UDP) Encode(Msg msg.Msg) []byte {

	var buffer bytes.Buffer
	en := gob.NewEncoder(&buffer)

	if err := en.Encode(Msg); err != nil {
		panic(err)
	}

	return buffer.Bytes()
}

// RecvFrom ->
func (udp *UDP) RecvFrom() {
	wait = make(chan struct{})

	// wait until we have been signaled
	<-wait
}

// Send ->
func (udp *UDP) Send(Addr string, Msg msg.Msg) error {

	host := udp.formatAddr(Addr)
	payload := udp.Encode(Msg)

	addr := net.UDPAddr{
		Port: udp.Port,
		IP:   net.ParseIP(host),
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

// Signal -> Will raise the signal chan releasing any functions waiting for the signal
func (udp *UDP) Signal() {
	fmt.Println("closing channel")
	close(wait)
}
