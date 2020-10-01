package Network

import (
	"fmt"
	"net"
	"time"
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

func (udp *UDP) Init(s_port int, r_port int, buffer int) {
	udp.send_port = s_port
	udp.recv_port = r_port
	udp.buffer = buffer
}

func(udp *UDP) FormatAddr(addr string) string {
	if strings.Contains(addr, ":") {
		host := strings.Split(addr, ":")[0]

		return host
	}
	return addr
} 

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

func (udp *UDP) Recv(total_packets int, timeout int) (error, *net.UDPConn) {
    p := make([]byte, udp.buffer)
    oob := make([]byte, udp.buffer)

    // listen to all addresses
    addr := net.UDPAddr{
        Port: udp.recv_port,
        IP: net.ParseIP("0.0.0.0"),
    }

    conn, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Println("failed to create socket:", err)
        return errors.New("Failed to create socket"), nil
    }

    // receive a specified number of packets
    packets_recv := 0

    // set a read timeout
    conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Second))

    for packets_recv < total_packets {

        _, _, _, _, err := conn.ReadMsgUDP(p, oob)
        msg := string(p)

        fmt.Printf("Received a message!", msg)

        if err != nil {
	       if e, ok := err.(net.Error); !ok || !e.Timeout() {
	           // handle error, it's not a timeout
	           fmt.Println("error getting request:", err)
	       }
	       return err, conn // this read was timed out
	   }

	   packets_recv++
    }

	return nil, conn
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
    if err != nil {
        fmt.Println("failed to create socket:", err)
        return errors.New("Failed to create socket")
    }

    _, _, _, _, rerr := conn.ReadMsgUDP(buffer.Bytes(), oob)

    udp.ActionHandler(*buffer) // determine what to do with this packet

    return rerr
}

func (udp *UDP) ActionHandler(buffer bytes.Buffer) error {
	var msg_decode Msg

	d := gob.NewDecoder(&buffer)
	
	if err := d.Decode(&msg_decode); err != nil {
	  panic(err)
	}
 
   fmt.Println("Decoded Struct ", msg_decode.Message,"\t", msg_decode.Action)

   return nil
}

func (udp *UDP) SendResponse(conn *net.UDPConn, addr *net.UDPAddr, msg string) {
    _, err := conn.WriteToUDP([]byte(msg), addr)
    if err != nil {
        fmt.Printf("Couldn't send response %v", err)
    }
}

func (udp *UDP) errorHandler(err error, action string){
    if err != nil { 
        fmt.Println(err)

        if action == "terminate" {
        	os.Exit(1)
        } 
    }
}


