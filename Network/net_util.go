package Network

import (
	"fmt"
	"net"
	"time"
	"errors"
	"os"
)

// create a network interface that defines general networking functions
type Network interface {
    Send()
    Recv()
}

type UDP struct {
    port int
    buffer int
    timeout int
}

type TCP struct {
    port int
    buffer int
    timeout int
}

func (udp *UDP) Init(port int, buffer int) {
	udp.port = port
	udp.buffer = buffer
}

func (udp *UDP) Send(host string, msg string) error {

    payload :=  make([]byte, udp.buffer) // make new buffer

    addr := net.UDPAddr{
        Port: udp.port,
        IP: net.ParseIP(host),
    }

    //net.DialUDP("udp", nil, raddr)
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

func (udp *UDP) Recv(total_packets int, timeout int) (error, *net.UDPConn) {
    p := make([]byte, udp.buffer)

    // listen to all addresses
    addr := net.UDPAddr{
        Port: udp.port,
        IP: net.ParseIP("127.0.0.1"),
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

        _, remoteaddr, err := conn.ReadFromUDP(p)

        if err != nil {
	       if e, ok := err.(net.Error); !ok || !e.Timeout() {
	           // handle error, it's not a timeout
	           fmt.Println("error getting request:", err)
	       }
	       return err, conn // this read was timed out
	   }

        fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
    }

	return nil, conn
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


