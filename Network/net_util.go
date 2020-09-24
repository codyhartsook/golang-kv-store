package Network

import (
	"fmt"
	"net"
	"log"
	"bufio"
)

type Network interface {
    Send()
    Recv()
}

type UDP struct {
    port string
    buffer int
    timeout int
}

type TCP struct {
    port string
    buffer int
    timeout int
}

func (udp *UDP) Init(port string, buffer int) {
	udp.port = port
	udp.buffer = buffer
}

func (udp *UDP) Send(host string, msg string) error {
    p :=  make([]byte, udp.buffer)
    addr := host + ":" + udp.port

    conn, err := net.Dial("udp", addr)

    if err != nil {
        fmt.Printf("Some error %v", err)
        return
    }

    fmt.Fprintf(conn, msg)

    _, err = bufio.NewReader(conn).Read(p)

    if err == nil {
        fmt.Printf("%s\n", p)
    } else {
        fmt.Printf("Some error %v\n", err)
    }
    conn.Close()
    return nil
}

func (udp *UDP) Recv(msg string) error {
    p := make([]byte, udp.buffer)

    addr := net.UDPAddr{
        Port: udp.port,
        IP: net.ParseIP("127.0.0.1"),
    }

    ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return
    }
    for {
        _, remoteaddr,err := ser.ReadFromUDP(p)
        fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
        
        if err !=  nil {
            fmt.Printf("Some error  %v", err)
            continue
        }
        go sendResponse(ser, remoteaddr, msg)
    }

	return nil
}

func (udp *UDP) sendResponse(conn *net.UDPConn, addr *net.UDPAddr, msg string) {
    _,err := conn.WriteToUDP([]byte(msg), addr)
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


