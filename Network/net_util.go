package Network

import (
	"fmt"
	"net"
	"log"
)

type Network interface {
    Send()
    Recv()
}

type UDP struct {
    port string
    buffer int
}

type TCP struct {
    port string
    buffer int
}

func (udp *UDP) Init(port string, buffer int) {
	udp.port = port
	udp.buffer = buffer
}

func (udp *UDP) Send() error {
    fmt.Println("sending message")

    return nil
}

func (udp *UDP) Recv() error {
    // listen to incoming udp packets
    recvAddr := ":" + udp.port
    
	pc, err := net.ListenPacket("udp", recvAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer pc.Close() // close socket before function returns

	for {
		buf := make([]byte, udp.buffer)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			continue
		}
		go udp.Write(pc, addr, buf[:n])
	}

	return nil
}

func (udp *UDP) Write(pc net.PacketConn, addr net.Addr, buf []byte) {
	// 0 - 1: ID
	// 2: QR(1): Opcode(4)
	buf[2] |= 0x80 // Set QR bit

	pc.WriteTo(buf, addr)
}