package node

import (
	"bytes"
	"errors"
	"fmt"
	database "kv-store/Database"
	msg "kv-store/Messages"
	consensus "kv-store/SystemServices/Consensus"
	protocols "kv-store/SystemServices/SysProtocols"
	log "kv-store/Logging"
	"net"
	"os"
	"strconv"
	"strings"
)

var logger log.AsyncLog

// Node -> Define node structure in order to provide access to the database and
// network fucntions wrapper
type Node struct {
	Oracle    protocols.Orchestrator
	ConEngine consensus.ConEngine
	Protocols protocols.Protocol
	ID        string
	Port      int
	IP        string
	index     int
	peers     []string
	DB        database.DB
	actions   map[string]interface{}
	buffer    string
}

// parseEnv -> exctract the initial view of the system from the os environment
func parseEnv() (string, []string, string, int, int, error) {
	addr := os.Getenv("ADDRESS")

	if addr == "" {
		err := errors.New("os environment variables not set")
		panic(err)
	}

	view := strings.Split(os.Getenv("VIEW"), ",")
	replFactor, _ := strconv.Atoi(os.Getenv("REPL_FACTOR"))
	ip := strings.Split(addr, ":")[0]
	port, _ := strconv.Atoi(strings.Split(addr, ":")[1])

	return addr, view, ip, port, replFactor, nil
}

// NewNode -> initialize a node structure and the dependent protocols
func NewNode() (*Node, error) {
	node := new(Node)

	addr, view, ip, port, replFactor, err := parseEnv()

	if err != nil {
		return node, err
	}

	node.ID = addr
	node.Port = port
	node.IP = ip
	node.peers = view

	// create database, partitioner and consensus engine
	node.DB = *database.NewDB()
	node.Oracle = *protocols.NewOrchestrator(node.ID, node.peers, replFactor)

	var peerReps []string
	var ok error
	peerReps, ok = node.Oracle.PeerReplicas(node.IP)
	if ok != nil {
		return node, ok
	}

	var numReps int
	numReps, ok = node.Oracle.NumReplicas()
	if ok != nil {
		return node, ok
	}
	node.ConEngine = *consensus.NewConEngine(ip, port, numReps, node.peers)
	node.Oracle.AddConsensusEngine(node.ConEngine)
	node.Protocols = *protocols.NewProtocol(node.IP, peerReps)

	// construct function mapping
	node.actions = map[string]interface{}{
		"signal": node.ConEngine.Signal,
		"read":   "",
		"put":    node.RemotePut,
		"get":    node.RemoteGet,
		"gossip": node.Protocols.RecvGossip,
	}

	logger = *log.New(nil) // create logger
	go logger.Start()

	return node, nil
}

// Info -> Print some node metadata
func Info() {
	logger.("Getting info for this node.")
}

// ServerDaemon -> listens to clients as a go routine and hands off
// any requests to the request handler.
func (node *Node) ServerDaemon() error {

	p := make([]byte, 1024)
	oob := make([]byte, 1024)
	buffer := bytes.NewBuffer(p)

	// listen to all addresses
	addr := net.UDPAddr{
		Port: node.ConEngine.Net.Port,
		IP:   net.ParseIP("0.0.0.0"),
	}

	conn, err := net.ListenUDP("udp", &addr)
	defer conn.Close() // close connection when function returns

	if err != nil {
		return fmt.Errorf("Failed to create socket %g", err)
	}

	// continuously listen to our connection
	for {
		_, _, _, _, err = conn.ReadMsgUDP(buffer.Bytes(), oob)

		if err != nil {
			return fmt.Errorf("ReadMsgUDP error %g", err)
		}

		// we got a packet, determine which action to take
		if len(buffer.Bytes()) > 0 {
			go node.MessageHandler(*buffer)
		}
	}
}

// MessageHandler -> Handle internal messages between shard replicas
func (node *Node) MessageHandler(buffer bytes.Buffer) error {
	msgDecode := node.ConEngine.Net.Decode(buffer)

	action := string(msgDecode.Action)

	// loop through actions map
	for k, v := range node.actions {
		if k != action {
			continue
		}

		switch k {

		case "signal":
			v.(func())()

		case "put":
			key := strings.Split(msgDecode.Payload, ":")[0]
			val := strings.Split(msgDecode.Payload, ":")[1]

			// update vector clock
			node.ConEngine.Increment(msgDecode.SrcAddr)
			v.(func(string, string))(key, val)

		case "get":
			// update vector clock
			node.ConEngine.Increment(msgDecode.SrcAddr)
			v.(func(msg.Msg))(msgDecode)

		case "read":
			// publish a message to the causal consensus engine
			node.ConEngine.Deliver(msgDecode)

		case "gossip":
			v.(func(msg.Msg, consensus.ConEngine))(msgDecode, node.ConEngine)

		default:
			logger.Write("case_default")
		}
	}

	return nil
}

// RemoteGet -> This node has a specified key, retreive it and send it back to client node
func (node *Node) RemoteGet(Msg msg.Msg) {
	got, _ := node.DB.Get(Msg.Payload)

	src := Msg.SrcAddr
	Msg.Payload = string(got)
	Msg.SrcAddr = node.ID
	Msg.Action = "read"

	// send value back to source node
	logger.Write("sending retrieved token: " + Msg.Payload, + " back to " + src)
	node.ConEngine.Send(src, Msg)
}

// RemotePut -> Insert the key, value pair into our local database
func (node *Node) RemotePut(key string, val string) {
	logger.Write("putting key->val into my database...")
	node.DB.Put(key, val)
}

// RunBackendSystem -> run all system level protocols needed to initiate the key value store
func (node *Node) RunBackendSystem() {
	// run the server daemon in the background
	go node.ServerDaemon()

	// use the peer to peer connectivity protocol to ensure all nodes up
	//go node.Protocols.InitGossipProtocol(node.ConEngine)
}
