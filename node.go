package main

import (
    "fmt"
    "log"
    "strconv"
    protocols "kv-store/Protocols"
    consensus "kv-store/Consensus"
    database "kv-store/Database"
    "net/http"
    "net"
    "errors"
    "strings"
    "bytes"
    "encoding/json"
    "os"
)

// Define node structure in order to provide access to the database and 
// network fucntions wrapper
type Node struct {
    oracle protocols.Orchestrator
    c_engine consensus.ConsensusEngine
    id string
    ip string
    index int
    peers []string
    db database.DB
    actions map[string]interface{}
    buffer string
}

func ParseEnv() (string, []string, string, int, int, error) {
    // exctract the initial view of the system from the os environment
    addr := os.Getenv("ADDRESS")

    if addr == "" {
        err := fmt.Errorf("os environment variables not set")
        panic(err)
    }

    view := strings.Split(os.Getenv("VIEW"), ",")
    repl_factor, _ := strconv.Atoi(os.Getenv("REPL_FACTOR"))
    ip := strings.Split(addr, ":")[0]
    port, _ := strconv.Atoi(strings.Split(addr, ":")[1])

    return addr, view, ip, port, repl_factor, nil
}

// initialize the key-value store
func NewNode() (*Node, error) {
    node := new(Node)

    addr, view, ip, port, repl_factor, err := ParseEnv()

    if err != nil {
        return node, err
    }
    
    node.id = addr
    node.peers = view

    // create database
    db := database.NewDB()
    node.db = *db

    oracle := protocols.NewOrchestrator(node.id, node.peers, repl_factor)
    node.oracle = *oracle

    c_engine := consensus.NewConsensusEngine(ip, port, node.oracle.NumReplicas(), node.peers)
    node.c_engine = *c_engine

    node.oracle.InitiateConsensus(node.c_engine)

    // construct function mapping 
    m := map[string]interface{} {
        "signal": node.c_engine.Signal,
        "read": "",
        "put": node.RemotePut,
        "get": node.RemoteGet,
    }
    node.actions = m

    return node, nil
}

// This function listens to clients as a go routine and hands off
// any requests to the request handler.
func (node *Node) ServerDaemon() error {
    
    p := make([]byte, 1024)
    oob := make([]byte, 1024)
    buffer := bytes.NewBuffer(p)

    // listen to all addresses
    addr := net.UDPAddr{
        Port: node.c_engine.Net.Port,
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

        if len(buffer.Bytes()) > 0 {
            go node.MessageHandler(*buffer) // determine what to do with this packet
        }
    }

    return nil
}

// Handle internal messages between shards and replicas
func (node *Node) MessageHandler(buffer bytes.Buffer) error {
    msg_decode := node.c_engine.Net.Decode(buffer)

    // update vector clock
    node.c_engine.Increment(msg_decode.SrcAddr)

    action := string(msg_decode.Action)

   // loop through actions map
   for k, v := range node.actions {
        if k != action {
            continue
        }
        
        switch k {

        case "signal":
            v.(func())()

        case "put":
            key := strings.Split(msg_decode.Message, ":")[0]
            val := strings.Split(msg_decode.Message, ":")[1]
            v.(func(string, string))(key, val)

        case "get":
            v.(func(string, string))(msg_decode.SrcAddr, msg_decode.Message)

        case "read":
            // publish a message to the causal consensus engine
            node.c_engine.Deliver(msg_decode)
            
        default:
            fmt.Println("case_default")
            fmt.Println(k, v)
        }
    }

   return nil
}

// This node has a specified key, retreive it and send it back to client node
func (node *Node) RemoteGet(src_addr string, key string) {
    got, err := node.db.Get(key)

    if err != nil {
        fmt.Println("This node does not have the key", key)
    }

    msg := string(got)
    action := "read"

    fmt.Println("sending message back to source", src_addr)
    // send value back to source node
    node.c_engine.Send(src_addr, msg, action)
}

// Insert the key, value pair into our local database
func (node *Node) RemotePut(key string, val string) {
    fmt.Println("putting key->val into my database...")

    node.db.Put(key, val) 
}

/* 
 * HTTP user endpoints
 */

// format key-value user entries
type Entry struct {
    Key         string `json:"Key"`
    Value       string `json:"Value"`
}

type Key struct {
    Key string `json:"Key"`
}

type Value struct {
    Value string `json:"Value"`
}

// display root message
func (node *Node) stateHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Node id: {%s}\n", node.id)
    fmt.Fprintf(w, "Node status: running\n")
    fmt.Fprintf(w, "shards:", node.oracle.Shard_groups)
    fmt.Fprintf(w, "Database state:\n")

    node.db.AllPairs(w)
}

// Put a new key, val into the database
func (node *Node) putHandler(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close()

    // cast the request 
    var newEntry Entry
    err := json.NewDecoder(r.Body).Decode(&newEntry)

    if err != nil {
        http.Error(w, err.Error(), 500)
        return 
    }

    store_local := node.oracle.Put(newEntry.Key, newEntry.Value)

    // put key-val in our database
    if store_local {
        node.db.Put(newEntry.Key, newEntry.Value) 
    }
}

// Get a key from the distributed database
// Send a get request to all replicas within the correct shard
// Wait to get request back from 2/3 of shard replicas
// Perform causal consensus on responses
func (node *Node) getHandler(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close()

    var thisKey Key
    err := json.NewDecoder(r.Body).Decode(&thisKey)

    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    // start causal engine
    node.c_engine.NewEventStream()

    // Request this key from each replica in the correct shard
    is_local := node.oracle.Get(thisKey.Key)
    
    // we are the correct shard, consider our key-val entry
    if is_local {
        got, _ := node.db.Get(thisKey.Key)
        strEntry := string(got[:])

        my_cpy := node.c_engine.Encode(node.ip, strEntry, "")
        node.c_engine.Deliver(my_cpy)
    }

    result := node.c_engine.OrderEvents() // blocking call

    output, err := json.Marshal(result.Message)
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("content-type", "application/json")
    w.Write(output)
}

/*
 * Start the client side API
*/
func main() {
    fmt.Println("Starting the distributed key values store...")
    fmt.Println("Using port:13800")

    // create the node instance
    node, nodeStatus := NewNode()
    errorHandler(nodeStatus)

    // run the server daemon in the background
    go node.ServerDaemon()
    
    // use the peer to peer connectivity protocol to ensure all nodes up
    //p2p := protocols.NewChainMessager()
    //p2p.ChainMsg(node.oracle, node.c_engine) // wait till all replicas are up

    // returns the contents of the database and any node info
    http.HandleFunc("/kv-store/snapshot", node.stateHandler)

    // define a values endpoint
    http.HandleFunc("/kv-store/put-key", node.putHandler)

    // define an endpoint for getting keys
    http.HandleFunc("/kv-store/get-key", node.getHandler)
    
    err := http.ListenAndServe(":13800", nil)
    log.Print(err)
    errorHandler(err)

}

func errorHandler(err error){
    if err!=nil { 
        fmt.Println(err)
        //os.Exit(1)
    }
}