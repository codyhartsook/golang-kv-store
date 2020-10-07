package main

import (
    "fmt"
    "log"
    "strconv"
    netutil "kv-store/Network"
    protocols "kv-store/Protocols"
    database "kv-store/Database"
    "net/http"
    "encoding/gob"
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
    udp netutil.UDP
    tcp netutil.TCP
    oracle protocols.Orchestrator
    c_engine protocols.CausalEngine
    id string
    ip string
    index int
    peers []string
    db database.DB
    actions map[string]interface{}
    buffer string
}

// initialize the key-value store
func (node *Node) Init() error {

    // exctract the initial view of the system from the os environment
    addr := os.Getenv("ADDRESS")

    if addr == "" {
        err := fmt.Errorf("os environment variables not set")
        return err
    }

    view := strings.Split(os.Getenv("VIEW"), ",")
    repl_factor, _ := strconv.Atoi(os.Getenv("REPL_FACTOR"))
    ip := strings.Split(addr, ":")[0]
    port, _ := strconv.Atoi(strings.Split(addr, ":")[1])
    node.id = addr
    node.peers = view

    // create database
    db := database.NewDB()
    node.db = *db

    // create a udp utility
    udp := new(netutil.UDP)
    udp.Init(ip, port, 13800, 1024)
    node.udp = *udp

    // construct function mapping 
    m := map[string]interface{} {
        "signal": node.udp.Signal,
        "read": "",
        "put": node.RemotePut,
        "get": node.RemoteGet,
    }
    node.actions = m

    oracle := protocols.NewOrchestrator(node.id, node.peers, repl_factor, node.udp)
    node.oracle = *oracle

    c_engine := protocols.NewCausalEngine(0)
    node.c_engine = *c_engine

    return nil
}

// This function listens to clients as a go routine and hands off
// any requests to the request handler.
func (node *Node) ServerDaemon() error {
    
    p := make([]byte, node.udp.Buffer)
    oob := make([]byte, node.udp.Buffer)
    buffer := bytes.NewBuffer(p)

    // listen to all addresses
    addr := net.UDPAddr{
        Port: node.udp.Recv_port,
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
            go node.MessageHandler(*buffer, conn) // determine what to do with this packet
        }
    }

    return nil
}

// Handle internal messages between shards and replicas
func (node *Node) MessageHandler(buffer bytes.Buffer, conn *net.UDPConn) error {
    var msg_decode netutil.Msg

    d := gob.NewDecoder(&buffer)
    
    if err := d.Decode(&msg_decode); err != nil {
      panic(err)
    }

    node.HandleContext(msg_decode.Context) // resolve causal conflicts
 
    fmt.Println("Decoded Struct \n", msg_decode,"\n",)
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
            v.(func(*net.UDPConn, string, string))(conn, msg_decode.SrcAddr, msg_decode.Message)

        case "read":
            // publish a message to the causal consensus engine
            node.c_engine.Publish(msg_decode)
            
        default:
            fmt.Println("case_default")
            fmt.Println(k, v)
        }
    }

   return nil
}

// Compare two vector clocks when a version conflict is realized
func (node *Node) HandleContext(context []int) {
    fmt.Println("test")
}

// This node has a specified key, retreive it and send it back to client node
func (node *Node) RemoteGet(conn *net.UDPConn, src_addr string, key string) {
    got, err := node.db.Get(key)

    if err != nil {
        fmt.Println("This node does not have the key", key)
    }

    msg := string(got)
    action := "read"

    // send value back to source node
    node.udp.Send(src_addr, msg, action, node.oracle.Context)
}

// Insert the key, value pair into our local database
func (node *Node) RemotePut(key string, val string) {
    fmt.Println("putting key->val into my database...")

    // context.ValidWrite(msg_decode.Context, node.oracle.Context)

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

    var req float32  = 0.66
    consensus_req := int(float32(node.oracle.NumReplicas()) * req)
    fmt.Println("we need", consensus_req, "in order to perform consensus")

    // start causel engine
    node.c_engine = *protocols.NewCausalEngine(consensus_req)

    // Request this key from each replica in the correct shard
    is_local := node.oracle.Get(thisKey.Key)
    
    // we are the correct shard, consider our key-val entry
    if is_local {
        got, _ := node.db.Get(thisKey.Key)
        strEntry := string(got[:])
        my_cpy := netutil.Msg{SrcAddr:"", Message:strEntry, Action:"", Context:node.oracle.Context}
        
        node.c_engine.Publish(my_cpy)
    }

    correct_read := node.c_engine.OrderEvents()

    output, err := json.Marshal(correct_read.Message)
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
    var node Node
    nodeStatus := node.Init()
    errorHandler(nodeStatus)

    // run the server daemon in the background
    go node.ServerDaemon()
    
    // use the peer to peer connectivity protocol to ensure all nodes up
    p2p := protocols.NewChainMessager()
    p2p.ChainMsg(node.oracle, node.udp) // wait till all replicas are up

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