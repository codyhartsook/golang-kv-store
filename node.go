package main

import (
    "fmt"
    "log"
    netutil "kv-store/Network"
    protocols "kv-store/Protocols"
    "github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/core/rawdb"
    "net/http"
    "strings"
    "encoding/json"
    "os"
)

// create an ethereum key-value database
func createDB() (ethdb.Database, error) {
    var err error
    var db ethdb.Database
    db = rawdb.NewMemoryDatabase()

    return db, err
}

// Define node structure in order to provide access to the database and 
// network fucntions wrapper
type Node struct {
    db ethdb.Database
    udp netutil.UDP
    tcp netutil.TCP
    id string
    index int
    peers []string
}

// initialize the key-value store
func (node *Node) Init() error {
    db, dberr := createDB()

    if dberr != nil {
        fmt.Println("database not created")
        return dberr
    }

    // exctract the initial view of the system from the os environment
    view := strings.Split(os.Getenv("VIEW"), ",")
    addr := os.Getenv("ADDRESS")
    node.peers = view

    // Set this nodes database
    node.db = db
    node.id = addr

    // create a udp utility
    udp := new(netutil.UDP)
    udp.Init("1053", 1024)
    node.udp = *udp

    // use the peer to peer connectivity protocol 
    p2p := new(protocols.Chain)
    numReplicas := len(node.peers)
    node.index = (p2p.Hash(node.id) % numReplicas)

    p2p.ChainMsg(node.udp, node.index)

    return nil
}

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

/* 
 * HTTP user endpoints
 */

// display root message
func (node *Node) stateHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Node id: {%s}\n", node.id)
    fmt.Fprintf(w, "Node status: running\n")
    fmt.Fprintf(w, "Peer nodes:")
    fmt.Fprintf(w, "Database state:\n")

    // Iterate over the database 
    it := node.db.NewIterator([]byte{}, []byte{})
    for it.Next() {
        thisKey := string(it.Key()[:])
        thisVal := string(it.Value()[:])
        fmt.Println(thisKey, thisVal)

        fmt.Fprintf(w, "    %s -> %s\n", thisKey, thisVal)
    }
}

// return current view of nodes  
func (node *Node) viewHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello %s!", r.URL.Path[1:])
    fmt.Println("RESTfulServ. on:8093, Controller:", r.URL.Path[1:])
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

    insertErr := node.db.Put([]byte(newEntry.Key), []byte(newEntry.Value))
    if insertErr != nil {
        fmt.Println("can't Put on open DB:", insertErr)
    }
}

// Get a key from the database
func (node *Node) getHandler(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close()

    var thisKey Key
    err := json.NewDecoder(r.Body).Decode(&thisKey)

    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    // get entry from database and handle any errors
    got, getErr := node.db.Get([]byte(thisKey.Key))
    if getErr != nil {
        fmt.Println("Could not retreive key")
        http.Error(w, getErr.Error(), 400)
        return
    }

    // convert byte array to string
    strEntry := string(got[:])

    output, err := json.Marshal(strEntry)
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    w.Header().Set("content-type", "application/json")
    w.Write(output)
}

/*
 * Start the API
*/
func main() {
    fmt.Println("Starting the distributed key values store...")
    fmt.Println("Using port:13800")

    // create the node instance
    var node Node
    nodeStatus := node.Init()
    errorHandler(nodeStatus)

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
        os.Exit(1)
    }
}