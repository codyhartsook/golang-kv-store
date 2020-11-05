package clientservices

import (
	"encoding/json"
	"fmt"
	msg "kv-store/Messages"
	node "kv-store/Node"
	"net/http"
	"strings"
)

/*
 * HTTP user endpoints
 */

const (
	keyPath   = "key"
	statePath = "snapshot"
)

// Create a handler type to store the reference to a node
type handler struct {
	nodeRef *node.Node
}

// display root message
func (h *handler) stateHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Node id: {%s}\n", h.nodeRef.ID)
	fmt.Fprintf(w, "Node status: running\n")
	fmt.Fprintf(w, "shards: {%v}\n", h.nodeRef.Oracle.ShardGroups)
	fmt.Fprintf(w, "Database state:\n")
}

// handleGet -> Retrieve value from the holding shard and return causaly
// consistent read
func (h *handler) handleGet(w http.ResponseWriter, r *http.Request) {
	urlPathSegments := strings.Split(r.URL.Path, fmt.Sprintf("%s/", keyPath))
	if len(urlPathSegments[1:]) > 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	Key := urlPathSegments[len(urlPathSegments)-1]

	// start causal event comparison
	eventID := h.nodeRef.ConEngine.NewEventStream()
	thisMsg := msg.Msg{
		SrcAddr: h.nodeRef.IP,
		Payload: Key,
		ID:      eventID,
		Action:  "get",
	}

	// Request this key from each replica in the correct shard
	ourShard := h.nodeRef.Oracle.KeyOp(thisMsg)

	// we are the correct shard, consider our key-val entry
	if ourShard {

		got, _ := h.nodeRef.DB.Get(Key)
		strEntry := string(got[:])
		thisMsg.Payload = strEntry

		myCpy := h.nodeRef.ConEngine.Encode(thisMsg)
		h.nodeRef.ConEngine.Deliver(myCpy)
	}

	result, notFound := h.nodeRef.ConEngine.OrderEvents(eventID) // blocking call
	if notFound != nil {
		panic(notFound)
	}

	output, err := json.Marshal(result.Payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Write(output)
}

// hadlePut ->
func (h *handler) handlePut(w http.ResponseWriter, r *http.Request) {
	// cast the request
	var newEntry msg.Entry
	err := json.NewDecoder(r.Body).Decode(&newEntry)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	thisMsg := msg.Msg{
		SrcAddr: h.nodeRef.IP,
		Payload: (newEntry.Key + ":" + newEntry.Value),
		ID:      "",
		Action:  "put",
	}

	storeLocal := h.nodeRef.Oracle.KeyOp(thisMsg)

	// put key-val in our database
	if storeLocal {
		h.nodeRef.DB.Put(newEntry.Key, newEntry.Value)
	}
}

// Handle request according to request method
func (h *handler) keyHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	switch r.Method {
	case http.MethodPost:
		h.handlePut(w, r)
		return
	case http.MethodDelete:
		w.WriteHeader(http.StatusBadRequest)
		return
	case http.MethodGet:
		h.handleGet(w, r)
		return
	case http.MethodPut:
		h.handlePut(w, r)
		return
	}
}

// SetupRoutes -> register the api endpoints
// This function is given a node reference in order to access its fields
func SetupRoutes(apiBasePath string, node *node.Node) {
	myHandlerType := new(handler)
	myHandlerType.nodeRef = node

	sHandler := http.HandlerFunc(myHandlerType.stateHandler)
	kHandler := http.HandlerFunc(myHandlerType.keyHandler)

	// API State endpoint
	http.Handle(fmt.Sprintf("%s/%s", apiBasePath, statePath), sHandler)

	// API key endpoint
	http.Handle(fmt.Sprintf("%s/%s", apiBasePath, keyPath), kHandler)
	http.Handle(fmt.Sprintf("%s/%s/", apiBasePath, keyPath), kHandler)
}
