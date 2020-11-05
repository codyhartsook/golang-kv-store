package main

import (
	"fmt"
	clientServices "kv-store/ClientServices"
	node "kv-store/Node"
	"net/http"
	"os"
	"strconv"
)

const basePath = "/kv-store"

const (
	critical = 0
	debug    = 1
	log      = 2
)

/*
 * Start the client side API
 */
func main() {
	fmt.Println("Starting the distributed key values store...")

	// create a node instance
	node, nodeStatus := node.NewNode()
	errorHandler(nodeStatus, critical)

	// Start up the backend services
	node.RunBackendSystem()

	// register client http endpoints
	clientServices.SetupRoutes(basePath, node)

	// start listening to client requests
	listenAddr := (":" + strconv.Itoa(node.Port))
	fmt.Println("Using port:", node.Port)
	err := http.ListenAndServe(listenAddr, nil)
	errorHandler(err, debug)

}

// Simple error handler
func errorHandler(err error, level int) {
	if err != nil {

		switch level {

		case critical:
			fmt.Println(err)
			os.Exit(1)
		case debug:
			fmt.Println(err)
		}
	}
}
