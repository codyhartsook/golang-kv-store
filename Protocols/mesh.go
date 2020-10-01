package p2p

import (
	"fmt"
	"sort"
	"hash/fnv"
	protocols "kv-store/Network"
)

/*type Exec struct {
	action_map map
}*/

type Orchestrator struct {
	view []string
	addr string
	v_nodes []int  // map of virtual nodes to physical nodes
	virtual_replicas int
	virtual_translation map[int]string
	ring_edge int
	predicessor string
	successor string
	context []int
}

func NewOrchestrator(addr string, view []string) *Orchestrator {
	//self.ring_edge = 691 if len(view) < 100 else 4127    # parameter for hash mod value
    oracle := new(Orchestrator)
    oracle.addr = addr
    oracle.view = view
    oracle.virtual_replicas = 8
    oracle.context = make([]int, len(view))

    if len(view) < 100 {
    	oracle.ring_edge = 691 // prime number to mod hash
    } else {
    	oracle.ring_edge = 4127
    }

    m := make(map[int]string)
    oracle.virtual_translation = m

    oracle.UpdateView(oracle.view)

    return oracle
}

// Given an input string, could be key or node ip, determine the which physical
// node this string belongs to.
func (proto *Orchestrator) GetMatch(key string) string {
	ring_val := proto.Hash(key)
	
	// get the virtual node number
	v_node := proto.FindNextNode(ring_val)
	// convert to physical shard
	node := proto.virtual_translation[v_node]

	return node
}

// given a ring value, perform a binary search to get the next virtual node
// in the ring.
func (proto *Orchestrator) FindNextNode(ring_val int) int {

	i := sort.Search(len(proto.v_nodes), func(i int) bool { return proto.v_nodes[i] >= ring_val })
    
    if i < len(proto.v_nodes) {
    	return proto.v_nodes[i]
    }
        
    return proto.v_nodes[0]
}

// determine which place this node is in the ring when considering nodes only.
func (proto *Orchestrator) GetNodeRingIndex() int {

	for index, node := range(proto.view) {
		if node == proto.addr {
			return index
		}
	}

	return -1
}

// given a string, hash the string an mod it by the ring edge value
func (proto *Orchestrator) Hash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return (int(h.Sum32()) % proto.ring_edge)
}

// create virtual nodes and determine successors and predicessors
func (proto *Orchestrator) UpdateView(new_view []string) {
	for _, node := range new_view {
		for v_rep := 0; v_rep < proto.virtual_replicas; v_rep ++ {
			v_node := proto.Hash(node + string(v_rep))

			proto.v_nodes = append(proto.v_nodes, v_node)
			proto.virtual_translation[v_node] = node 
		}
	}

	sort.Ints(proto.v_nodes) // keep a sorted view in order to perform binary search

	proto.view = new_view
	sort.Strings(proto.view)
}

// simple protocol to propogate a message from the start node to the last
// node. 
type Chain struct {
	msg string
	action string
	timeout int
	num_packets int
}

func NewChainMessager() *Chain {
	chain := new(Chain)

	return chain
}

// Define a start node by choosing the smallest ring hash, then send a message
// to the next phyical node in the ring.
func (proto *Chain) ChainMsg(oracle Orchestrator, udp protocols.UDP) error {

	if len(oracle.view) == 1 {
		return nil
	}
	
	proto.msg = "Are you up?"
	proto.action = "echo"
	proto.num_packets = 1
	proto.timeout = 60

	// determine which node we are in the ring
	index := oracle.GetNodeRingIndex()

	fmt.Println("my index:", index)

	// is this is node0, send packet to node1
	if index == 0 {
		initAddr := oracle.view[index+1]

		// send message to this node
		udp.Send(initAddr, proto.msg, proto.action, oracle.context)
		fmt.Println("Sending message to first node", initAddr)
	}
	
	// wait to receive packet from node in chain
	// note that this will block until all messages have been received or a timeotu occures
	/*cerr, conn := udp.Recv(proto.num_packets, proto.timeout)

	if cerr != nil {
		fmt.Println("error getting request from peer, check that all nodes are up:", cerr)
		return cerr
	}

	conn.Close()

	// if we are the last node, reset index 
	if index == len(oracle.view) {
		index = -1
	}

	// if we are not the first node, send message to next node in chain
	if index != 0 {
		addr := oracle.view[index+1]
		udp.Send(addr, proto.msg, proto.action, oracle.context)
	}*/

	return nil
}

