package P2P

import (
	"fmt"
	"sort"
	"hash/fnv"
	"encoding/json"
	consensus_engine "kv-store/Consensus"
)

type Orchestrator struct {
	view []string
	addr string
	repl_factor int
	virtual_factor int
	virtual_shards int
	Shard_groups [][]string             // breakdown of replica nodes per shard
	v_shards []int                      // map of virtual nodes to physical nodes
	virtual_translation map[int]int     // translation of virtual shards to physical
	ring_edge int
	consensus consensus_engine.ConsensusEngine
}

func NewOrchestrator(addr string, view []string, repl_factor int) *Orchestrator {
    oracle := new(Orchestrator)
    oracle.addr = addr
    oracle.view = view
    oracle.virtual_factor = 8
    oracle.repl_factor = repl_factor

    if len(view) < 100 {
    	oracle.ring_edge = 691 // prime number to mod hash
    } else {
    	oracle.ring_edge = 4127
    }

    m := make(map[int]int)
    oracle.virtual_translation = m

    oracle.UpdateView(oracle.view, oracle.repl_factor) // create node to shard mapping

    return oracle
}

// Set the consensus engine field
func (oracle *Orchestrator) InitiateConsensus(c consensus_engine.ConsensusEngine) {
	oracle.consensus = c
}

//
func(oracle *Orchestrator) State() (string, error) {
	out, err := json.Marshal(oracle)
    if err != nil {
        panic (err)
    }

    str_out := string(out)
	return str_out, err
}

func (oracle *Orchestrator) NumReplicas() int {
	num_shards := int(len(oracle.view) / oracle.repl_factor)
	replicas := int(len(oracle.view) / num_shards)

	return replicas
}

func (oracle *Orchestrator) ShardReplicas(node string) []string {
	shard := oracle.GetMatch(node)

	return oracle.Shard_groups[shard]
}

// Put x nodes into y shards with even distribution
func (oracle *Orchestrator) DistributeNodes(view []string, repl_factor int) [][]string { 
	sort.Strings(view)

	num_shards := int(len(view) / repl_factor)
	replicas := int(len(view) / num_shards)
	overflow := (len(view) % num_shards)

	Shard_groups := make([][]string, num_shards)
	node_iter := 0
	extra := 0
	interval := 0

	// evenly map nodes to shards
	for shard := 0; shard < num_shards; shard++ { 
		if shard < overflow { // in case division has remainder
			extra = 1
		} else {
			extra = 0
		}

		interval = replicas + extra

		Shard_groups[shard] = oracle.view[node_iter:(node_iter+interval)]
		node_iter += interval
	}

	return Shard_groups
}

// Given an input string, could be key or node ip, determine the which physical
// node this string belongs to.
func (oracle *Orchestrator) GetMatch(key string) int {
	ring_val := oracle.PartitionHash(key)
	
	// get the virtual node number
	v_shard := oracle.FindNextShard(ring_val)

	// convert to physical shard
	shard := oracle.virtual_translation[v_shard]

	return shard
}

// given a ring value, perform a binary search to get the next virtual node
// in the ring.
func (oracle *Orchestrator) FindNextShard(ring_val int) int {

	i := sort.Search(len(oracle.v_shards), func(i int) bool { return oracle.v_shards[i] >= ring_val })
    
    if i < len(oracle.v_shards) {
    	return oracle.v_shards[i]
    }
        
    return oracle.v_shards[0]
}

// determine which place this node is in the ring when considering nodes only.
func (oracle *Orchestrator) GetNodeRingIndex() int {

	for index, node := range(oracle.view) {
		if node == oracle.addr {
			return index
		}
	}

	return -1
}

// given a string, hash the string an mod it by the ring edge value
func (oracle *Orchestrator) PartitionHash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return (int(h.Sum32()) % oracle.ring_edge)
}

/*
	Map n nodes into s shards
	Map v virtual shards to physical shards
*/
func (oracle *Orchestrator) UpdateView(new_view []string, repl_factor int) {

	Shard_groups := oracle.DistributeNodes(new_view, repl_factor)
	oracle.Shard_groups = Shard_groups

	for shard_id := 0; shard_id < len(oracle.Shard_groups); shard_id++ {
		for v_shard := 0; v_shard < oracle.virtual_factor; v_shard++ {

			v_shard_str := string(shard_id) + string(v_shard)
			v_shard_hash := oracle.PartitionHash(v_shard_str)
			
			oracle.v_shards = append(oracle.v_shards, v_shard_hash)
			oracle.virtual_translation[v_shard_hash] = shard_id
		}
	}

	sort.Ints(oracle.v_shards) // sort virtual shards for binary search
}

// key operation universal function, find the correct shard then apply action
func (oracle *Orchestrator) KeyOp(token string, msg string, action string) bool {
	// find which shard this token belongs to
	shard := oracle.GetMatch(token)
	local := false

	// send each shard node the key update
	for _, node := range oracle.Shard_groups[shard] {
		
		// store the key-value pair in our database
		if node == oracle.addr {
			local = true
		} else { 
			go oracle.consensus.Send(node, msg, action) // send key-val pair to correct replicas
		}
	}

	return local
}

// Database functions that the high level node package queries
func (oracle *Orchestrator) Get(Key string) bool {

	action := "get"
	store_local := oracle.KeyOp(Key, Key, action)

	return store_local
}

//
func (oracle *Orchestrator) Put(Key, Value string) bool {

	token := Key
	msg := Key + ":" + Value
	action := "put"

	store_local := oracle.KeyOp(token, msg, action)

	return store_local
}

//
func (oracle *Orchestrator) Delete(Key string) ([]byte, error) {
	fmt.Println("Deleting key")

	return nil, nil
}
