package protocols

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	log "kv-store/Logging"
	msg "kv-store/Messages"
	consensusEng "kv-store/SystemServices/Consensus"
	"sort"
	"strconv"
	"strings"
)

const (
	smallMod  = 691
	medeumMod = 4127
	largeMod  = 7451
)

var logger log.AsyncLog

// Orchestrator -> used to partition shards and assign nodes to shards
type Orchestrator struct {
	view               []string
	hostAddr           string
	replFactor         int
	virtualFactor      int
	numShards          int
	ShardGroups        [][]string  // breakdown of replica nodes per shard
	vShards            []int       // map of virtual nodes to physical nodes
	virtualTranslation map[int]int // translation of virtual shards to physical
	ringEdge           int
	consensusEng.ConEngine
}

// NewOrchestrator -> construct a new orchestrator
func (oracle *Orchestrator) NewOrchestrator(addr string, view []string, replFactor int) {
	oracle.hostAddr = addr
	oracle.view = view
	oracle.virtualFactor = 8
	oracle.replFactor = replFactor
	oracle.numShards = int(len(oracle.view) / oracle.replFactor)

	logger = *log.New(nil) // create logger
	go logger.Start()

	// pick a prime number to mod our hash by
	// this defines our ring edge, where values circle back to 0
	viewLen := len(view)
	switch {
	case viewLen < 100:
		oracle.ringEdge = smallMod
	case viewLen < 250:
		oracle.ringEdge = medeumMod
	default:
		oracle.ringEdge = largeMod
	}

	oracle.virtualTranslation = make(map[int]int)

	// create node to shard mapping
	oracle.UpdateView(oracle.view, oracle.replFactor)
}

// AddConsensusEngine -> set the consensus engine field
func (oracle *Orchestrator) AddConsensusEngine(c consensusEng.ConEngine) {
	oracle.ConEngine = c
}

// State -> Return the json representation of the struct
func (oracle *Orchestrator) State() (string, error) {
	out, err := json.Marshal(oracle)
	if err != nil {
		return "", err
	}

	return string(out), nil
}

// DistributeNodes -> put x nodes into y shards with even distribution
// Note this operation clears he previous mapping of shards
func (oracle *Orchestrator) distributeNodes(view []string, replFactor int) [][]string {
	sort.Strings(view)

	numShards := int(len(view) / replFactor)
	replicas := int(len(view) / numShards)
	overflow := (len(view) % numShards)

	ShardGroups := make([][]string, numShards) // clears previous mapping
	nodeIter := 0
	extra := 0
	interval := 0

	// evenly map nodes to shards
	for shard := 0; shard < numShards; shard++ {
		if shard < overflow { // in case division has remainder
			extra = 1
		} else {
			extra = 0
		}

		interval = replicas + extra

		ShardGroups[shard] = oracle.view[nodeIter:(nodeIter + interval)]
		nodeIter += interval
	}

	return ShardGroups
}

// generateVirtualShards -> distribute virutal nodes to each shard
func (oracle *Orchestrator) generateVirtualShards() {

	for shardID := 0; shardID < oracle.numShards; shardID++ {
		for vShard := 0; vShard < oracle.virtualFactor; vShard++ {

			vShardStr := (strconv.Itoa(shardID) + strconv.Itoa(vShard))
			vShardHash := oracle.partitionHash(vShardStr)

			oracle.vShards = append(oracle.vShards, vShardHash)
			oracle.virtualTranslation[vShardHash] = shardID
		}
	}
	sort.Ints(oracle.vShards) // sort virtual shards for binary search ops
}

// UpdateView -> given a view of nodes and a replication factor, distribute nodes
// to shards.
func (oracle *Orchestrator) UpdateView(newView []string, replFactor int) {

	// update how many shards we should maintain
	ShardGroups := oracle.distributeNodes(newView, replFactor)
	oracle.ShardGroups = ShardGroups

	// distribute virutal nodes to each shard
	oracle.generateVirtualShards()

	sort.Ints(oracle.vShards) // sort virtual shards for binary search ops
}

// PartitionHash -> given a string, hash the string an mod it by the ring edge value
func (oracle *Orchestrator) partitionHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return (int(h.Sum32()) % oracle.ringEdge)
}

// NumReplicas -> return how many replicas each shard should have
func (oracle *Orchestrator) NumReplicas() (int, error) {
	if oracle.replFactor == 0 {
		return 0, errors.New("error, divide by 0. oracle, replFactor is set to 0")
	}

	numShards := int(len(oracle.view) / oracle.replFactor)
	replicas := int(len(oracle.view) / numShards)

	return replicas, nil
}

// GetShardID -> determine which place this node is in the ring when
// considering nodes only.
func (oracle *Orchestrator) GetShardID(node string) int {
	var currAddr string
	for shardID, shardGroup := range oracle.ShardGroups {
		for _, currNode := range shardGroup {

			currAddr = strings.Split(currNode, ":")[0]
			if currAddr == node {
				return shardID
			}
		}
	}

	return -1
}

// PeerReplicas -> return all replicas for this shard
func (oracle *Orchestrator) PeerReplicas(node string) ([]string, error) {
	shard := oracle.GetShardID(node)

	if shard >= len(oracle.ShardGroups) {
		return nil, errors.New("shard_id not in range of shard groupds")
	}
	return oracle.ShardGroups[shard], nil
}

// FindNextShard -> given a ring value, perform a binary search to get the next
// virtual node in the ring.
func (oracle *Orchestrator) findNextShard(ringVal int) int {

	i := sort.Search(len(oracle.vShards), func(i int) bool { return oracle.vShards[i] >= ringVal })

	if i < len(oracle.vShards) {
		return oracle.vShards[i]
	}

	return oracle.vShards[0]
}

// GetMatch -> given an input token, determine which shard the token belongs to
func (oracle *Orchestrator) GetMatch(key string) int {
	ringVal := oracle.partitionHash(key)

	// get the virtual node number
	vShard := oracle.findNextShard(ringVal)

	// convert to physical shard
	shard := oracle.virtualTranslation[vShard]

	return shard
}

// KeyOp -> general key operationfunction, find the correct shard then apply given action
func (oracle *Orchestrator) KeyOp(Msg msg.Msg) bool {
	// find which shard this token belongs to
	token := strings.Split(Msg.PayloadToStr(), ":")[0]
	shard := oracle.GetMatch(token)
	local := false

	// send each shard node the key update
	for _, node := range oracle.ShardGroups[shard] {

		// store the key-value pair in our database
		if node == oracle.hostAddr {
			local = true
		} else {
			logger.Write("Sending key op to node " + node + " with ID " + Msg.ID)
			go oracle.Send(node, Msg) // send key-val pair to correct replicas
		}
	}
	// return whether we need to store this key on this node
	return local
}
