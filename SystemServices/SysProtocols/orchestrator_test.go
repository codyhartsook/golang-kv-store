package protocols

import "testing"

func TestNewOrchestrator(t *testing.T) {
	scenarios := []struct {
		addr       string
		view       []string
		replFactor int
	}{
		{addr: "id0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 1},
		{addr: "id0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 2},
		{addr: "id0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 3},
		{addr: "id0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 4},
	}

	for _, s := range scenarios {
		oracle := new(Orchestrator)
		oracle.NewOrchestrator(s.addr, s.view, s.replFactor)

		replicas, _ := oracle.NumReplicas()
		if replicas < s.replFactor {
			t.Errorf("Replica factor does not match for input '%v'. Expected %d, got %d", s.view, s.replFactor, replicas)
		}
	}
}

func TestGetShardID(t *testing.T) {
	scenarios := []struct {
		addr       string
		view       []string
		replFactor int
	}{
		{addr: "node0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 1},
		{addr: "node0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 2},
		{addr: "node0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 3},
		{addr: "node0", view: []string{"node0", "node1", "node2", "node3"}, replFactor: 4},
	}

	for _, s := range scenarios {
		oracle := new(Orchestrator)
		oracle.NewOrchestrator(s.addr, s.view, s.replFactor)

		index := oracle.GetShardID(oracle.hostAddr)
		if index < 0 {
			t.Errorf("Shard Id is not valid upon input '%v', '%v'", s.view, s.replFactor)
		}
	}
}

func TestDistributeNodes(t *testing.T) {

}

func TestGetMatch(t *testing.T) {

}

func TestFindNextShard(t *testing.T) {

}

func TestGetNodeRingIndex(t *testing.T) {

}

func TestUpdateView(t *testing.T) {

}
