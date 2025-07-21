package raft_consensus

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// SimulatedNetwork provides a way to simulate network communication between nodes
type SimulatedNetwork struct {
	mu         sync.RWMutex
	nodes      map[NodeID]*Node
	partitions map[NodeID]map[NodeID]bool // tracks which nodes can communicate
	latency    time.Duration
	dropRate   float32 // probability of dropping messages
}

// NewSimulatedNetwork creates a new simulated network
func NewSimulatedNetwork() *SimulatedNetwork {
	return &SimulatedNetwork{
		nodes:      make(map[NodeID]*Node),
		partitions: make(map[NodeID]map[NodeID]bool),
		latency:    time.Duration(10+rand.Intn(20)) * time.Millisecond,
		dropRate:   0.1, // 10% message drop rate
	}
}

// RegisterNode adds a node to the network
func (n *SimulatedNetwork) RegisterNode(node *Node) {
	n.mu.Lock()
	defer n.mu.Unlock()

	nodeID := node.config.NodeID
	n.nodes[nodeID] = node

	// Initialize partition map - by default all nodes can communicate
	n.partitions[nodeID] = make(map[NodeID]bool)
	for otherID := range n.nodes {
		n.partitions[nodeID][otherID] = true
		if n.partitions[otherID] != nil {
			n.partitions[otherID][nodeID] = true
		}
	}
}

// SendRequestVote sends a RequestVote RPC through the simulated network
func (n *SimulatedNetwork) SendRequestVote(from, to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	// Check if nodes can communicate
	n.mu.RLock()
	canCommunicate := n.partitions[from][to]
	fromNode := n.nodes[from]
	toNode := n.nodes[to]
	n.mu.RUnlock()

	if !canCommunicate || fromNode == nil || toNode == nil {
		return RequestVoteReply{Term: 0, VoteGranted: false}, ErrNetworkPartition
	}

	// Simulate message drop
	if rand.Float32() < n.dropRate {
		return RequestVoteReply{Term: 0, VoteGranted: false}, ErrMessageDropped
	}

	// Simulate network latency
	time.Sleep(n.latency)

	// Process the RPC
	reply := toNode.handleRequestVote(args)

	log.Printf("Network: RequestVote %s -> %s: Term=%d, Vote=%v",
		from, to, reply.Term, reply.VoteGranted)

	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC through the simulated network
func (n *SimulatedNetwork) SendAppendEntries(from, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	// Check if nodes can communicate
	n.mu.RLock()
	canCommunicate := n.partitions[from][to]
	fromNode := n.nodes[from]
	toNode := n.nodes[to]
	n.mu.RUnlock()

	if !canCommunicate || fromNode == nil || toNode == nil {
		return AppendEntriesReply{Term: 0, Success: false}, ErrNetworkPartition
	}

	// Simulate message drop
	if rand.Float32() < n.dropRate {
		return AppendEntriesReply{Term: 0, Success: false}, ErrMessageDropped
	}

	// Simulate network latency
	time.Sleep(n.latency)

	// Process the RPC
	reply := toNode.handleAppendEntries(args)

	if len(args.Entries) == 0 {
		log.Printf("Network: Heartbeat %s -> %s: Success=%v", from, to, reply.Success)
	} else {
		log.Printf("Network: AppendEntries %s -> %s: Success=%v", from, to, reply.Success)
	}

	return reply, nil
}

// CreatePartition simulates a network partition between two groups of nodes
func (n *SimulatedNetwork) CreatePartition(group1, group2 []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Disable communication between the two groups
	for _, id1 := range group1 {
		for _, id2 := range group2 {
			if n.partitions[id1] != nil {
				n.partitions[id1][id2] = false
			}
			if n.partitions[id2] != nil {
				n.partitions[id2][id1] = false
			}
		}
	}

	log.Printf("Network: Created partition between %v and %v", group1, group2)
}

// HealPartition restores communication between all nodes
func (n *SimulatedNetwork) HealPartition() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Enable communication between all nodes
	for id1 := range n.nodes {
		for id2 := range n.nodes {
			if n.partitions[id1] != nil {
				n.partitions[id1][id2] = true
			}
		}
	}

	log.Printf("Network: Healed all partitions")
}

// SetLatency changes the network latency
func (n *SimulatedNetwork) SetLatency(latency time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latency = latency
}

// SetDropRate changes the message drop rate
func (n *SimulatedNetwork) SetDropRate(rate float32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = rate
}

// Network errors
var (
	ErrNetworkPartition = &RaftError{Code: "NETWORK_PARTITION", Message: "Network partition prevents communication"}
	ErrMessageDropped   = &RaftError{Code: "MESSAGE_DROPPED", Message: "Message was dropped by network"}
)

// Global network instance for simulation
var globalNetwork *SimulatedNetwork

func init() {
	globalNetwork = NewSimulatedNetwork()
}

// GetSimulatedNetwork returns the global simulated network
func GetSimulatedNetwork() *SimulatedNetwork {
	return globalNetwork
}
