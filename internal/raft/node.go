package raft

import (
	"context"
	"log"
	"sync"
	"time"
)

// Node represents a Raft node in the cluster
type Node struct {
	mu sync.RWMutex

	// Core Raft state
	persistentState *PersistentState
	volatileState   *VolatileState
	config          *Config

	// Election management
	electionState *ElectionState

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewNode creates a new Raft node
func NewNode(nodeID NodeID, peers []NodeID, addresses map[NodeID]string) *Node {
	config := &Config{
		NodeID:    nodeID,
		Peers:     peers,
		Addresses: addresses,
	}

	ctx, cancel := context.WithCancel(context.Background())

	node := &Node{
		persistentState: NewPersistentState(),
		volatileState:   NewVolatileState(),
		config:          config,
		electionState:   NewElectionState(),
		ctx:             ctx,
		cancel:          cancel,
		done:            make(chan struct{}),
	}

	// Register with the simulated network
	GetSimulatedNetwork().RegisterNode(node)

	return node
}

// Start begins the Raft node operation
func (n *Node) Start() error {
	log.Printf("Node %s: Starting Raft node", n.config.NodeID)

	// Start main event loop
	go n.run()

	return nil
}

// Stop gracefully shuts down the Raft node
func (n *Node) Stop() error {
	log.Printf("Node %s: Stopping Raft node", n.config.NodeID)

	n.cancel()
	<-n.done

	return nil
}

// run is the main event loop for the Raft node
func (n *Node) run() {
	defer close(n.done)

	// Election timeout ticker
	electionTicker := time.NewTicker(10 * time.Millisecond)
	defer electionTicker.Stop()

	log.Printf("Node %s: Started main loop", n.config.NodeID)

	for {
		select {
		case <-n.ctx.Done():
			log.Printf("Node %s: Shutting down", n.config.NodeID)
			return

		case <-electionTicker.C:
			n.checkElectionTimeout()
		}
	}
}

// GetState returns the current state of the node
func (n *Node) GetState() (NodeState, Term, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	state := n.volatileState.GetState()
	term := n.persistentState.GetCurrentTerm()
	isLeader := (state == Leader)

	return state, term, isLeader
}

// GetLeader returns the current leader ID (if known)
func (n *Node) GetLeader() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// In a full implementation, we'd track the current leader
	// For now, return self if we're the leader
	if n.volatileState.GetState() == Leader {
		return n.config.NodeID
	}

	return ""
}

// IsLeader returns true if this node is currently the leader
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.volatileState.GetState() == Leader
}

// GetTerm returns the current term
func (n *Node) GetTerm() Term {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.persistentState.GetCurrentTerm()
}

// GetLogInfo returns information about the log
func (n *Node) GetLogInfo() (LogIndex, Term, LogIndex) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	lastIndex := n.persistentState.Log.LastIndex()
	lastTerm := n.persistentState.Log.LastTerm()
	commitIndex := n.volatileState.GetCommitIndex()

	return lastIndex, lastTerm, commitIndex
}

// GetClusterInfo returns information about the cluster
func (n *Node) GetClusterInfo() (int, int) {
	return n.config.ClusterSize(), n.config.MajoritySize()
}

// handleAppendEntries processes incoming AppendEntries RPC
func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.persistentState.GetCurrentTerm()

	// Reply false if term < currentTerm
	if args.Term < currentTerm {
		return AppendEntriesReply{
			Term:    currentTerm,
			Success: false,
		}
	}

	// If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
	if args.Term > currentTerm {
		n.stepDown(args.Term)
		currentTerm = args.Term
	}

	// Convert to follower if not already (could be candidate)
	if n.volatileState.GetState() != Follower {
		n.volatileState.SetState(Follower)
	}

	// Reset election timeout - we heard from the leader
	n.volatileState.UpdateLastHeartbeat()

	// For heartbeats (empty entries), just return success
	if len(args.Entries) == 0 {
		log.Printf("Node %s: Received heartbeat from leader %s (term %d)",
			n.config.NodeID, args.LeaderID, args.Term)

		return AppendEntriesReply{
			Term:    currentTerm,
			Success: true,
		}
	}

	// TODO: Implement full log replication logic
	// For now, just acknowledge heartbeats

	return AppendEntriesReply{
		Term:    currentTerm,
		Success: true,
	}
}

// SubmitCommand submits a command to the Raft cluster (leader only)
func (n *Node) SubmitCommand(command interface{}) error {
	n.mu.RLock()
	if n.volatileState.GetState() != Leader {
		n.mu.RUnlock()
		return ErrNotLeader
	}
	n.mu.RUnlock()

	// TODO: Implement command submission
	// This would:
	// 1. Append command to leader's log
	// 2. Replicate to followers
	// 3. Commit when majority acknowledges
	// 4. Apply to state machine
	// 5. Return result to client

	log.Printf("Node %s: Command submitted: %v", n.config.NodeID, command)
	return nil
}

// Custom errors
var (
	ErrNotLeader = &RaftError{Code: "NOT_LEADER", Message: "Node is not the leader"}
)

// RaftError represents a Raft-specific error
type RaftError struct {
	Code    string
	Message string
}

func (e *RaftError) Error() string {
	return e.Message
}
