package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// ElectionState manages the leader election process
type ElectionState struct {
	mu               sync.Mutex
	votes            map[NodeID]bool // Track votes received in current election
	voteCount        int             // Number of votes received
	electionDeadline time.Time       // When current election times out
}

// NewElectionState creates a new election state
func NewElectionState() *ElectionState {
	return &ElectionState{
		votes:     make(map[NodeID]bool),
		voteCount: 0,
	}
}

// startElection initiates a new leader election
func (n *Node) startElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Transition to candidate state
	n.volatileState.SetState(Candidate)

	// Increment current term
	newTerm := n.persistentState.GetCurrentTerm() + 1
	n.persistentState.SetCurrentTerm(newTerm)

	// Vote for self
	n.persistentState.SetVotedFor(n.config.NodeID)

	// Reset election state
	n.electionState = NewElectionState()
	n.electionState.voteCount = 1 // Vote for self
	n.electionState.votes[n.config.NodeID] = true

	// Set election deadline
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	n.electionState.electionDeadline = time.Now().Add(timeout)

	log.Printf("Node %s: Started election for term %d", n.config.NodeID, newTerm)

	// Request votes from all peers
	go n.requestVotes()
}

// requestVotes sends RequestVote RPCs to all peers
func (n *Node) requestVotes() {
	n.mu.RLock()
	term := n.persistentState.GetCurrentTerm()
	candidateID := n.config.NodeID
	lastLogIndex := n.persistentState.Log.LastIndex()
	lastLogTerm := n.persistentState.Log.LastTerm()
	peers := make([]NodeID, len(n.config.Peers))
	copy(peers, n.config.Peers)
	n.mu.RUnlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Send RequestVote to all peers in parallel
	var wg sync.WaitGroup
	for _, peerID := range peers {
		wg.Add(1)
		go func(peer NodeID) {
			defer wg.Done()
			n.sendRequestVote(peer, args)
		}(peerID)
	}

	wg.Wait()
}

// sendRequestVote sends a RequestVote RPC to a specific peer
func (n *Node) sendRequestVote(peerID NodeID, args RequestVoteArgs) {
	// Use the simulated network for communication
	network := GetSimulatedNetwork()

	go func() {
		reply, err := network.SendRequestVote(n.config.NodeID, peerID, args)
		if err != nil {
			// Handle network errors (partition, message drop, etc.)
			log.Printf("Node %s: Failed to send RequestVote to %s: %v",
				n.config.NodeID, peerID, err)
			return
		}

		// Process the reply
		n.handleVoteReply(peerID, args, reply)
	}()
}

// handleVoteReply processes a RequestVote reply
func (n *Node) handleVoteReply(peerID NodeID, args RequestVoteArgs, reply RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Ignore stale replies
	if reply.Term < n.persistentState.GetCurrentTerm() {
		return
	}

	// If reply term is higher, step down
	if reply.Term > n.persistentState.GetCurrentTerm() {
		n.stepDown(reply.Term)
		return
	}

	// Only process if still candidate and from current term election
	if n.volatileState.GetState() != Candidate || args.Term != n.persistentState.GetCurrentTerm() {
		return
	}

	// Record the vote
	if reply.VoteGranted {
		if !n.electionState.votes[peerID] {
			n.electionState.votes[peerID] = true
			n.electionState.voteCount++

			log.Printf("Node %s: Received vote from %s (total: %d)",
				n.config.NodeID, peerID, n.electionState.voteCount)
		}

		// Check if we have majority
		majoritySize := n.config.MajoritySize()
		if n.electionState.voteCount >= majoritySize {
			n.becomeLeader()
		}
	}
}

// becomeLeader transitions node to leader state
func (n *Node) becomeLeader() {
	if n.volatileState.GetState() != Candidate {
		return
	}

	n.volatileState.SetState(Leader)

	// Initialize leader state
	lastLogIndex := n.persistentState.Log.LastIndex()
	for _, peerID := range n.config.Peers {
		n.volatileState.NextIndex[peerID] = lastLogIndex + 1
		n.volatileState.MatchIndex[peerID] = 0
	}

	log.Printf("Node %s: Became leader for term %d",
		n.config.NodeID, n.persistentState.GetCurrentTerm())

	// Start sending heartbeats immediately
	go n.sendHeartbeats()
}

// stepDown transitions node back to follower state
func (n *Node) stepDown(newTerm Term) {
	n.volatileState.SetState(Follower)
	n.persistentState.SetCurrentTerm(newTerm)
	n.persistentState.SetVotedFor("")
	n.volatileState.UpdateLastHeartbeat()

	log.Printf("Node %s: Stepped down to follower for term %d",
		n.config.NodeID, newTerm)
}

// handleRequestVote processes incoming RequestVote RPC
func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.persistentState.GetCurrentTerm()

	// Reply false if term < currentTerm
	if args.Term < currentTerm {
		return RequestVoteReply{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
	if args.Term > currentTerm {
		n.stepDown(args.Term)
		currentTerm = args.Term
	}

	votedFor := n.persistentState.GetVotedFor()

	// Check if we can grant vote
	canVote := (votedFor == "" || votedFor == args.CandidateID)

	// Check if candidate's log is at least as up-to-date as ours
	logUpToDate := n.isLogUpToDate(args.LastLogTerm, args.LastLogIndex)

	voteGranted := canVote && logUpToDate

	if voteGranted {
		n.persistentState.SetVotedFor(args.CandidateID)
		n.volatileState.UpdateLastHeartbeat() // Reset election timer

		log.Printf("Node %s: Granted vote to %s for term %d",
			n.config.NodeID, args.CandidateID, args.Term)
	} else {
		log.Printf("Node %s: Denied vote to %s for term %d (canVote=%v, logUpToDate=%v)",
			n.config.NodeID, args.CandidateID, args.Term, canVote, logUpToDate)
	}

	return RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as ours
func (n *Node) isLogUpToDate(candidateLastTerm Term, candidateLastIndex LogIndex) bool {
	ourLastTerm := n.persistentState.Log.LastTerm()
	ourLastIndex := n.persistentState.Log.LastIndex()

	// Candidate's log is more up-to-date if:
	// 1. Last log term is higher, OR
	// 2. Last log terms are equal AND last log index is higher or equal

	if candidateLastTerm > ourLastTerm {
		return true
	}

	if candidateLastTerm == ourLastTerm {
		return candidateLastIndex >= ourLastIndex
	}

	return false
}

// checkElectionTimeout checks if election timeout has occurred
func (n *Node) checkElectionTimeout() {
	n.mu.RLock()
	state := n.volatileState.GetState()
	shouldStartElection := false

	switch state {
	case Follower:
		// Start election if haven't heard from leader
		shouldStartElection = n.volatileState.IsElectionTimeout()
	case Candidate:
		// Start new election if current one timed out
		shouldStartElection = time.Now().After(n.electionState.electionDeadline)
	case Leader:
		// Leaders don't start elections
		shouldStartElection = false
	}
	n.mu.RUnlock()

	if shouldStartElection {
		n.startElection()
	}
}

// sendHeartbeats sends periodic heartbeats to maintain leadership
func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(50 * time.Millisecond) // Send heartbeats every 50ms
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.RLock()
			if n.volatileState.GetState() != Leader {
				n.mu.RUnlock()
				return // No longer leader, stop sending heartbeats
			}
			n.mu.RUnlock()

			n.sendAppendEntriesToAll()
		}
	}
}

// sendAppendEntriesToAll sends AppendEntries (heartbeats) to all followers
func (n *Node) sendAppendEntriesToAll() {
	n.mu.RLock()
	peers := make([]NodeID, len(n.config.Peers))
	copy(peers, n.config.Peers)
	n.mu.RUnlock()

	for _, peerID := range peers {
		go n.sendAppendEntries(peerID)
	}
}

// sendAppendEntries sends AppendEntries RPC to a specific follower
func (n *Node) sendAppendEntries(followerID NodeID) {
	n.mu.RLock()
	term := n.persistentState.GetCurrentTerm()
	leaderID := n.config.NodeID
	leaderCommit := n.volatileState.GetCommitIndex()

	// For heartbeats, send empty entries
	// In full implementation, this would include actual log entries
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: 0, // Simplified for heartbeats
		PrevLogTerm:  0,
		Entries:      []LogEntry{}, // Empty for heartbeat
		LeaderCommit: leaderCommit,
	}
	n.mu.RUnlock()

	// Use the simulated network for communication
	network := GetSimulatedNetwork()

	go func() {
		reply, err := network.SendAppendEntries(n.config.NodeID, followerID, args)
		if err != nil {
			// Handle network errors
			log.Printf("Node %s: Failed to send AppendEntries to %s: %v",
				n.config.NodeID, followerID, err)
			return
		}

		n.handleAppendEntriesReply(followerID, args, reply)
	}()
}

// handleAppendEntriesReply processes AppendEntries reply
func (n *Node) handleAppendEntriesReply(followerID NodeID, args AppendEntriesArgs, reply AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If reply term is higher, step down
	if reply.Term > n.persistentState.GetCurrentTerm() {
		n.stepDown(reply.Term)
		return
	}

	// Process successful heartbeat
	if reply.Success {
		// Update follower state (simplified for heartbeats)
		log.Printf("Node %s: Successful heartbeat to %s", n.config.NodeID, followerID)
	}
}
