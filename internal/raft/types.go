package raft

import (
	"math/rand"
	"sync"
	"time"
)

// NodeState represents the three possible states of a Raft node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Term represents a logical time in Raft
type Term uint64

// NodeID uniquely identifies a node in the cluster
type NodeID string

// LogIndex represents the position of an entry in the log
type LogIndex uint64

// LogEntry represents a single entry in the replicated log
type LogEntry struct {
	Index   LogIndex    `json:"index"`
	Term    Term        `json:"term"`
	Command interface{} `json:"command"`
}

// Log represents the replicated log structure
type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
}

// NewLog creates a new empty log
func NewLog() *Log {
	return &Log{
		entries: make([]LogEntry, 0),
	}
}

// Append adds entries to the log
func (l *Log) Append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// Get returns the entry at the given index
func (l *Log) Get(index LogIndex) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 || index > LogIndex(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index-1], true
}

// LastIndex returns the index of the last entry
func (l *Log) LastIndex() LogIndex {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return LogIndex(len(l.entries))
}

// LastTerm returns the term of the last entry
func (l *Log) LastTerm() Term {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// TruncateFrom removes entries from the given index onwards
func (l *Log) TruncateFrom(index LogIndex) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index == 0 || index > LogIndex(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index-1]
}

// Entries returns a slice of entries starting from the given index
func (l *Log) Entries(from LogIndex) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if from == 0 || from > LogIndex(len(l.entries)) {
		return nil
	}

	result := make([]LogEntry, len(l.entries)-(int(from)-1))
	copy(result, l.entries[from-1:])
	return result
}

// RequestVoteArgs represents arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         Term     `json:"term"`
	CandidateID  NodeID   `json:"candidate_id"`
	LastLogIndex LogIndex `json:"last_log_index"`
	LastLogTerm  Term     `json:"last_log_term"`
}

// RequestVoteReply represents response for RequestVote RPC
type RequestVoteReply struct {
	Term        Term `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs represents arguments for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         Term       `json:"term"`
	LeaderID     NodeID     `json:"leader_id"`
	PrevLogIndex LogIndex   `json:"prev_log_index"`
	PrevLogTerm  Term       `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit LogIndex   `json:"leader_commit"`
}

// AppendEntriesReply represents response for AppendEntries RPC
type AppendEntriesReply struct {
	Term          Term     `json:"term"`
	Success       bool     `json:"success"`
	ConflictIndex LogIndex `json:"conflict_index,omitempty"`
}

// PersistentState represents state that must survive crashes
type PersistentState struct {
	mu          sync.RWMutex
	CurrentTerm Term   `json:"current_term"`
	VotedFor    NodeID `json:"voted_for"`
	Log         *Log   `json:"log"`
}

// NewPersistentState creates a new persistent state
func NewPersistentState() *PersistentState {
	return &PersistentState{
		CurrentTerm: 0,
		VotedFor:    "",
		Log:         NewLog(),
	}
}

// GetCurrentTerm returns the current term
func (ps *PersistentState) GetCurrentTerm() Term {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.CurrentTerm
}

// SetCurrentTerm sets the current term
func (ps *PersistentState) SetCurrentTerm(term Term) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.CurrentTerm = term
}

// GetVotedFor returns the node voted for in current term
func (ps *PersistentState) GetVotedFor() NodeID {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.VotedFor
}

// SetVotedFor sets the node voted for in current term
func (ps *PersistentState) SetVotedFor(nodeID NodeID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.VotedFor = nodeID
}

// VolatileState represents state that doesn't need to survive crashes
type VolatileState struct {
	mu          sync.RWMutex
	State       NodeState `json:"state"`
	CommitIndex LogIndex  `json:"commit_index"`
	LastApplied LogIndex  `json:"last_applied"`

	// Leader state (valid only when state == Leader)
	NextIndex  map[NodeID]LogIndex `json:"next_index,omitempty"`
	MatchIndex map[NodeID]LogIndex `json:"match_index,omitempty"`

	// Timing
	LastHeartbeat   time.Time     `json:"last_heartbeat"`
	ElectionTimeout time.Duration `json:"election_timeout"`
}

// NewVolatileState creates a new volatile state
func NewVolatileState() *VolatileState {
	return &VolatileState{
		State:           Follower,
		CommitIndex:     0,
		LastApplied:     0,
		NextIndex:       make(map[NodeID]LogIndex),
		MatchIndex:      make(map[NodeID]LogIndex),
		LastHeartbeat:   time.Now(),
		ElectionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
	}
}

// GetState returns the current node state
func (vs *VolatileState) GetState() NodeState {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.State
}

// SetState sets the node state
func (vs *VolatileState) SetState(state NodeState) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.State = state
}

// GetCommitIndex returns the commit index
func (vs *VolatileState) GetCommitIndex() LogIndex {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.CommitIndex
}

// SetCommitIndex sets the commit index
func (vs *VolatileState) SetCommitIndex(index LogIndex) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.CommitIndex = index
}

// UpdateLastHeartbeat updates the last heartbeat timestamp
func (vs *VolatileState) UpdateLastHeartbeat() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.LastHeartbeat = time.Now()
}

// IsElectionTimeout checks if election timeout has elapsed
func (vs *VolatileState) IsElectionTimeout() bool {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return time.Since(vs.LastHeartbeat) > vs.ElectionTimeout
}

// Config represents the cluster configuration
type Config struct {
	NodeID    NodeID            `json:"node_id"`
	Peers     []NodeID          `json:"peers"`
	Addresses map[NodeID]string `json:"addresses"`
}

// ClusterSize returns the total number of nodes in the cluster
func (c *Config) ClusterSize() int {
	return len(c.Peers) + 1 // +1 for self
}

// MajoritySize returns the number of nodes needed for majority
func (c *Config) MajoritySize() int {
	return c.ClusterSize()/2 + 1
}
