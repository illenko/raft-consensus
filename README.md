# Raft Consensus Algorithm - Complete Reference

A comprehensive implementation of the Raft consensus algorithm in Go with detailed theory, algorithms, and hands-on examples. Perfect for interview preparation and distributed systems learning.

## Quick Start

```bash
# Run all tests
go test ./...

# Try split-brain prevention demo
go run examples/split_brain_prevention/main.go

# Try network partition handling
go run examples/network_partition/main.go
```

## Table of Contents

1. [The Consensus Problem](#the-consensus-problem)
2. [Raft Overview](#raft-overview)
3. [Core Data Structures](#core-data-structures)
4. [Leader Election Algorithm](#leader-election-algorithm)
5. [Log Replication Algorithm](#log-replication-algorithm)
6. [Safety Properties](#safety-properties)
7. [Failure Scenarios](#failure-scenarios)
8. [Implementation Details](#implementation-details)
9. [Hands-On Examples](#hands-on-examples)
10. [Interview Questions](#common-interview-questions)

## The Consensus Problem

### Definition
**Consensus**: Getting multiple distributed nodes to agree on a single value despite failures.

### Requirements
1. **Agreement**: All non-faulty nodes decide on the same value
2. **Validity**: The decided value must be proposed by some node  
3. **Termination**: All non-faulty nodes eventually decide

### Challenges
- **Node failures**: Crash-stop, slow nodes, restarts
- **Network failures**: Message loss, delays, partitions
- **Split-brain**: Multiple leaders causing conflicting decisions
- **Timing**: No synchronized clocks, timeout selection

### CAP Theorem Context
- **Consistency**: All nodes see same data simultaneously
- **Availability**: System remains operational  
- **Partition Tolerance**: Continues despite network splits

**Raft Choice**: CP system (Consistency + Partition tolerance over Availability)

## Raft Overview

### Design Principles
1. **Strong Leadership**: Only leaders make decisions
2. **Decomposition**: Split into leader election + log replication
3. **Understandability**: Simpler than Paxos
4. **State Reduction**: Fewer states than alternatives

### Key Properties
- **Strong Consistency**: Linearizability guarantees
- **Majority Rule**: Need >50% nodes for decisions
- **Term-based Leadership**: Logical time prevents conflicts
- **Log-centric**: All changes go through replicated log

## Core Data Structures

### Node States
```
┌─────────────┐    timeout/    ┌─────────────┐    majority    ┌─────────────┐
│             │   start election│             │     votes      │             │
│  Follower   ├────────────────▶│ Candidate   ├───────────────▶│   Leader    │
│             │                 │             │                │             │
└─────┬───────┘                 └─────┬───────┘                └─────┬───────┘
      │                               │                              │
      │          higher term received │                              │
      │                               │         higher term          │
      │                               │         discovered           │
      └───────────────────────────────┼──────────────────────────────┘
                                      │
                                      ▼
                                 timeout/split vote
```

### States Explained
- **Follower**: Passive, only responds to leaders/candidates
- **Candidate**: Actively seeking leadership votes
- **Leader**: Handles all client requests and log replication

### Terms (Logical Clock)
```
Term 1    Term 2    Term 3    Term 4    Term 5
┌─────┐   ┌─────┐   ┌─────┐   ┌─────┐   ┌─────┐
│  L  │   │  L  │   │ No  │   │  L  │   │  L  │
│     │   │     │   │Lead │   │     │   │     │
└─────┘   └─────┘   └─────┘   └─────┘   └─────┘
Success   Success   Failed    Success   Success
```

**Term Properties**:
- Monotonically increasing
- At most one leader per term
- Higher terms always override lower terms
- Election increments term

### Log Structure
```
Index:  1    2    3    4    5    6    7
Term:  [1]  [1]  [2]  [2]  [2]  [3]  [3]
Cmd:   [x=1][y=2][z=3][x=4][y=5][x=6][z=7]
       └──── Committed ────┘      │
                               Uncommitted
```

**Log Properties**:
- **Append-only**: Never delete entries
- **Sequential**: No gaps in indices
- **Immutable**: Committed entries never change
- **Ordered**: Applied to state machine in order

## Leader Election Algorithm

### Election Trigger Conditions
1. Follower election timeout expires (no heartbeat from leader)
2. Candidate election timeout expires (no majority votes received)
3. Node startup (joins cluster as follower)

### Detailed Election Process

```go
// Simplified election algorithm
func (n *Node) startElection() {
    n.state = Candidate
    n.currentTerm++
    n.votedFor = n.nodeID
    
    votes := 1 // Vote for self
    
    for each peer {
        go func(peer) {
            reply := peer.RequestVote(RequestVoteArgs{
                Term:         n.currentTerm,
                CandidateID:  n.nodeID,
                LastLogIndex: n.log.lastIndex(),
                LastLogTerm:  n.log.lastTerm(),
            })
            
            if reply.VoteGranted {
                votes++
                if votes > len(cluster)/2 {
                    n.becomeLeader()
                }
            }
        }(peer)
    }
}
```

### RequestVote RPC
```go
type RequestVoteArgs struct {
    Term         uint64   // Candidate's term
    CandidateID  string   // Candidate requesting vote
    LastLogIndex uint64   // Index of candidate's last log entry
    LastLogTerm  uint64   // Term of candidate's last log entry
}

type RequestVoteReply struct {
    Term        uint64   // Current term (for candidate to update)
    VoteGranted bool     // True means candidate received vote
}
```

### Voting Rules
```go
func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
    // 1. Reply false if term < currentTerm
    if args.Term < n.currentTerm {
        return RequestVoteReply{n.currentTerm, false}
    }
    
    // 2. If RPC request contains term T > currentTerm, 
    //    set currentTerm = T, convert to follower
    if args.Term > n.currentTerm {
        n.currentTerm = args.Term
        n.state = Follower
        n.votedFor = ""
    }
    
    // 3. Grant vote if:
    //    - Haven't voted OR voted for this candidate
    //    - Candidate's log is at least as up-to-date
    canVote := (n.votedFor == "" || n.votedFor == args.CandidateID)
    logUpToDate := isLogUpToDate(args.LastLogTerm, args.LastLogIndex, 
                                 n.log.lastTerm(), n.log.lastIndex())
    
    if canVote && logUpToDate {
        n.votedFor = args.CandidateID
        return RequestVoteReply{n.currentTerm, true}
    }
    
    return RequestVoteReply{n.currentTerm, false}
}

func isLogUpToDate(candidateTerm, candidateIndex, voterTerm, voterIndex uint64) bool {
    if candidateTerm > voterTerm {
        return true
    }
    if candidateTerm == voterTerm {
        return candidateIndex >= voterIndex
    }
    return false
}
```

### Split-Brain Prevention Mechanisms

1. **Majority Requirement**: Need >50% votes
```
5-node cluster: Need 3 votes minimum
- Impossible for two candidates to both get 3 votes
- At most one leader per term
```

2. **Randomized Timeouts**: Prevent simultaneous elections
```
Node A: 150-300ms timeout
Node B: 150-300ms timeout  
Node C: 150-300ms timeout
→ High probability one times out first
```

3. **Term Comparison**: Higher terms win
```
Old leader (term 5) vs New leader (term 6)
→ Old leader immediately steps down
```

## Log Replication Algorithm

### AppendEntries RPC
```go
type AppendEntriesArgs struct {
    Term         uint64      // Leader's term
    LeaderID     string      // Leader's ID
    PrevLogIndex uint64      // Index of log entry before new ones
    PrevLogTerm  uint64      // Term of prevLogIndex entry
    Entries      []LogEntry  // Log entries to store (empty = heartbeat)
    LeaderCommit uint64      // Leader's commitIndex
}

type AppendEntriesReply struct {
    Term          uint64  // Current term (leader updates itself)
    Success       bool    // True if follower had matching prev entry
    ConflictIndex uint64  // First index of conflicting term
}
```

### Replication Process

```go
func (leader *Node) replicateEntry(command interface{}) {
    // 1. Append entry to leader's log
    newIndex := leader.log.lastIndex() + 1
    entry := LogEntry{
        Index:   newIndex,
        Term:    leader.currentTerm,
        Command: command,
    }
    leader.log.append(entry)
    
    // 2. Send AppendEntries to all followers
    for each follower {
        go leader.sendAppendEntries(follower)
    }
    
    // 3. Commit when majority acknowledges
    if replicationCount >= majority {
        leader.commitIndex = newIndex
        leader.applyToStateMachine(entry)
        // Notify followers of commitment in next AppendEntries
    }
}
```

### Consistency Check Algorithm
```go
func (follower *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
    // 1. Reply false if term < currentTerm
    if args.Term < follower.currentTerm {
        return AppendEntriesReply{follower.currentTerm, false, 0}
    }
    
    // 2. Convert to follower if higher term
    if args.Term > follower.currentTerm {
        follower.currentTerm = args.Term
        follower.state = Follower
    }
    
    // 3. Reset election timeout (heard from leader)
    follower.resetElectionTimeout()
    
    // 4. Reply false if log doesn't contain entry at prevLogIndex 
    //    whose term matches prevLogTerm
    if args.PrevLogIndex > 0 {
        if args.PrevLogIndex > follower.log.lastIndex() {
            return AppendEntriesReply{follower.currentTerm, false, follower.log.lastIndex() + 1}
        }
        
        if follower.log.termAt(args.PrevLogIndex) != args.PrevLogTerm {
            // Find first index of conflicting term
            conflictIndex := args.PrevLogIndex
            conflictTerm := follower.log.termAt(args.PrevLogIndex)
            for i := args.PrevLogIndex; i > 0; i-- {
                if follower.log.termAt(i) != conflictTerm {
                    break
                }
                conflictIndex = i
            }
            return AppendEntriesReply{follower.currentTerm, false, conflictIndex}
        }
    }
    
    // 5. Delete conflicting entries and append new ones
    for i, entry := range args.Entries {
        index := args.PrevLogIndex + 1 + uint64(i)
        if index <= follower.log.lastIndex() {
            if follower.log.termAt(index) != entry.Term {
                follower.log.truncateFrom(index)
            }
        }
        follower.log.append(entry)
    }
    
    // 6. Update commit index
    if args.LeaderCommit > follower.commitIndex {
        follower.commitIndex = min(args.LeaderCommit, follower.log.lastIndex())
        follower.applyCommittedEntries()
    }
    
    return AppendEntriesReply{follower.currentTerm, true, 0}
}
```

### Commitment Rules

**When Leader Commits Entry**:
1. Entry is stored on majority of servers
2. At least one entry from current term is committed (safety requirement)

```go
func (leader *Node) updateCommitIndex() {
    for index := leader.commitIndex + 1; index <= leader.log.lastIndex(); index++ {
        if leader.log.termAt(index) != leader.currentTerm {
            continue // Only commit entries from current term
        }
        
        replicationCount := 1 // Count leader
        for _, follower := range leader.followers {
            if follower.matchIndex >= index {
                replicationCount++
            }
        }
        
        if replicationCount > len(cluster)/2 {
            leader.commitIndex = index
        }
    }
}
```

## Safety Properties

### 1. Election Safety
**Property**: At most one leader can be elected in a given term

**Mechanism**: Majority voting ensures only one candidate can get >50% votes

### 2. Leader Append-Only  
**Property**: Leaders never overwrite or delete entries in their logs

**Mechanism**: Leaders only append new entries, never modify existing ones

### 3. Log Matching
**Property**: If two logs contain an entry with same index and term, then:
- The entries are identical
- All preceding entries are identical

**Mechanism**: AppendEntries consistency check

### 4. Leader Completeness
**Property**: If a log entry is committed in a given term, then that entry will be present in logs of all leaders for all higher-numbered terms

**Mechanism**: Election restriction - only candidates with up-to-date logs can win

### 5. State Machine Safety
**Property**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index

**Mechanism**: Combination of all above properties

## Failure Scenarios

### 1. Leader Crash During Replication
```
Scenario:
1. Leader receives command
2. Replicates to 1 out of 3 followers  
3. Crashes before committing
4. New leader elected

Possible Outcomes:
- If new leader has the entry → entry gets committed
- If new leader lacks the entry → entry is discarded
- Never partially committed (safety preserved)
```

### 2. Network Partition
```
Initial: [A-B-C-D-E] Leader=A

Partition: [A-B-C] | [D-E]
          majority | minority

Result:
- Left partition: A remains leader, continues operating
- Right partition: Cannot elect leader, becomes unavailable
- No conflicting decisions possible
```

### 3. Message Reordering
```
Problem: AppendEntries messages arrive out of order
Solution: PrevLogIndex/PrevLogTerm consistency check
- Followers reject entries that don't match expected sequence
- Leader retries with correct sequence
```

### 4. Duplicate Messages
```
Problem: Network delivers same AppendEntries multiple times
Solution: Idempotent operations
- Followers check if entry already exists at index
- Applying same entry multiple times has no effect
```

### 5. Slow Followers
```
Problem: Some followers are slow to respond
Solution: Majority consensus
- Don't wait for all followers
- Commit when majority acknowledges
- Slow followers catch up eventually
```

## Implementation Details

### Core Components
- **`internal/raft/node.go`** - Node lifecycle, RPC handling, state management
- **`internal/raft/election.go`** - Complete leader election algorithm
- **`internal/raft/replication.go`** - Log replication and commitment protocol
- **`internal/raft/types.go`** - All core data structures and RPC messages
- **`internal/raft/network.go`** - Realistic network simulation with failures

### Key Data Structures
```go
type Node struct {
    // Persistent state (survives crashes)
    currentTerm uint64
    votedFor    string
    log         *Log
    
    // Volatile state (all nodes)
    commitIndex uint64
    lastApplied uint64
    
    // Volatile state (leaders only)
    nextIndex  map[string]uint64  // Next entry to send to each follower
    matchIndex map[string]uint64  // Highest known replicated entry per follower
}

type LogEntry struct {
    Index   uint64
    Term    uint64
    Command interface{}
}
```

### Network Simulation Features
- **Message drops**: Simulate network packet loss
- **Message delays**: Add realistic latency
- **Network partitions**: Split cluster into groups
- **Node crashes**: Simulate complete node failures
- **Concurrent operations**: Multiple clients simultaneously

## Hands-On Examples

### 1. Split-Brain Prevention
```bash
go run examples/split_brain_prevention/main.go
```
**Demonstrates**: 5-node cluster, network partitions, majority voting
**Key Insight**: Only majority partition can elect leaders

### 2. Network Partition Handling  
```bash
go run examples/network_partition/main.go
```
**Demonstrates**: Majority continues, minority becomes unavailable
**Key Insight**: Availability sacrificed for consistency

### 3. Leader Failure Recovery
```bash
go run examples/leader_failure/main.go
```
**Demonstrates**: Automatic elections, seamless transitions
**Key Insight**: ~150-300ms recovery time

### 4. Log Consistency
```bash
go run examples/log_consistency/main.go
```
**Demonstrates**: Identical replication, consistency checks
**Key Insight**: All committed entries identical across nodes

### 5. Concurrent Client Requests
```bash
go run examples/concurrent_requests/main.go
```
**Demonstrates**: Request serialization, order preservation
**Key Insight**: Leader provides global ordering

### 6. Node Crash and Recovery
```bash
go run examples/node_recovery/main.go
```
**Demonstrates**: Crash handling, automatic catch-up
**Key Insight**: Seamless recovery with log replay

## Common Interview Questions

### Q: How does Raft prevent split-brain?
**A**: Majority voting - need >50% of nodes to make decisions. Impossible for two groups to both have majority.

### Q: What happens during network partition?
**A**: Only majority partition remains available. Minority partition cannot elect leaders or commit entries, preventing conflicting decisions.

### Q: How does leader election work?
**A**: Followers timeout → become candidates → request votes → need majority to win → become leader. Randomized timeouts prevent simultaneous elections.

### Q: What is the election restriction?
**A**: Only candidates with up-to-date logs can become leaders. Ensures all committed entries are preserved.

### Q: How does log replication ensure consistency?
**A**: AppendEntries consistency check ensures followers only accept entries that match expected sequence. Leaders retry until successful.

### Q: What are Raft's safety properties?
**A**: Election safety, leader append-only, log matching, leader completeness, state machine safety.

### Q: How does Raft handle message loss?
**A**: Leaders retry failed AppendEntries. Majority consensus means some message loss is tolerable.

### Q: What's the difference between Raft and Paxos?
**A**: Raft has strong leadership and is easier to understand. Paxos is more general but complex.

### Q: How does Raft ensure linearizability?
**A**: All operations go through single leader who provides global ordering. Clients see operations as if executed instantly.

### Q: What happens if leader commits entry then crashes?
**A**: If majority replicated it, new leader will have it. If not, entry is discarded. Never partially committed.

## Project Structure

```
raft-consensus/
├── README.md                          # This comprehensive guide
├── internal/raft/                     # Core implementation
│   ├── node.go                        # Node lifecycle and RPC handling
│   ├── election.go                    # Leader election algorithm
│   ├── replication.go                 # Log replication protocol
│   ├── types.go                       # Core data structures
│   └── network.go                     # Network simulation
└── examples/                          # Educational demonstrations
    ├── split_brain_prevention/        # Majority voting demo
    ├── network_partition/             # Partition tolerance demo
    ├── leader_failure/                # Fault tolerance demo
    ├── log_consistency/               # Replication demo
    ├── concurrent_requests/           # Concurrency demo
    └── node_recovery/                 # Recovery demo
```

## Development Commands

```bash
# Run tests
go test ./...

# Build project  
go build ./...

# Format code
go fmt ./...

# Run linting
golangci-lint run
```

This implementation provides a complete, production-quality Raft system perfect for understanding distributed consensus algorithms and preparing for technical interviews.