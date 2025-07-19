# Raft Log Replication Theory

## Log Structure

### Log Entry Format
```
LogEntry {
    Index: 1, 2, 3, 4, 5, ...     // Sequential position
    Term:  1, 1, 2, 2, 3, ...     // Term when created  
    Command: "set x=3", "incr y"  // State machine command
}
```

### Log Properties
- **Append-only**: Entries are never deleted, only appended
- **Sequential**: No gaps in indices (1, 2, 3, ...)
- **Immutable**: Once committed, entries never change
- **Ordered**: Commands applied to state machine in index order

### Log Example
```
Index:   1    2    3    4    5    6    7
Term:   [1]  [1]  [1]  [2]  [2]  [3]  [3]
Cmd:    [x=1][y=2][z=3][x=4][y=5][x=6][z=7]
        └─ Committed ─┘           │
                               Uncommitted
```

## Replication Process

### Normal Operation Flow

```
Client          Leader          Follower A      Follower B
  │               │                 │               │
  │ Command       │                 │               │
  │ ─────────────▶│                 │               │
  │               │ 1. Append to    │               │
  │               │    local log    │               │
  │               │                 │               │
  │               │ 2. AppendEntries│               │
  │               │ ───────────────▶│               │
  │               │ ───────────────────────────────▶│
  │               │                 │               │
  │               │    3. Success   │               │
  │               │ ◀───────────────│               │
  │               │ ◀───────────────────────────────│
  │               │                 │               │
  │               │ 4. Commit entry │               │
  │               │    when majority│               │
  │               │                 │               │
  │  Response     │ 5. AppendEntries│               │
  │ ◀─────────────│    (commit info)│               │
  │               │ ───────────────▶│               │
  │               │ ───────────────────────────────▶│
```

### Detailed Steps

1. **Client Request**: Client sends command to leader
2. **Local Append**: Leader appends entry to its local log
3. **Replicate**: Leader sends AppendEntries RPC to all followers
4. **Acknowledge**: Followers respond with success/failure
5. **Commit**: Leader commits entry when majority responds
6. **Apply**: Leader applies entry to state machine
7. **Response**: Leader responds to client
8. **Propagate**: Leader notifies followers of commitment in next AppendEntries

## AppendEntries RPC Details

### Request Structure
```go
type AppendEntriesArgs struct {
    Term         uint64      // Leader's current term
    LeaderID     string      // Leader's identifier
    PrevLogIndex uint64      // Index of log entry before new ones
    PrevLogTerm  uint64      // Term of prevLogIndex entry
    Entries      []LogEntry  // Log entries to store (empty for heartbeat)
    LeaderCommit uint64      // Leader's commit index
}
```

### Response Structure
```go
type AppendEntriesReply struct {
    Term          uint64 // Current term, for leader to update itself
    Success       bool   // True if follower contained entry matching prevLogIndex/prevLogTerm
    ConflictIndex uint64 // Index of first conflicting entry (for fast recovery)
}
```

### Consistency Check

The consistency check ensures log matching property:

```
Leader Log:      [1][1][2][2][3]
                    │  │  │  │  │
PrevLogIndex=3 ────┘  │  │  │  │
PrevLogTerm=2 ────────┘  │  │  │
New Entries ─────────────┘  │  │
                            
Follower Log:    [1][1][2][2]
Match at index 3? ────┘  │
Term 2 matches? ─────────┘
✓ Consistency check passes
```

### Consistency Check Algorithm
```go
func (f *Follower) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
    // 1. Reply false if term < currentTerm
    if args.Term < f.currentTerm {
        return AppendEntriesReply{Term: f.currentTerm, Success: false}
    }
    
    // 2. Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
    if args.PrevLogIndex > 0 {
        if f.log.length() < args.PrevLogIndex {
            return AppendEntriesReply{Term: f.currentTerm, Success: false}
        }
        
        entry := f.log.getEntry(args.PrevLogIndex)
        if entry.Term != args.PrevLogTerm {
            return AppendEntriesReply{Term: f.currentTerm, Success: false}
        }
    }
    
    // 3. Delete conflicting entries and append new ones
    f.log.deleteFrom(args.PrevLogIndex + 1)
    f.log.append(args.Entries...)
    
    // 4. Update commit index
    if args.LeaderCommit > f.commitIndex {
        f.commitIndex = min(args.LeaderCommit, f.log.lastIndex())
    }
    
    return AppendEntriesReply{Term: f.currentTerm, Success: true}
}
```

## Log Conflicts and Recovery

### Conflict Scenarios

#### Scenario 1: Missing Entries
```
Leader:   [1][1][2][2][3]
Follower: [1][1]
          
Missing entries 3, 4, 5 - follower needs to catch up
```

#### Scenario 2: Extra Entries  
```
Leader:   [1][1][2]
Follower: [1][1][2][2][3]

Follower has extra uncommitted entries - needs truncation
```

#### Scenario 3: Divergent Entries
```
Leader:   [1][1][2][3][3]
Follower: [1][1][2][2][2]

Logs diverge at index 4 - follower needs repair
```

### Conflict Resolution Process

```
Leader sends AppendEntries with:
PrevLogIndex=4, PrevLogTerm=3, Entries=[3,3,3]

Follower Log: [1][1][2][2][2]
                     │
              Index 4 has term 2, not 3
              
1. Follower rejects (Success=false)
2. Leader decrements nextIndex for this follower
3. Leader retries with earlier PrevLogIndex
4. Process repeats until consistency check passes
5. Follower truncates conflicting entries
6. Follower appends new entries
```

### Optimized Conflict Resolution

Instead of decrementing one index at a time:

```go
type AppendEntriesReply struct {
    Term          uint64
    Success       bool
    ConflictIndex uint64 // First index of conflicting term
    ConflictTerm  uint64 // Term of conflicting entry
}

// Leader optimization:
func (l *Leader) handleAppendEntriesReply(follower NodeID, reply AppendEntriesReply) {
    if !reply.Success {
        if reply.ConflictTerm == 0 {
            // Follower's log is too short
            l.nextIndex[follower] = reply.ConflictIndex
        } else {
            // Find last entry with ConflictTerm in leader's log
            lastIndex := l.findLastEntryWithTerm(reply.ConflictTerm)
            if lastIndex > 0 {
                l.nextIndex[follower] = lastIndex + 1
            } else {
                l.nextIndex[follower] = reply.ConflictIndex
            }
        }
    }
}
```

## Commitment Rules

### When to Commit

A leader commits an entry when:
1. Entry is stored on majority of servers
2. At least one entry from current term is committed

### Commitment Safety

```
Term 2 Leader creates entry at index 4:

Server 1: [1][1][2][ ][ ]  (Leader)
Server 2: [1][1][2][ ][ ]
Server 3: [1][1][ ][ ][ ]
Server 4: [1][1][ ][ ][ ]  
Server 5: [1][1][ ][ ][ ]

Leader replicates to majority:
Server 1: [1][1][2][2][ ]  
Server 2: [1][1][2][2][ ]  ✓ Majority
Server 3: [1][1][2][2][ ]  ✓ reached
Server 4: [1][1][ ][ ][ ]
Server 5: [1][1][ ][ ][ ]

Entry at index 4 can now be committed
```

### Why Current Term Requirement?

Prevents commitment of entries from previous terms that might be overwritten:

```
Scenario: Leader from term 2 commits entry from term 1

Initial state:
S1: [1][1][2]  (died)
S2: [1][1][2]  
S3: [1][1]

S2 becomes leader in term 3:
S2: [1][1][2][3]  (wants to commit index 3)
S3: [1][1][3]     (received different entry)

Without current term requirement, both entries 
at index 3 could be considered committed!
```

## Leader State Management

### Per-Follower State
```go
type LeaderState struct {
    nextIndex  map[NodeID]uint64  // Next entry to send to each follower
    matchIndex map[NodeID]uint64  // Highest known replicated entry per follower
}
```

### nextIndex Management
- **Initialization**: Set to leader's last log index + 1
- **Success**: Increment after successful AppendEntries
- **Failure**: Decrement and retry with earlier entries
- **Purpose**: Track where to start sending entries to each follower

### matchIndex Management  
- **Initialization**: Set to 0
- **Update**: Set to last known replicated index after success
- **Purpose**: Track replication progress for commitment decisions

### Commitment Algorithm
```go
func (l *Leader) updateCommitIndex() {
    // Find highest index replicated on majority
    for index := l.commitIndex + 1; index <= l.log.lastIndex(); index++ {
        count := 1 // Count leader itself
        
        for _, matchIndex := range l.matchIndex {
            if matchIndex >= index {
                count++
            }
        }
        
        // Commit if majority and entry from current term
        if count > len(l.peers)/2 && l.log.getEntry(index).Term == l.currentTerm {
            l.commitIndex = index
        }
    }
}
```

## Performance Optimizations

### Batching
- **Multiple entries**: Send multiple entries in single AppendEntries
- **Reduces RPCs**: Fewer network round trips
- **Better throughput**: Higher overall system throughput

### Pipelining
- **Parallel RPCs**: Send AppendEntries to followers in parallel
- **Don't wait**: Don't wait for responses before sending next batch
- **Flow control**: Limit number of outstanding requests per follower

### Heartbeats
- **Empty AppendEntries**: No log entries, just heartbeat
- **Maintain authority**: Prevent followers from starting elections
- **Commitment propagation**: Update follower commit indices

## Safety Guarantees

The log replication mechanism ensures:

1. **Log Matching**: Identical entries at same index/term across all nodes
2. **Leader Completeness**: Committed entries appear in all future leaders  
3. **State Machine Safety**: All nodes apply same commands in same order

These properties combined with leader election safety create a strongly consistent replicated state machine.

## Next: Implementation Details
The next phase covers implementing these algorithms in Go with proper error handling and optimization.