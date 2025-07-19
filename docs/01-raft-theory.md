# Raft Consensus Algorithm Theory

## The Consensus Problem

### What is Consensus?
Consensus is the fundamental problem of getting multiple distributed nodes to agree on a single value, even when some nodes may fail. It's essential for:
- Maintaining consistent state across replicas
- Coordinating distributed transactions
- Implementing fault-tolerant services
- Managing cluster membership and configuration

### Problem Requirements
A consensus algorithm must satisfy:
1. **Agreement**: All non-faulty nodes decide on the same value
2. **Validity**: The decided value must be proposed by some node
3. **Termination**: All non-faulty nodes eventually decide

### Challenges in Distributed Systems

#### Node Failures
- **Crash Failures**: Nodes stop responding permanently or temporarily
- **Slow Nodes**: Nodes respond very slowly, appearing failed
- **Restart Failures**: Nodes crash and restart, potentially losing in-memory state
- **Partial Failures**: Some components of a node fail while others continue working

#### Network Failures
- **Message Loss**: Networks drop packets, RPC calls fail
- **Message Delays**: High latency causes timeouts and false failure detection
- **Message Duplication**: Networks can deliver messages multiple times
- **Message Reordering**: Messages arrive out of order due to different network paths

#### Network Partitions
Network partitions are particularly dangerous for distributed systems:

```
Original Cluster:          After Partition:
┌─────────────────┐        ┌─────────┐    ┌─────────┐
│  A ←→ B ←→ C    │   →    │ A ←→ B  │    │    C    │
│                 │        │         │    │         │
│ All connected   │        │ Group 1 │    │ Group 2 │
└─────────────────┘        └─────────┘    └─────────┘
```

**Split-Brain Problem**: Each partition might elect its own leader, causing:
- **Data Divergence**: Different partitions accept different updates
- **Inconsistent State**: Impossible to merge conflicting changes later
- **Availability vs Consistency Trade-off**: Must choose between staying available or maintaining consistency

#### Timing and Coordination Problems
- **Clock Skew**: Different nodes have different times
- **Timeout Selection**: Too short causes false failures, too long delays recovery
- **Ordering Events**: Without synchronized clocks, hard to order concurrent events
- **Liveness vs Safety**: Aggressive timeouts improve liveness but hurt safety

## Raft Overview

### Design Principles
Raft was designed with **understandability** as the primary goal:
- **Decomposition**: Split consensus into leader election and log replication
- **State Space Reduction**: Simplified state space compared to Paxos
- **Strong Leadership**: Only leaders make decisions, reducing complexity

### Key Properties
- **Strong Consistency**: Provides linearizability guarantees
- **Availability**: Available as long as majority of nodes are reachable
- **Partition Tolerance**: Continues operating during network partitions
- **Simplicity**: Easier to understand and implement than alternatives

### How Raft Solves Distributed Systems Problems

#### Split-Brain Prevention
Raft prevents split-brain through **majority consensus**:

```
5-Node Cluster Partition:
┌─────────────┐    ┌─────────────┐
│  A    B    C│    │  D    E     │
│             │    │             │
│ 3 nodes     │    │ 2 nodes     │
│ (majority)  │    │ (minority)  │
│             │    │             │
│ ✓ Can elect │    │ ✗ Cannot    │
│   leader    │    │   elect     │
└─────────────┘    └─────────────┘
```

**Majority Rule**: 
- Need >50% of nodes to elect leader (3 out of 5)
- Minority partition cannot form quorum
- Only one partition can have active leader
- Prevents conflicting decisions

#### Failure Detection and Recovery
```
Normal Operation:    Leader Failure:      New Election:
     Leader               ???               New Leader
       │                   │                   │
   Heartbeat           No response         Fresh leader
       ▼                   ▼                   ▼
   Followers           Election          Resume operation
                       timeout
```

**Raft's Approach**:
- **Heartbeats**: Leader sends regular empty AppendEntries
- **Timeouts**: Followers start election if no heartbeat received
- **Randomization**: Random election timeouts prevent simultaneous elections
- **Term Numbers**: Higher terms override lower terms, preventing old leaders

#### Network Partition Handling

**Scenario 1: Majority Partition**
```
Original: [A-B-C-D-E] Leader=A

Partition: [A-B-C] | [D-E]
          majority | minority

Result:
- Left partition (A-B-C): Continues operating, A remains leader
- Right partition (D-E): Cannot elect leader, becomes unavailable
- No split-brain: Only A can make decisions
```

**Scenario 2: Partition Heals**
```
Before Heal: [A-B-C] | [D-E]
            Leader=A | No leader

After Heal: [A-B-C-D-E]
           All recognize A as leader
           D,E catch up on missed updates
```

#### Consistency Guarantees

**Problem**: Concurrent updates to same data
```
Client 1: SET x=5    Client 2: SET x=10
    │                     │
    ▼                     ▼
Node A                Node B
```

**Raft Solution**: All updates go through leader
```
Client 1 ──┐
           ▼
         Leader ──── Followers
           ▲
Client 2 ──┘

Result: Serialized updates, consistent ordering
```

## Raft State Machine

### Node States
```
┌─────────────┐    timeout/    ┌─────────────┐    receive votes    ┌─────────────┐
│             │   start election│             │   from majority     │             │
│  Follower   ├────────────────▶│ Candidate   ├────────────────────▶│   Leader    │
│             │                 │             │                     │             │
└─────┬───────┘                 └─────┬───────┘                     └─────┬───────┘
      │                               │                                   │
      │          receive AppendEntries │                                   │
      │          with higher term      │                                   │
      │                               │          discover server           │
      │                               │          with higher term          │
      │                               │                                   │
      └───────────────────────────────┼───────────────────────────────────┘
                                      │
                                      ▼
                                 timeout/
                                split vote
```

#### Follower State
- **Passive role**: Only responds to requests from leaders and candidates
- **No client interactions**: Redirects clients to current leader
- **Election participation**: Votes in leader elections when requested
- **Heartbeat monitoring**: Becomes candidate if election timeout expires without hearing from leader

#### Candidate State
- **Active election**: Seeks to become the new leader
- **Vote collection**: Requests votes from all other nodes in cluster
- **Majority requirement**: Needs majority of votes to become leader
- **Term increment**: Increments current term when starting election
- **Self-vote**: Always votes for itself in its own election

#### Leader State
- **Client service**: Handles all client requests for the cluster
- **Log replication**: Sends AppendEntries to followers to replicate log
- **Heartbeats**: Sends empty AppendEntries as heartbeats to maintain authority
- **Commit decisions**: Decides when log entries are safe to commit

### Terms (Logical Time)

```
Term 1    Term 2    Term 3    Term 4    Term 5
┌─────┐   ┌─────┐   ┌─────┐   ┌─────┐   ┌─────┐
│  L  │   │  L  │   │     │   │  L  │   │  L  │
│     │   │     │   │ No  │   │     │   │     │
│     │   │     │   │Lead.│   │     │   │     │
└─────┘   └─────┘   └─────┘   └─────┘   └─────┘
   │         │         │         │         │
   ▼         ▼         ▼         ▼         ▼
Election  Election  Failed    Election  Election
Success   Success   Election  Success   Success
```

#### Purpose of Terms
- **Logical timestamps**: Replace wall-clock time in distributed environment
- **Leadership epochs**: Each term has at most one leader
- **Stale detection**: Higher terms always override lower terms
- **Election coordination**: Prevents conflicts during leader elections

#### Term Properties
- **Monotonic**: Terms always increase over time
- **Election triggers**: New election increments term
- **Authority establishment**: Leader authority tied to specific term
- **Conflict resolution**: Higher term always wins in conflicts

## Leader Election Algorithm

### Election Trigger
Election starts when:
1. Follower doesn't receive heartbeat within election timeout
2. Candidate doesn't receive majority votes and times out
3. Node starts up and joins cluster

### Election Process

```
Follower           Candidate              Other Nodes
    │                   │                      │
    │ Election timeout  │                      │
    │ ─────────────────▶│                      │
    │                   │ Increment term       │
    │                   │ Vote for self        │
    │                   │ Reset election timer │
    │                   │                      │
    │                   │ RequestVote RPC      │
    │                   │ ────────────────────▶│
    │                   │                      │ Check term
    │                   │                      │ Check log
    │                   │                      │ Check vote status
    │                   │                      │
    │                   │      Vote Response   │
    │                   │ ◀────────────────────│
    │                   │                      │
    │                   │ Collect votes        │
    │                   │                      │
    │   Majority votes? │                      │
    │        ┌─────────▶│                      │
    │        │ YES      │ Become Leader        │
    │        │          │ Send heartbeats      │
    │        │          │ ────────────────────▶│
    │        │          │                      │
    │        │ NO       │                      │
    │        └─────────▶│ Return to Follower   │
    │                   │ (if higher term seen)│
```

### Election Safety Properties

#### Election Restriction
A candidate can only become leader if its log is at least as up-to-date as any other node's log:

```go
// Log is more up-to-date if:
// 1. Last log term is higher, OR
// 2. Last log terms are equal AND last log index is higher or equal

func isLogUpToDate(candidateLastTerm, candidateLastIndex, voterLastTerm, voterLastIndex) bool {
    if candidateLastTerm > voterLastTerm {
        return true
    }
    if candidateLastTerm == voterLastTerm {
        return candidateLastIndex >= voterLastIndex
    }
    return false
}
```

#### Voting Rules
1. **One vote per term**: Each node votes for at most one candidate per term
2. **Persistence**: Vote choice is persistent across crashes
3. **Log requirement**: Only vote for candidates with up-to-date logs
4. **Term comparison**: Always update to higher terms

#### Split-Brain Prevention in Elections

**Problem Scenario**: Multiple simultaneous elections
```
Term 5 Election:
Node A: Votes for A
Node B: Votes for B  ← Potential split brain
Node C: Votes for C
```

**Raft's Solutions**:
1. **Majority Required**: Need >50% votes to win
```
5 Nodes: Need 3 votes minimum
- A gets votes from A,D: 2 votes (insufficient)
- B gets votes from B,E: 2 votes (insufficient)  
- C gets votes from C: 1 vote (insufficient)
→ No leader elected, retry with new term
```

2. **Randomized Timeouts**: Prevent simultaneous elections
```
Node A timeout: 150ms
Node B timeout: 200ms  ← A starts first
Node C timeout: 250ms
```

3. **Higher Terms Win**: Prevent old leaders from causing conflicts
```
Old Leader (Term 5) tries to send heartbeat
New Leader (Term 6) already elected
→ Old leader steps down immediately
```

### Election Timing

#### Randomized Timeouts
```
Node A: ├─────────────────┤ (150-300ms)
Node B:   ├─────────────────────┤ (150-300ms)  
Node C:     ├─────────────────┤ (150-300ms)
            │
            ▼
        A times out first
        and starts election
```

#### Why Randomization?
- **Split vote prevention**: Reduces chance of simultaneous elections
- **Leader convergence**: Ensures eventual leader election
- **Performance**: Faster convergence to stable leadership

## Safety Properties and Problem Prevention

### Leader Completeness
**Property**: If a log entry is committed in a given term, then that entry will be present in the logs of all leaders for all higher-numbered terms.

**Problem Solved**: Prevents data loss during leader changes
```
Scenario: Leader commits entry, then crashes
Bad outcome: New leader doesn't have the committed entry
Raft solution: Election restriction ensures new leader has all committed entries
```

### Log Matching Property
**Property**: If two logs contain an entry with the same index and term, then:
1. The entries are identical
2. All preceding entries in both logs are identical

**Problem Solved**: Prevents log inconsistencies
```
Problem: Followers have different entries at same index
Solution: AppendEntries consistency check rejects conflicting entries
```

### State Machine Safety
**Property**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

**Problem Solved**: Prevents divergent state machines
```
Problem: Different nodes apply different commands at same index
Result: Nodes have inconsistent state
Solution: All nodes apply same sequence of committed entries
```

### Common Failure Scenarios Raft Handles

#### Scenario 1: Leader Crashes During Replication
```
1. Leader receives command: SET x=5
2. Leader replicates to 1 follower
3. Leader crashes before committing
4. New leader elected
5. Command may or may not be committed depending on new leader's log
```

**Raft's Guarantee**: Either all nodes commit or none do

#### Scenario 2: Network Partition During Operation
```
Initial: [A-B-C-D-E] Leader=A
Partition: [A-B] | [C-D-E]

A's partition: Cannot commit (no majority)
C's partition: Elects new leader, continues operating
Result: No conflicting updates, consistency maintained
```

#### Scenario 3: Duplicate Leadership Claims
```
Problem: Old leader doesn't realize it's been replaced
Solution: Term numbers - higher term always wins
```

#### Scenario 4: Message Reordering
```
Problem: AppendEntries messages arrive out of order
Solution: Sequence numbers and consistency checks reject invalid entries
```

## Common Anti-Patterns Raft Prevents

### The Split-Brain Anti-Pattern
```
❌ Bad: Multiple active leaders
Node A thinks it's leader
Node B also thinks it's leader
Both accept client requests
→ Conflicting updates, data corruption

✅ Raft: Majority consensus prevents this
Only one leader possible per term
Minority partitions cannot elect leaders
```

### The Lost Update Anti-Pattern
```
❌ Bad: Updates disappear during failures
Client sends: SET x=10
Leader crashes before replicating
New leader doesn't have the update
→ Client thinks update succeeded but it's lost

✅ Raft: Explicit commit protocol
Updates not acknowledged until majority replication
Client knows if update succeeded or failed
```

### The Inconsistent State Anti-Pattern
```
❌ Bad: Nodes have different data
Node A: x=5, y=10
Node B: x=3, y=15
→ Which is correct? Cannot reconcile

✅ Raft: Single leader serializes all updates
All nodes apply same sequence of operations
Guaranteed consistent state across cluster
```

## Next: Log Replication
The next phase covers how Raft replicates log entries across the cluster while maintaining these safety properties and preventing the problems outlined above.