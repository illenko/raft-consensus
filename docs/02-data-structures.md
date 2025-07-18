# Raft Data Structures & System Comparisons

## Core Raft Data Structures

### 1. Node States

Raft nodes operate in one of three states:

#### **Follower**
- **Role**: Passive, responds to leader and candidate requests
- **Behavior**: 
  - Receives AppendEntries from leader
  - Votes in elections when requested
  - Converts to candidate if election timeout expires
- **Analogy**: Worker node following instructions

#### **Candidate**
- **Role**: Seeking to become leader
- **Behavior**:
  - Increments term and votes for self
  - Sends RequestVote RPCs to other nodes
  - Becomes leader if majority votes received
  - Returns to follower if higher term discovered
- **Analogy**: Political candidate campaigning for office

#### **Leader**
- **Role**: Handles all client requests and manages log replication
- **Behavior**:
  - Sends AppendEntries RPCs to followers
  - Commits entries when majority replicated
  - Steps down if higher term discovered
- **Analogy**: Manager coordinating team activities

### 2. Terms (Logical Time)

#### **Purpose**
- **Logical Clock**: Prevents old leaders from causing issues
- **Election Epochs**: Each leader election increments term
- **Consistency**: Ensures newer information overrides older

#### **Properties**
- **Monotonic**: Terms always increase
- **Unique Leadership**: At most one leader per term
- **Term Comparison**: Higher term always wins

### 3. Log Entries

#### **Structure**
```
LogEntry {
    Index: Sequential position in log
    Term: Term when entry was created
    Command: State machine command
    Committed: Whether entry is committed
}
```

#### **Properties**
- **Append-Only**: Entries never deleted, only appended
- **Sequential**: Consecutive indices with no gaps
- **Immutable**: Committed entries never change

### 4. RPC Messages

#### **RequestVote RPC**
```
RequestVote {
    Term: Candidate's term
    CandidateId: Candidate requesting vote
    LastLogIndex: Index of candidate's last log entry
    LastLogTerm: Term of candidate's last log entry
}

RequestVoteResponse {
    Term: Current term for candidate to update itself
    VoteGranted: True if candidate received vote
}
```

#### **AppendEntries RPC**
```
AppendEntries {
    Term: Leader's term
    LeaderId: Leader's identifier
    PrevLogIndex: Index of log entry immediately preceding new ones
    PrevLogTerm: Term of prevLogIndex entry
    Entries: Log entries to store (empty for heartbeat)
    LeaderCommit: Leader's commit index
}

AppendEntriesResponse {
    Term: Current term for leader to update itself
    Success: True if follower contained entry matching prevLogIndex and prevLogTerm
    ConflictIndex: Index of first conflicting entry (optimization)
}
```

## System Comparisons

### Log Structure Comparisons

#### **Raft Log vs Kafka Partitions**

| Aspect | Raft Log | Kafka Partition |
|--------|----------|-----------------|
| **Structure** | Single sequential log per cluster | Multiple partitions per topic |
| **Leadership** | Single leader for entire log | Leader per partition |
| **Ordering** | Total order across all entries | Order within partition only |
| **Replication** | Synchronous to majority | Synchronous to in-sync replicas |
| **Compaction** | Snapshot + log truncation | Log compaction + retention |

#### **Raft Log vs RabbitMQ Queues**

| Aspect | Raft Log | RabbitMQ Queue |
|--------|----------|----------------|
| **Persistence** | Always persisted | Optional persistence |
| **Consumption** | State machine replay | Message consumption |
| **Ordering** | Strict sequential | FIFO within queue |
| **Replication** | Raft consensus | Mirror queue synchronization |
| **Durability** | Majority consensus | Configurable durability |

#### **Raft Log vs NATS Streams**

| Aspect | Raft Log | NATS Stream |
|--------|----------|-------------|
| **Consensus** | Raft for all entries | Raft for metadata only |
| **Performance** | Consensus overhead | Optimized for throughput |
| **Consistency** | Strong consistency | Configurable consistency |
| **Retention** | Snapshot-based | Time/size/count based |
| **Replay** | Sequential replay | Subject-based replay |

### State Management Comparisons

#### **Raft States vs Kafka Broker States**

| Aspect | Raft | Kafka |
|--------|------|-------|
| **States** | Follower/Candidate/Leader | Broker states + Controller |
| **Leadership** | Cluster-wide leader | Per-partition leaders |
| **Transitions** | Term-based transitions | ZooKeeper/KRaft based |
| **Consistency** | Strong consistency | Eventually consistent |

#### **Raft States vs RabbitMQ Clustering**

| Aspect | Raft | RabbitMQ |
|--------|------|----------|
| **States** | Follower/Candidate/Leader | Running/Stopped/Partitioned |
| **Leadership** | Elected leader | Master-slave (manual) |
| **Coordination** | Consensus-based | Erlang distribution |
| **Split-Brain** | Prevented by majority | Handled by pause_minority |

### RPC Communication Comparisons

#### **Raft RPCs vs Kafka Protocol**

| Aspect | Raft | Kafka |
|--------|------|-------|
| **Core RPCs** | RequestVote, AppendEntries | Produce, Fetch, Metadata |
| **Wire Format** | Typically JSON/Binary | Custom binary protocol |
| **Reliability** | At-most-once semantics | Configurable delivery semantics |
| **Batching** | Entry batching | Message batching |

#### **Raft RPCs vs NATS Messaging**

| Aspect | Raft | NATS |
|--------|------|------|
| **Communication** | Leader-follower | Publish-subscribe |
| **Reliability** | Consensus-based | At-most-once (core) |
| **Routing** | Direct node-to-node | Subject-based routing |
| **Scalability** | Limited by consensus | High scalability |

#### **Raft RPCs vs RabbitMQ AMQP**

| Aspect | Raft | RabbitMQ |
|--------|------|----------|
| **Protocol** | Custom consensus RPCs | AMQP 0-9-1 |
| **Reliability** | Consensus guarantees | Message acknowledgments |
| **Routing** | Leader-based | Exchange-based routing |
| **Flow Control** | Implicit in consensus | Explicit flow control |

## Implementation Considerations

### Memory Management
- **Raft**: Log compaction via snapshots
- **Kafka**: Log segment rolling and compaction
- **RabbitMQ**: Message TTL and queue limits
- **NATS**: Stream retention policies

### Performance Optimization
- **Raft**: Batch AppendEntries, pipeline RPCs
- **Kafka**: Producer batching, zero-copy transfers
- **RabbitMQ**: Message prefetch, clustering
- **NATS**: Connection pooling, async processing

### Fault Tolerance
- **Raft**: Majority consensus, leader election
- **Kafka**: Replication factor, ISR management
- **RabbitMQ**: Queue mirroring, node clustering
- **NATS**: Cluster mesh, failover

## Design Decisions for Our Implementation

### 1. Simplicity First
- Use clear, readable data structures
- Implement core Raft without optimizations initially
- Add performance improvements later

### 2. Kafka Alignment
- Structure logs similar to Kafka partitions
- Use similar terminology where applicable
- Consider KRaft-style optimizations

### 3. Go Idioms
- Use channels for goroutine communication
- Implement proper context cancellation
- Follow Go concurrency patterns

### 4. Testing Support
- Design for easy unit testing
- Support deterministic execution
- Enable failure injection

This foundation will guide our implementation of the core Raft data structures in Go.