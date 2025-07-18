# Raft Consensus Implementation Plan

## Project Overview
This project implements a simplified Raft consensus algorithm in Go to better understand how consensus works in distributed systems like Kafka.

## Development Commands
```bash
# Run tests
go test ./...

# Build the project
go build ./...

# Run linting (if golangci-lint is available)
golangci-lint run

# Format code
go fmt ./...
```

## Implementation Plan with Theory & Comparisons

### Phase 1: Foundation & Comparative Theory

#### 1. Raft vs Other Consensus Algorithms
**Theory:**
- **Consensus Problem**: Achieving agreement among distributed nodes in presence of failures
- **CAP Theorem**: Consistency, Availability, Partition tolerance - choose 2
- **Raft vs Paxos**: Raft designed for understandability, Paxos for theoretical elegance
- **Byzantine vs Non-Byzantine**: Raft assumes non-Byzantine failures (crash-stop model)

**Algorithm Comparisons:**
- **Raft**: Leader-based, strong consistency, simple to understand
- **Paxos**: Leaderless, complex, theoretical foundation
- **PBFT**: Handles Byzantine failures, requires 3f+1 nodes for f failures
- **Gossip Protocols**: Eventual consistency, highly available

**Real-world Usage:**
- **Kafka**: Uses Raft for metadata management (KRaft mode)
- **etcd**: Raft for distributed key-value store
- **Consul**: Raft for service discovery consensus
- **RabbitMQ**: Uses different clustering (not Raft)
- **NATS**: Uses different approaches for clustering

#### 2. Data Structures Comparison
**Raft Core Structures:**
- **Node States**: Follower, Candidate, Leader
- **Log Entries**: Command, term, index
- **Terms**: Logical clock for leader election

**Comparisons:**
- **Kafka**: Topics → Partitions → Segments (vs Raft's single log)
- **RabbitMQ**: Queues → Messages (vs Raft's log entries)
- **NATS**: Subjects → Messages (vs Raft's structured log)

#### 3. Leader Election Comparison
**Raft Election:**
- Randomized timeouts prevent split votes
- Majority voting ensures single leader
- Term-based leadership prevents old leaders

**Comparisons:**
- **Kafka Controller**: ZooKeeper-based election → KRaft election
- **RabbitMQ**: Master-slave with manual failover
- **NATS**: No single leader, distributed routing

### Phase 2: Replication & Consistency Models

#### 4. Log Replication Comparison
**Raft Log Replication:**
- Leader receives entries, replicates to followers
- Commit only after majority acknowledgment
- Log matching property ensures consistency

**Comparisons:**
- **Kafka Partitions**: Similar append-only log, but distributed leadership
- **RabbitMQ Mirroring**: Synchronous replication to mirror queues
- **NATS JetStream**: Stream replication with configurable consistency

#### 5. Consistency Models
**Raft**: Strong consistency (linearizability)
**Comparisons:**
- **Eventual Consistency**: Cassandra, DynamoDB, DNS
- **Causal Consistency**: Some NoSQL databases
- **Session Consistency**: Many web applications

### Phase 3: Communication & Fault Tolerance

#### 6. RPC Communication Comparison
**Raft RPCs:**
- RequestVote: For leader election
- AppendEntries: For log replication and heartbeats

**Comparisons:**
- **NATS**: Publish-subscribe messaging
- **Kafka Protocol**: Binary protocol with various request types
- **RabbitMQ AMQP**: Advanced message queuing protocol

#### 7. Failure Detection Comparison
**Raft**: Election timeouts and heartbeats
**Comparisons:**
- **RabbitMQ**: Network partition handling with pause_minority
- **NATS**: Cluster autodiscovery and failure detection
- **Kafka**: Broker failure detection via ZooKeeper/KRaft

#### 8. Persistence Comparison
**Raft**: Persistent state (currentTerm, votedFor, log)
**Comparisons:**
- **Kafka**: Log segments with configurable retention
- **RabbitMQ**: Optional message persistence to disk
- **NATS JetStream**: Configurable storage backends

### Phase 4: Client Integration & Testing

#### 9. Client Interface Comparison
**Raft Client**: Submit commands to leader
**Comparisons:**
- **Kafka**: Producers/consumers with partition awareness
- **NATS**: Publishers/subscribers with subject routing
- **RabbitMQ**: Publishers/consumers with queue-based routing

#### 10. Testing & Failure Scenarios
**Scenarios to Test:**
- Network partitions (split-brain prevention)
- Leader crashes during log replication
- Follower crashes and recovery
- Concurrent leader elections
- Log divergence and repair

## Project Structure
```
raft-consensus/
├── cmd/                 # CLI applications
├── internal/
│   ├── raft/           # Core Raft implementation
│   ├── rpc/            # RPC communication layer
│   └── storage/        # Persistence layer
├── pkg/                # Public APIs
├── examples/           # Usage examples
└── tests/              # Integration tests
```

## Progress Status

### ✅ Completed
1. **Consensus Theory Documentation** (`docs/01-consensus-theory.md`)
   - Comprehensive comparison of Raft vs Paxos vs PBFT
   - Real-world usage in Kafka, etcd, Consul, RabbitMQ, NATS
   - CAP theorem context and trade-offs

2. **Data Structures Theory & Implementation** (`docs/02-data-structures.md`, `internal/raft/types.go`)
   - Core Raft data structures with system comparisons
   - Go implementation of Node states, Terms, Log entries, RPC messages
   - Comparison with Kafka partitions, RabbitMQ queues, NATS streams

### 🔄 In Progress
3. **Leader Election Theory** - Next step ready to begin

### 📋 Pending
4. Log replication theory and implementation
5. Safety properties and consistency models
6. RPC communication layer
7. Failure detection mechanisms
8. Persistence layer
9. Client interface
10. Testing suite and examples

### 📁 Files Created
- `docs/01-consensus-theory.md` - Complete consensus algorithm theory
- `docs/02-data-structures.md` - Data structure theory and comparisons
- `internal/raft/types.go` - Core Raft data structures in Go
- `CLAUDE.md` - Project documentation and progress tracking

### 🎯 Next Session Goals
- Complete leader election theory documentation
- Implement leader election mechanism
- Compare with Kafka Controller and RabbitMQ approaches

## Key Learning Objectives
1. Understand consensus algorithms in distributed systems
2. Compare different approaches to distributed coordination
3. Learn how Kafka uses Raft for metadata management
4. Understand trade-offs between consistency, availability, and partition tolerance
5. Implement a production-ready consensus system