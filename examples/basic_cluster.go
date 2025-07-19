package main

import (
	"fmt"
	"log"
	"time"

	"raft-consensus/internal/raft"
)

func main() {
	// Create a 3-node cluster
	nodes := createThreeNodeCluster()

	// Start all nodes
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}

	// Let the cluster run and observe leader election
	fmt.Println("=== Starting Raft Cluster Demo ===")
	fmt.Println("Observing leader election process...")

	// Monitor cluster state for 10 seconds
	for i := 0; i < 20; i++ {
		time.Sleep(500 * time.Millisecond)
		printClusterState(nodes, i+1)
	}

	// Stop all nodes
	fmt.Println("\n=== Shutting Down Cluster ===")
	for i, node := range nodes {
		if err := node.Stop(); err != nil {
			log.Printf("Error stopping node %d: %v", i, err)
		}
	}

	fmt.Println("Demo completed!")
}

func createThreeNodeCluster() []*raft.Node {
	// Define cluster configuration
	nodeIDs := []raft.NodeID{"node-1", "node-2", "node-3"}
	addresses := map[raft.NodeID]string{
		"node-1": "localhost:8001",
		"node-2": "localhost:8002",
		"node-3": "localhost:8003",
	}

	var nodes []*raft.Node

	// Create each node with knowledge of its peers
	for _, nodeID := range nodeIDs {
		// Create peer list (all nodes except self)
		var peers []raft.NodeID
		for _, id := range nodeIDs {
			if id != nodeID {
				peers = append(peers, id)
			}
		}

		node := raft.NewNode(nodeID, peers, addresses)
		nodes = append(nodes, node)
	}

	return nodes
}

func printClusterState(nodes []*raft.Node, iteration int) {
	fmt.Printf("\n--- Iteration %d ---\n", iteration)

	var leaderCount int
	var candidateCount int
	var followerCount int

	for i, node := range nodes {
		state, term, isLeader := node.GetState()

		// Count states
		switch state {
		case raft.Leader:
			leaderCount++
		case raft.Candidate:
			candidateCount++
		case raft.Follower:
			followerCount++
		}

		// Print detailed state
		fmt.Printf("Node %d: State=%s, Term=%d, IsLeader=%v\n",
			i+1, state.String(), term, isLeader)
	}

	// Print summary
	fmt.Printf("Summary: Leaders=%d, Candidates=%d, Followers=%d\n",
		leaderCount, candidateCount, followerCount)

	// Analyze cluster health
	if leaderCount == 1 && candidateCount == 0 {
		fmt.Printf("✓ Cluster is healthy with stable leader\n")
	} else if leaderCount > 1 {
		fmt.Printf("⚠ Split brain detected! Multiple leaders\n")
	} else if leaderCount == 0 && candidateCount > 0 {
		fmt.Printf("⏳ Election in progress...\n")
	} else {
		fmt.Printf("❌ Unhealthy cluster state\n")
	}
}
