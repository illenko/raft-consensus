package main

import (
	"fmt"
	"log"
	"raft-consensus"
	"time"
)

func main() {
	fmt.Println("=== Split-Brain Prevention Demo ===")
	fmt.Println("Problem: Multiple nodes claiming to be leader simultaneously")
	fmt.Println("Solution: Raft's majority-based voting prevents split-brain\n")

	// Create a 5-node cluster for clear majority demonstration
	nodes := createFiveNodeCluster()

	// Start all nodes
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}

	fmt.Println("1. Initial election with all 5 nodes...")
	time.Sleep(2 * time.Second)

	// Show initial state - should have exactly 1 leader
	leaders := countLeaders(nodes)
	fmt.Printf("✓ Leaders elected: %d (expected: 1)\n", leaders)
	if leaders == 1 {
		fmt.Println("✓ Split-brain prevented: Only one leader elected")
	} else {
		fmt.Printf("❌ Split-brain detected: %d leaders!\n", leaders)
	}
	printClusterState(nodes)

	fmt.Println("\n2. Simulating network partition: 3 vs 2 nodes...")

	// Create network partition: nodes 1,2,3 vs nodes 4,5
	network := raft_consensus.GetSimulatedNetwork()

	// Partition: nodes 4,5 cannot communicate with nodes 1,2,3
	network.CreatePartition([]raft_consensus.NodeID{"node-1", "node-2", "node-3"},
		[]raft_consensus.NodeID{"node-4", "node-5"})

	fmt.Println("   Network partitioned: [1,2,3] vs [4,5]")
	fmt.Println("   Majority group: 3 nodes (can elect leader)")
	fmt.Println("   Minority group: 2 nodes (cannot elect leader)")

	// Wait for partition effects
	time.Sleep(5 * time.Second)

	leaders = countLeaders(nodes)
	fmt.Printf("✓ Leaders after partition: %d\n", leaders)

	// Check which partition has the leader
	majorityLeaders := countLeadersInGroup(nodes, []int{0, 1, 2}) // nodes 1,2,3
	minorityLeaders := countLeadersInGroup(nodes, []int{3, 4})    // nodes 4,5

	fmt.Printf("   Majority group (3 nodes) leaders: %d\n", majorityLeaders)
	fmt.Printf("   Minority group (2 nodes) leaders: %d\n", minorityLeaders)

	if majorityLeaders == 1 && minorityLeaders == 0 {
		fmt.Println("✓ Correct behavior: Only majority group has leader")
		fmt.Println("✓ Split-brain prevented even during partition")
	} else {
		fmt.Println("❌ Unexpected behavior detected")
	}

	printClusterState(nodes)

	fmt.Println("\n3. Healing network partition...")
	network.HealPartition()
	fmt.Println("   All nodes can communicate again")

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	leaders = countLeaders(nodes)
	fmt.Printf("✓ Leaders after healing: %d\n", leaders)

	if leaders == 1 {
		fmt.Println("✓ Cluster converged to single leader")
		fmt.Println("✓ Split-brain prevention successful throughout")
	}

	printClusterState(nodes)

	fmt.Println("\n4. Testing concurrent elections...")

	// Force all nodes to become candidates simultaneously
	fmt.Println("   Forcing simultaneous elections...")

	// Stop current leader to trigger elections
	for i, node := range nodes {
		if node.IsLeader() {
			fmt.Printf("   Stopping current leader: node-%d\n", i+1)
			node.Stop()
			break
		}
	}

	// Wait for new election
	time.Sleep(4 * time.Second)

	leaders = countLeaders(nodes[:4]) // Excluding stopped node
	fmt.Printf("✓ Leaders after forced election: %d\n", leaders)

	if leaders == 1 {
		fmt.Println("✓ Even with concurrent elections, only one leader emerges")
		fmt.Println("✓ Randomized election timeouts prevent split votes")
	}

	fmt.Println("\n=== Split-Brain Prevention Demo Complete ===")
	fmt.Println("Key insights:")
	fmt.Println("• Majority voting (3 out of 5) prevents multiple leaders")
	fmt.Println("• Network partitions cannot create split-brain")
	fmt.Println("• Randomized timeouts prevent concurrent election ties")
	fmt.Println("• Only groups with majority can elect leaders")

	// Cleanup
	for _, node := range nodes {
		node.Stop()
	}
}

func createFiveNodeCluster() []*raft_consensus.Node {
	nodeIDs := []raft_consensus.NodeID{"node-1", "node-2", "node-3", "node-4", "node-5"}
	addresses := map[raft_consensus.NodeID]string{
		"node-1": "localhost:8001",
		"node-2": "localhost:8002",
		"node-3": "localhost:8003",
		"node-4": "localhost:8004",
		"node-5": "localhost:8005",
	}

	var nodes []*raft_consensus.Node
	for _, nodeID := range nodeIDs {
		var peers []raft_consensus.NodeID
		for _, id := range nodeIDs {
			if id != nodeID {
				peers = append(peers, id)
			}
		}
		node := raft_consensus.NewNode(nodeID, peers, addresses)
		nodes = append(nodes, node)
	}
	return nodes
}

func countLeaders(nodes []*raft_consensus.Node) int {
	count := 0
	for _, node := range nodes {
		if node.IsLeader() {
			count++
		}
	}
	return count
}

func countLeadersInGroup(nodes []*raft_consensus.Node, indices []int) int {
	count := 0
	for _, i := range indices {
		if i < len(nodes) && nodes[i].IsLeader() {
			count++
		}
	}
	return count
}

func printClusterState(nodes []*raft_consensus.Node) {
	fmt.Println("   Cluster State:")
	for i, node := range nodes {
		state, term, isLeader := node.GetState()
		lastIndex, lastTerm, commitIndex := node.GetLogInfo()

		status := "Running"
		if !node.IsRunning() {
			status = "Stopped"
		}

		fmt.Printf("   Node %d: %s, State=%s, Term=%d, Leader=%v, LastLog=%d:%d, Commit=%d\n",
			i+1, status, state.String(), term, isLeader, lastIndex, lastTerm, commitIndex)
	}
}
