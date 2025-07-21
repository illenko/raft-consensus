package main

import (
	"fmt"
	"log"
	"raft-consensus"
	"time"
)

func main() {
	fmt.Println("=== Node Crash and Recovery Demo ===")
	fmt.Println("Problem: Nodes can crash and lose volatile state")
	fmt.Println("Solution: Raft handles recovery with log replay and state sync\n")

	// Create a 3-node cluster
	nodes := createThreeNodeCluster()

	// Start all nodes
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}

	fmt.Println("1. Initial cluster setup...")
	time.Sleep(2 * time.Second)

	leader := findLeader(nodes)
	if leader == nil {
		fmt.Println("❌ No leader found")
		return
	}

	fmt.Printf("✓ Leader: %s\n", getNodeID(nodes, leader))
	printClusterState(nodes)

	fmt.Println("\n2. Building log before node failure...")

	// Submit commands to build a log
	preFailureCommands := []interface{}{
		"CREATE account alice balance=1000",
		"CREATE account bob balance=500",
		"TRANSFER alice->bob 100",
		"UPDATE alice balance=900",
		"UPDATE bob balance=600",
	}

	for i, cmd := range preFailureCommands {
		fmt.Printf("   Command %d: %v\n", i+1, cmd)
		err := leader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ Success\n")
		}
		time.Sleep(300 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("   Log state before failure:")
	printDetailedLogState(nodes)

	fmt.Println("\n3. Simulating follower node crash...")

	// Choose a follower to crash
	var crashedNode *raft_consensus.Node
	var crashedNodeIndex int
	for i, node := range nodes {
		if !node.IsLeader() {
			crashedNode = node
			crashedNodeIndex = i
			break
		}
	}

	if crashedNode == nil {
		fmt.Println("❌ No follower found to crash")
		return
	}

	fmt.Printf("   Crashing follower node-%d...\n", crashedNodeIndex+1)
	crashedNode.Stop()

	fmt.Println("   Cluster state after node crash:")
	printClusterState(nodes)

	fmt.Println("\n4. Continuing operations with crashed node...")

	// Continue submitting commands while node is down
	duringFailureCommands := []interface{}{
		"CREATE account charlie balance=750",
		"TRANSFER bob->charlie 150",
		"UPDATE charlie balance=900",
		"DELETE account alice",
	}

	for i, cmd := range duringFailureCommands {
		fmt.Printf("   Command %d: %v\n", i+1, cmd)
		err := leader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ Success\n")
		}
		time.Sleep(300 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("   Log state while node is crashed:")
	printDetailedLogState(nodes)

	fmt.Println("\n5. Recovering the crashed node...")

	// Create a new instance to simulate recovery from crash
	// In a real system, this would restart with persistent state
	fmt.Printf("   Restarting node-%d...\n", crashedNodeIndex+1)

	// Create new node instance (simulating restart)
	nodeIDs := []raft_consensus.NodeID{"node-1", "node-2", "node-3"}
	addresses := map[raft_consensus.NodeID]string{
		"node-1": "localhost:8001",
		"node-2": "localhost:8002",
		"node-3": "localhost:8003",
	}

	recoveredNodeID := nodeIDs[crashedNodeIndex]
	var peers []raft_consensus.NodeID
	for _, id := range nodeIDs {
		if id != recoveredNodeID {
			peers = append(peers, id)
		}
	}

	recoveredNode := raft_consensus.NewNode(recoveredNodeID, peers, addresses)
	nodes[crashedNodeIndex] = recoveredNode

	if err := recoveredNode.Start(); err != nil {
		log.Fatalf("Failed to restart node: %v", err)
	}

	fmt.Printf("   ✓ Node-%d restarted\n", crashedNodeIndex+1)

	// Wait for the node to catch up
	fmt.Println("   Waiting for node to catch up...")
	time.Sleep(3 * time.Second)

	fmt.Println("   Cluster state after recovery:")
	printClusterState(nodes)

	fmt.Println("\n6. Verifying recovered node state...")

	// Check if recovered node has caught up
	time.Sleep(2 * time.Second)

	recoveredConsistent := checkNodeConsistency(nodes, crashedNodeIndex)
	if recoveredConsistent {
		fmt.Printf("   ✓ Node-%d successfully caught up with cluster\n", crashedNodeIndex+1)
	} else {
		fmt.Printf("   ❌ Node-%d still inconsistent\n", crashedNodeIndex+1)
	}

	fmt.Println("   Final log comparison:")
	printDetailedLogState(nodes)

	fmt.Println("\n7. Testing operations after recovery...")

	// Submit new commands to verify full cluster operation
	postRecoveryCommands := []interface{}{
		"CREATE account david balance=1200",
		"AUDIT all_accounts",
		"BACKUP system_state",
	}

	for i, cmd := range postRecoveryCommands {
		fmt.Printf("   Post-recovery command %d: %v\n", i+1, cmd)
		err := leader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ Success\n")
		}
		time.Sleep(300 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("   Final cluster state:")
	printDetailedLogState(nodes)

	fmt.Println("\n8. Testing leader crash and recovery...")

	// Now crash the leader
	currentLeader := findLeader(nodes)
	if currentLeader != nil {
		leaderID := getNodeID(nodes, currentLeader)
		fmt.Printf("   Crashing current leader %s...\n", leaderID)
		currentLeader.Stop()

		// Wait for new election
		time.Sleep(3 * time.Second)

		newLeader := findLeader(nodes)
		if newLeader != nil {
			fmt.Printf("   ✓ New leader elected: %s\n", getNodeID(nodes, newLeader))

			// Test that system still works
			fmt.Println("   Testing system after leader crash:")
			err := newLeader.SubmitCommand("VERIFY system_integrity")
			if err != nil {
				fmt.Printf("   ❌ System test failed: %v\n", err)
			} else {
				fmt.Printf("   ✓ System functioning normally\n")
			}
		} else {
			fmt.Println("   ❌ No new leader elected")
		}
	}

	fmt.Println("\n=== Node Crash and Recovery Demo Complete ===")
	fmt.Println("Key insights:")
	fmt.Println("• Crashed nodes can rejoin and catch up automatically")
	fmt.Println("• Log replication ensures no data loss during recovery")
	fmt.Println("• System continues operating with majority of nodes")
	fmt.Println("• Leader crashes trigger automatic leader election")
	fmt.Println("• Persistent state would preserve data across restarts")
	fmt.Println("• Recovery is transparent to clients once complete")

	// Cleanup
	for _, node := range nodes {
		if node.IsRunning() {
			node.Stop()
		}
	}
}

func createThreeNodeCluster() []*raft_consensus.Node {
	nodeIDs := []raft_consensus.NodeID{"node-1", "node-2", "node-3"}
	addresses := map[raft_consensus.NodeID]string{
		"node-1": "localhost:8001",
		"node-2": "localhost:8002",
		"node-3": "localhost:8003",
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

func findLeader(nodes []*raft_consensus.Node) *raft_consensus.Node {
	for _, node := range nodes {
		if node.IsRunning() && node.IsLeader() {
			return node
		}
	}
	return nil
}

func getNodeID(nodes []*raft_consensus.Node, target *raft_consensus.Node) string {
	for i, node := range nodes {
		if node == target {
			return fmt.Sprintf("node-%d", i+1)
		}
	}
	return "unknown"
}

func checkNodeConsistency(nodes []*raft_consensus.Node, nodeIndex int) bool {
	if nodeIndex >= len(nodes) || !nodes[nodeIndex].IsRunning() {
		return false
	}

	// Get the recovered node's state
	_, _, recoveredCommit := nodes[nodeIndex].GetLogInfo()

	// Compare with other running nodes
	for i, node := range nodes {
		if i != nodeIndex && node.IsRunning() {
			_, _, commit := node.GetLogInfo()
			// Allow small differences due to timing
			if abs(int(commit)-int(recoveredCommit)) > 2 {
				return false
			}
		}
	}

	return true
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func printClusterState(nodes []*raft_consensus.Node) {
	fmt.Println("   Cluster State:")
	for i, node := range nodes {
		if node.IsRunning() {
			state, term, isLeader := node.GetState()
			lastIndex, lastTerm, commitIndex := node.GetLogInfo()

			fmt.Printf("   Node %d: Running, State=%s, Term=%d, Leader=%v, LastLog=%d:%d, Commit=%d\n",
				i+1, state.String(), term, isLeader, lastIndex, lastTerm, commitIndex)
		} else {
			fmt.Printf("   Node %d: Stopped\n", i+1)
		}
	}
}

func printDetailedLogState(nodes []*raft_consensus.Node) {
	for i, node := range nodes {
		if node.IsRunning() {
			lastIndex, lastTerm, commitIndex := node.GetLogInfo()
			state, term, isLeader := node.GetState()

			fmt.Printf("   Node %d (%s, Term %d, Leader=%v):\n",
				i+1, state.String(), term, isLeader)
			fmt.Printf("     LastIndex=%d, LastTerm=%d, CommitIndex=%d\n",
				lastIndex, lastTerm, commitIndex)
		} else {
			fmt.Printf("   Node %d: Stopped\n", i+1)
		}
	}
}
