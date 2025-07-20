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

	fmt.Println("=== Raft Log Replication Demo ===")
	fmt.Println("1. Waiting for leader election...")

	// Wait for leader election to complete
	time.Sleep(2 * time.Second)

	// Find the leader
	var leader *raft.Node
	var leaderID raft.NodeID
	for i, node := range nodes {
		if node.IsLeader() {
			leader = node
			leaderID = raft.NodeID(fmt.Sprintf("node-%d", i+1))
			break
		}
	}

	if leader == nil {
		fmt.Println("❌ No leader elected, exiting...")
		return
	}

	fmt.Printf("✓ Leader elected: %s\n", leaderID)
	printSimpleClusterState(nodes)

	fmt.Println("\n2. Submitting commands to leader...")

	// Submit some commands
	commands := []interface{}{
		"SET x=1",
		"SET y=2",
		"SET z=3",
		"INCREMENT x",
		"DELETE y",
	}

	for i, cmd := range commands {
		fmt.Printf("   Submitting command %d: %v\n", i+1, cmd)

		err := leader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed to submit command: %v\n", err)
		} else {
			fmt.Printf("   ✓ Command submitted successfully\n")
		}

		// Give time for replication
		time.Sleep(500 * time.Millisecond)

		// Show log state after each command
		printLogState(nodes, i+1)
	}

	fmt.Println("\n3. Final cluster state:")
	printSimpleClusterState(nodes)
	printDetailedLogState(nodes)

	// Test leader failure scenario
	fmt.Println("\n4. Testing leader failure and recovery...")

	// Stop the current leader
	fmt.Printf("   Stopping leader %s...\n", leaderID)
	leader.Stop()

	// Wait for new election
	time.Sleep(3 * time.Second)

	// Find new leader
	var newLeader *raft.Node
	var newLeaderID raft.NodeID
	for i, node := range nodes {
		nodeID := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if nodeID != leaderID && node.IsLeader() {
			newLeader = node
			newLeaderID = nodeID
			break
		}
	}

	if newLeader != nil {
		fmt.Printf("   ✓ New leader elected: %s\n", newLeaderID)

		// Submit more commands to new leader
		fmt.Println("   Submitting commands to new leader...")
		newCommands := []interface{}{
			"SET a=10",
			"SET b=20",
		}

		for _, cmd := range newCommands {
			fmt.Printf("   Submitting: %v\n", cmd)
			err := newLeader.SubmitCommand(cmd)
			if err != nil {
				fmt.Printf("   ❌ Failed: %v\n", err)
			} else {
				fmt.Printf("   ✓ Success\n")
			}
			time.Sleep(500 * time.Millisecond)
		}
	} else {
		fmt.Println("   ❌ No new leader elected")
	}

	fmt.Println("\n5. Final state after leader change:")
	printDetailedLogState(nodes)

	// Stop remaining nodes
	fmt.Println("\n=== Shutting Down Cluster ===")
	for i, node := range nodes {
		nodeID := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if nodeID != leaderID { // Don't stop the already stopped leader
			if err := node.Stop(); err != nil {
				log.Printf("Error stopping node %d: %v", i+1, err)
			}
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

func printSimpleClusterState(nodes []*raft.Node) {
	fmt.Println("   Cluster State:")
	for i, node := range nodes {
		state, term, isLeader := node.GetState()
		lastIndex, lastTerm, commitIndex := node.GetLogInfo()

		fmt.Printf("   Node %d: State=%s, Term=%d, Leader=%v, LastLog=%d:%d, Commit=%d\n",
			i+1, state.String(), term, isLeader, lastIndex, lastTerm, commitIndex)
	}
}

func printLogState(nodes []*raft.Node, commandNum int) {
	fmt.Printf("   Log state after command %d:\n", commandNum)
	for i, node := range nodes {
		lastIndex, lastTerm, commitIndex := node.GetLogInfo()
		fmt.Printf("     Node %d: LastLog=%d:%d, Commit=%d\n",
			i+1, lastIndex, lastTerm, commitIndex)
	}
}

func printDetailedLogState(nodes []*raft.Node) {
	fmt.Println("   Detailed Log State:")
	for i, node := range nodes {
		lastIndex, lastTerm, commitIndex := node.GetLogInfo()
		state, term, isLeader := node.GetState()

		fmt.Printf("   Node %d (%s, Term %d, Leader=%v):\n",
			i+1, state.String(), term, isLeader)
		fmt.Printf("     LastIndex=%d, LastTerm=%d, CommitIndex=%d\n",
			lastIndex, lastTerm, commitIndex)

		if lastIndex > 0 {
			fmt.Printf("     Log contents: [entries 1-%d]\n", lastIndex)
		} else {
			fmt.Printf("     Log contents: [empty]\n")
		}
	}
}
