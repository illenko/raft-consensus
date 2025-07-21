package main

import (
	"fmt"
	"log"
	"raft-consensus"
	"time"
)

func main() {
	fmt.Println("=== Leader Failure Recovery Demo ===")
	fmt.Println("Problem: Leader crashes can halt the system")
	fmt.Println("Solution: Raft automatically elects new leader and continues\n")

	// Create a 3-node cluster for simpler demonstration
	nodes := createThreeNodeCluster()

	// Start all nodes
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}

	fmt.Println("1. Initial cluster setup and leader election...")
	time.Sleep(2 * time.Second)

	// Find initial leader
	initialLeader := findLeader(nodes)
	if initialLeader == nil {
		fmt.Println("❌ No leader found")
		return
	}

	initialLeaderID := getNodeID(nodes, initialLeader)
	fmt.Printf("✓ Initial leader: %s\n", initialLeaderID)
	printClusterState(nodes)

	fmt.Println("\n2. Submitting commands to initial leader...")

	// Submit commands before failure
	preFailureCommands := []interface{}{
		"SET user1=alice",
		"SET user2=bob",
		"SET counter=10",
	}

	for i, cmd := range preFailureCommands {
		fmt.Printf("   Command %d: %v\n", i+1, cmd)
		err := initialLeader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ Success\n")
		}
		time.Sleep(300 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("   Commands before failure:")
	printLogState(nodes)

	fmt.Println("\n3. Simulating leader failure...")

	// Stop the current leader
	fmt.Printf("   Stopping leader %s...\n", initialLeaderID)
	initialLeader.Stop()

	// Show immediate state
	fmt.Println("   Immediate state after leader failure:")
	printClusterState(nodes)

	// Wait for election timeout and new election
	fmt.Println("   Waiting for new leader election...")
	time.Sleep(3 * time.Second)

	fmt.Println("\n4. New leader election results...")

	// Find new leader
	newLeader := findLeader(nodes)
	if newLeader == nil {
		fmt.Println("❌ No new leader elected")
		return
	}

	newLeaderID := getNodeID(nodes, newLeader)
	fmt.Printf("✓ New leader elected: %s\n", newLeaderID)

	if newLeaderID == initialLeaderID {
		fmt.Println("❌ Same leader - unexpected!")
	} else {
		fmt.Println("✓ Leadership transferred successfully")
	}

	printClusterState(nodes)

	fmt.Println("\n5. Testing new leader functionality...")

	// Submit commands to new leader
	postFailureCommands := []interface{}{
		"SET recovery=successful",
		"INCREMENT counter",
		"SET user3=charlie",
	}

	for i, cmd := range postFailureCommands {
		fmt.Printf("   Command %d: %v\n", i+1, cmd)
		err := newLeader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ Success\n")
		}
		time.Sleep(300 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("   Commands after leader change:")
	printLogState(nodes)

	fmt.Println("\n6. Testing client request handling during failure...")

	// Find current leader and simulate failure during command processing
	currentLeader := findLeader(nodes)
	if currentLeader == nil {
		fmt.Println("   No leader available")
	} else {
		fmt.Printf("   Current leader: %s\n", getNodeID(nodes, currentLeader))

		// Try to submit command and then immediately stop leader
		fmt.Println("   Submitting command and immediately stopping leader...")

		go func() {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("   Stopping leader %s during command processing...\n",
				getNodeID(nodes, currentLeader))
			currentLeader.Stop()
		}()

		err := currentLeader.SubmitCommand("SET during_failure=test")
		if err != nil {
			fmt.Printf("   ❌ Command failed (expected): %v\n", err)
		} else {
			fmt.Printf("   ✓ Command succeeded despite failure\n")
		}

		// Wait for new election
		time.Sleep(3 * time.Second)

		// Check if we have a leader from remaining node
		remainingLeader := findLeader(nodes)
		if remainingLeader != nil {
			fmt.Printf("   ✓ New leader: %s\n", getNodeID(nodes, remainingLeader))

			// Retry the command
			fmt.Println("   Retrying failed command with new leader...")
			err := remainingLeader.SubmitCommand("SET retry=success")
			if err != nil {
				fmt.Printf("   ❌ Retry failed: %v\n", err)
			} else {
				fmt.Printf("   ✓ Retry succeeded\n")
			}
		} else {
			fmt.Println("   ❌ No leader available (insufficient nodes)")
		}
	}

	time.Sleep(1 * time.Second)
	fmt.Println("\n7. Final cluster state:")
	printClusterState(nodes)
	printLogState(nodes)

	fmt.Println("\n8. Testing cluster with only one node (no majority)...")

	// Count running nodes
	runningNodes := 0
	for _, node := range nodes {
		if node.IsRunning() {
			runningNodes++
		}
	}

	fmt.Printf("   Running nodes: %d\n", runningNodes)

	if runningNodes < 2 {
		fmt.Println("   ✓ Cluster unavailable with minority of nodes")
		fmt.Println("   ✓ Prevents split-brain and maintains consistency")

		// Try to submit command - should fail
		for _, node := range nodes {
			if node.IsRunning() {
				err := node.SubmitCommand("SET impossible=true")
				if err != nil {
					fmt.Printf("   ✓ Command rejected: %v\n", err)
				} else {
					fmt.Printf("   ❌ Command accepted (unexpected)\n")
				}
				break
			}
		}
	}

	fmt.Println("\n=== Leader Failure Recovery Demo Complete ===")
	fmt.Println("Key insights:")
	fmt.Println("• New leader automatically elected after failure")
	fmt.Println("• Commands continue processing with new leader")
	fmt.Println("• Client requests can be retried with new leader")
	fmt.Println("• System becomes unavailable when majority fails")
	fmt.Println("• Consistency maintained throughout leadership changes")

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

func printLogState(nodes []*raft_consensus.Node) {
	for i, node := range nodes {
		if node.IsRunning() {
			lastIndex, lastTerm, commitIndex := node.GetLogInfo()
			fmt.Printf("   Node %d: LastLog=%d:%d, Commit=%d\n",
				i+1, lastIndex, lastTerm, commitIndex)
		} else {
			fmt.Printf("   Node %d: Stopped\n", i+1)
		}
	}
}
