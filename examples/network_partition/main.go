package main

import (
	"fmt"
	"log"
	"raft-consensus"
	"time"
)

func main() {
	fmt.Println("=== Network Partition Handling Demo ===")
	fmt.Println("Problem: Network partitions can cause inconsistency")
	fmt.Println("Solution: Raft maintains consistency through majority consensus\n")

	// Create a 5-node cluster
	nodes := createFiveNodeCluster()

	// Start all nodes
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}

	fmt.Println("1. Initial cluster setup...")
	time.Sleep(2 * time.Second)

	// Find leader and submit initial commands
	leader := findLeader(nodes)
	if leader == nil {
		fmt.Println("❌ No leader found")
		return
	}

	fmt.Printf("✓ Leader: %s\n", getNodeID(nodes, leader))
	printClusterState(nodes)

	fmt.Println("\n2. Submitting commands to leader...")

	// Submit some initial commands
	commands := []interface{}{
		"SET initial=1",
		"SET before_partition=2",
	}

	for i, cmd := range commands {
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
	fmt.Println("   Initial commands replicated:")
	printLogState(nodes)

	fmt.Println("\n3. Creating network partition...")

	// Create partition: majority (3) vs minority (2)
	network := raft_consensus.GetSimulatedNetwork()
	majorityNodes := []raft_consensus.NodeID{"node-1", "node-2", "node-3"}
	minorityNodes := []raft_consensus.NodeID{"node-4", "node-5"}

	network.CreatePartition(majorityNodes, minorityNodes)
	fmt.Printf("   Partition: %v vs %v\n", majorityNodes, minorityNodes)

	// Wait for partition to take effect
	time.Sleep(3 * time.Second)

	// Check which partition has the leader
	majorityLeader := findLeaderInGroup(nodes, []int{0, 1, 2})
	minorityLeader := findLeaderInGroup(nodes, []int{3, 4})

	fmt.Printf("   Majority partition leader: %v\n", majorityLeader != nil)
	fmt.Printf("   Minority partition leader: %v\n", minorityLeader != nil)

	if majorityLeader != nil && minorityLeader == nil {
		fmt.Println("✓ Correct: Only majority partition can elect leader")
	}

	fmt.Println("\n4. Attempting commands on both partitions...")

	// Try to submit commands to majority partition
	if majorityLeader != nil {
		fmt.Println("   Submitting to majority partition:")
		majorityCommands := []interface{}{
			"SET majority_1=100",
			"SET majority_2=200",
		}

		for _, cmd := range majorityCommands {
			fmt.Printf("     %v: ", cmd)
			err := majorityLeader.SubmitCommand(cmd)
			if err != nil {
				fmt.Printf("Failed (%v)\n", err)
			} else {
				fmt.Printf("Success\n")
			}
			time.Sleep(300 * time.Millisecond)
		}
	}

	// Try to submit commands to minority partition nodes
	fmt.Println("   Attempting commands on minority partition:")
	minorityNode := nodes[3] // node-4
	minorityCommands := []interface{}{
		"SET minority_1=300",
		"SET minority_2=400",
	}

	for _, cmd := range minorityCommands {
		fmt.Printf("     %v: ", cmd)
		err := minorityNode.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("Failed (%v) ✓ Expected\n", err)
		} else {
			fmt.Printf("Success ❌ Unexpected\n")
		}
		time.Sleep(300 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("\n   Log state during partition:")
	printLogState(nodes)

	fmt.Println("\n5. Healing network partition...")

	network.HealPartition()
	fmt.Println("   Network partition healed")

	// Wait for cluster to synchronize
	time.Sleep(4 * time.Second)

	// Check final state
	fmt.Println("   Cluster state after healing:")
	printClusterState(nodes)

	fmt.Println("   Final log state:")
	printLogState(nodes)

	// Submit commands to verify cluster is working
	fmt.Println("\n6. Verifying cluster operation after healing...")

	finalLeader := findLeader(nodes)
	if finalLeader != nil {
		fmt.Printf("   New leader: %s\n", getNodeID(nodes, finalLeader))

		finalCommands := []interface{}{
			"SET after_healing=500",
			"SET final=600",
		}

		for _, cmd := range finalCommands {
			fmt.Printf("   %v: ", cmd)
			err := finalLeader.SubmitCommand(cmd)
			if err != nil {
				fmt.Printf("Failed (%v)\n", err)
			} else {
				fmt.Printf("Success\n")
			}
			time.Sleep(300 * time.Millisecond)
		}

		time.Sleep(1 * time.Second)
		fmt.Println("\n   Final log state:")
		printLogState(nodes)
	}

	fmt.Println("\n=== Network Partition Demo Complete ===")
	fmt.Println("Key insights:")
	fmt.Println("• Only majority partition can continue operations")
	fmt.Println("• Minority partition becomes unavailable (prevents split-brain)")
	fmt.Println("• Commands during partition only committed in majority")
	fmt.Println("• Cluster automatically heals when partition resolves")
	fmt.Println("• Consistency maintained throughout partition event")

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

func findLeader(nodes []*raft_consensus.Node) *raft_consensus.Node {
	for _, node := range nodes {
		if node.IsLeader() {
			return node
		}
	}
	return nil
}

func findLeaderInGroup(nodes []*raft_consensus.Node, indices []int) *raft_consensus.Node {
	for _, i := range indices {
		if i < len(nodes) && nodes[i].IsLeader() {
			return nodes[i]
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
		state, term, isLeader := node.GetState()
		lastIndex, lastTerm, commitIndex := node.GetLogInfo()

		fmt.Printf("   Node %d: State=%s, Term=%d, Leader=%v, LastLog=%d:%d, Commit=%d\n",
			i+1, state.String(), term, isLeader, lastIndex, lastTerm, commitIndex)
	}
}

func printLogState(nodes []*raft_consensus.Node) {
	for i, node := range nodes {
		lastIndex, lastTerm, commitIndex := node.GetLogInfo()
		fmt.Printf("   Node %d: LastLog=%d:%d, Commit=%d\n",
			i+1, lastIndex, lastTerm, commitIndex)
	}
}
