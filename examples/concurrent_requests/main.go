package main

import (
	"fmt"
	"log"
	"raft-consensus"
	"sync"
	"time"
)

func main() {
	fmt.Println("=== Concurrent Client Requests Demo ===")
	fmt.Println("Problem: Multiple clients sending requests simultaneously")
	fmt.Println("Solution: Raft serializes all requests through leader\n")

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

	fmt.Println("\n2. Testing sequential requests (baseline)...")

	// First, show sequential behavior
	sequentialCommands := []interface{}{
		"INIT counter=0",
		"INIT balance=1000",
	}

	for i, cmd := range sequentialCommands {
		fmt.Printf("   Sequential %d: %v\n", i+1, cmd)
		err := leader.SubmitCommand(cmd)
		if err != nil {
			fmt.Printf("   ❌ Failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ Success\n")
		}
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	printLogState(nodes)

	fmt.Println("\n3. Testing concurrent requests from multiple clients...")

	// Simulate multiple clients sending requests concurrently
	fmt.Println("   Launching 5 concurrent clients...")

	var wg sync.WaitGroup
	results := make([]error, 10)

	// Each client sends 2 commands
	for clientID := 1; clientID <= 5; clientID++ {
		wg.Add(2)

		// Command 1 for this client
		go func(id int) {
			defer wg.Done()
			cmd := fmt.Sprintf("INCREMENT counter client_%d_cmd_1", id)
			fmt.Printf("   Client %d: %s\n", id, cmd)
			err := leader.SubmitCommand(cmd)
			results[(id-1)*2] = err
			if err != nil {
				fmt.Printf("   Client %d cmd 1: ❌ %v\n", id, err)
			} else {
				fmt.Printf("   Client %d cmd 1: ✓\n", id)
			}
		}(clientID)

		// Command 2 for this client
		go func(id int) {
			defer wg.Done()
			cmd := fmt.Sprintf("DEBIT balance 10 client_%d_cmd_2", id)
			fmt.Printf("   Client %d: %s\n", id, cmd)
			time.Sleep(time.Duration(id*50) * time.Millisecond) // Stagger slightly
			err := leader.SubmitCommand(cmd)
			results[(id-1)*2+1] = err
			if err != nil {
				fmt.Printf("   Client %d cmd 2: ❌ %v\n", id, err)
			} else {
				fmt.Printf("   Client %d cmd 2: ✓\n", id)
			}
		}(clientID)
	}

	// Wait for all clients to complete
	wg.Wait()

	// Wait for replication
	time.Sleep(1 * time.Second)

	fmt.Println("\n   Results summary:")
	successCount := 0
	for i, err := range results {
		if err == nil {
			successCount++
		} else {
			fmt.Printf("   Command %d failed: %v\n", i+1, err)
		}
	}
	fmt.Printf("   ✓ %d/%d commands succeeded\n", successCount, len(results))

	printLogState(nodes)

	fmt.Println("\n4. Testing concurrent requests with leader failure...")

	// Start concurrent requests and then fail the leader
	fmt.Println("   Starting concurrent requests and then failing leader...")

	var failureWg sync.WaitGroup
	failureResults := make([]error, 6)

	// Start requests
	for i := 1; i <= 6; i++ {
		failureWg.Add(1)
		go func(reqNum int) {
			defer failureWg.Done()
			cmd := fmt.Sprintf("UPDATE status request_%d", reqNum)

			// Stagger the requests
			time.Sleep(time.Duration(reqNum*100) * time.Millisecond)

			fmt.Printf("   Request %d: %s\n", reqNum, cmd)
			err := leader.SubmitCommand(cmd)
			failureResults[reqNum-1] = err

			if err != nil {
				fmt.Printf("   Request %d: ❌ %v\n", reqNum, err)
			} else {
				fmt.Printf("   Request %d: ✓\n", reqNum)
			}
		}(i)
	}

	// Fail the leader after some requests have started
	go func() {
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("   💥 Stopping leader %s during concurrent requests...\n",
			getNodeID(nodes, leader))
		leader.Stop()
	}()

	failureWg.Wait()

	// Wait for new leader election
	time.Sleep(3 * time.Second)

	newLeader := findLeader(nodes)
	if newLeader != nil {
		fmt.Printf("   ✓ New leader: %s\n", getNodeID(nodes, newLeader))
	}

	// Count successes during failure
	failureSuccessCount := 0
	for _, err := range failureResults {
		if err == nil {
			failureSuccessCount++
		}
	}
	fmt.Printf("   ✓ %d/%d requests succeeded despite leader failure\n",
		failureSuccessCount, len(failureResults))

	fmt.Println("\n5. Testing client retry mechanism...")

	if newLeader != nil {
		fmt.Println("   Simulating client retries for failed requests...")

		retryCommands := []interface{}{
			"RETRY operation_1",
			"RETRY operation_2",
			"RETRY operation_3",
		}

		var retryWg sync.WaitGroup
		for i, cmd := range retryCommands {
			retryWg.Add(1)
			go func(reqID int, command interface{}) {
				defer retryWg.Done()

				fmt.Printf("   Retry %d: %v\n", reqID+1, command)
				err := newLeader.SubmitCommand(command)
				if err != nil {
					fmt.Printf("   Retry %d: ❌ %v\n", reqID+1, err)
				} else {
					fmt.Printf("   Retry %d: ✓\n", reqID+1)
				}
			}(i, cmd)
		}

		retryWg.Wait()
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n6. Testing high concurrency stress test...")

	if newLeader != nil {
		fmt.Println("   Stress test: 20 concurrent requests...")

		var stressWg sync.WaitGroup
		stressResults := make([]error, 20)

		for i := 1; i <= 20; i++ {
			stressWg.Add(1)
			go func(reqNum int) {
				defer stressWg.Done()
				cmd := fmt.Sprintf("STRESS_TEST operation_%d", reqNum)
				err := newLeader.SubmitCommand(cmd)
				stressResults[reqNum-1] = err
			}(i)
		}

		stressWg.Wait()
		time.Sleep(1 * time.Second)

		stressSuccessCount := 0
		for _, err := range stressResults {
			if err == nil {
				stressSuccessCount++
			}
		}
		fmt.Printf("   ✓ %d/%d stress test requests succeeded\n",
			stressSuccessCount, len(stressResults))
	}

	fmt.Println("\n7. Final cluster state...")
	printClusterState(nodes)
	printLogState(nodes)

	fmt.Println("\n=== Concurrent Client Requests Demo Complete ===")
	fmt.Println("Key insights:")
	fmt.Println("• All requests are serialized through the leader")
	fmt.Println("• Concurrent requests are processed in deterministic order")
	fmt.Println("• Failed requests can be safely retried with new leader")
	fmt.Println("• System maintains consistency despite high concurrency")
	fmt.Println("• Leader failure affects in-flight requests but not system state")

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

			fmt.Printf("   Node %d: State=%s, Term=%d, Leader=%v, LastLog=%d:%d, Commit=%d\n",
				i+1, state.String(), term, isLeader, lastIndex, lastTerm, commitIndex)
		} else {
			fmt.Printf("   Node %d: Stopped\n", i+1)
		}
	}
}

func printLogState(nodes []*raft_consensus.Node) {
	fmt.Println("   Log State:")
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
