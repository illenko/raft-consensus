package raft

import (
	"log"
)

// sendAppendEntries sends AppendEntries RPC to a specific follower with actual log entries
func (n *Node) sendAppendEntriesToFollower(followerID NodeID) {
	n.mu.RLock()
	if n.volatileState.GetState() != Leader {
		n.mu.RUnlock()
		return
	}

	term := n.persistentState.GetCurrentTerm()
	leaderID := n.config.NodeID
	leaderCommit := n.volatileState.GetCommitIndex()

	// Get the next index to send to this follower
	nextIndex := n.volatileState.NextIndex[followerID]
	if nextIndex == 0 {
		nextIndex = 1 // Start from index 1 if not initialized
	}

	// Calculate previous log entry for consistency check
	var prevLogIndex LogIndex
	var prevLogTerm Term

	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
		if entry, exists := n.persistentState.Log.Get(prevLogIndex); exists {
			prevLogTerm = entry.Term
		} else {
			// Log entry doesn't exist, this shouldn't happen in correct implementation
			log.Printf("Node %s: Missing log entry at index %d", n.config.NodeID, prevLogIndex)
			n.mu.RUnlock()
			return
		}
	} else {
		prevLogIndex = 0
		prevLogTerm = 0
	}

	// Get entries to send (from nextIndex onwards)
	var entries []LogEntry
	lastLogIndex := n.persistentState.Log.LastIndex()

	if nextIndex <= lastLogIndex {
		// Send actual log entries
		for i := nextIndex; i <= lastLogIndex; i++ {
			if entry, exists := n.persistentState.Log.Get(i); exists {
				entries = append(entries, entry)
			}
		}
	}
	// If nextIndex > lastLogIndex, send empty entries (heartbeat)

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	n.mu.RUnlock()

	// Send via network
	network := GetSimulatedNetwork()

	go func() {
		reply, err := network.SendAppendEntries(n.config.NodeID, followerID, args)
		if err != nil {
			log.Printf("Node %s: Failed to send AppendEntries to %s: %v",
				n.config.NodeID, followerID, err)
			return
		}

		n.handleAppendEntriesReplyFromFollower(followerID, args, reply)
	}()
}

// handleAppendEntriesReplyFromFollower processes AppendEntries reply from followers
func (n *Node) handleAppendEntriesReplyFromFollower(followerID NodeID, args AppendEntriesArgs, reply AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Ignore if we're no longer leader
	if n.volatileState.GetState() != Leader {
		return
	}

	// If reply term is higher, step down
	if reply.Term > n.persistentState.GetCurrentTerm() {
		n.stepDown(reply.Term)
		return
	}

	// Ignore stale replies
	if reply.Term < n.persistentState.GetCurrentTerm() {
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex for successful replication
		if len(args.Entries) > 0 {
			// Update match index to the last entry we sent
			lastSentIndex := args.PrevLogIndex + LogIndex(len(args.Entries))
			n.volatileState.MatchIndex[followerID] = lastSentIndex
			n.volatileState.NextIndex[followerID] = lastSentIndex + 1

			log.Printf("Node %s: Successfully replicated to %s up to index %d",
				n.config.NodeID, followerID, lastSentIndex)

			// Check if we can commit new entries
			n.updateCommitIndex()
		} else {
			// Heartbeat successful
			log.Printf("Node %s: Successful heartbeat to %s", n.config.NodeID, followerID)
		}
	} else {
		// Log replication failed - decrement nextIndex and retry
		if n.volatileState.NextIndex[followerID] > 1 {
			n.volatileState.NextIndex[followerID]--
			log.Printf("Node %s: Log mismatch with %s, decremented nextIndex to %d",
				n.config.NodeID, followerID, n.volatileState.NextIndex[followerID])

			// Retry immediately
			go n.sendAppendEntriesToFollower(followerID)
		}
	}
}

// updateCommitIndex checks if new entries can be committed
func (n *Node) updateCommitIndex() {
	if n.volatileState.GetState() != Leader {
		return
	}

	currentCommitIndex := n.volatileState.GetCommitIndex()
	lastLogIndex := n.persistentState.Log.LastIndex()

	// Try to commit entries from commitIndex+1 to lastLogIndex
	for index := currentCommitIndex + 1; index <= lastLogIndex; index++ {
		// Check if this entry is from current term
		if entry, exists := n.persistentState.Log.Get(index); exists {
			if entry.Term != n.persistentState.GetCurrentTerm() {
				continue // Can only commit entries from current term
			}

			// Count how many nodes have this entry
			replicationCount := 1 // Count leader itself
			for _, matchIndex := range n.volatileState.MatchIndex {
				if matchIndex >= index {
					replicationCount++
				}
			}

			// If majority has this entry, commit it
			majoritySize := n.config.MajoritySize()
			if replicationCount >= majoritySize {
				n.volatileState.SetCommitIndex(index)
				log.Printf("Node %s: Committed entry at index %d (replicated on %d/%d nodes)",
					n.config.NodeID, index, replicationCount, n.config.ClusterSize())
			} else {
				// If this entry can't be committed, later entries can't either
				break
			}
		}
	}
}

// applyCommittedEntries applies committed log entries to the state machine
func (n *Node) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	commitIndex := n.volatileState.GetCommitIndex()
	lastApplied := n.volatileState.LastApplied

	// Apply entries from lastApplied+1 to commitIndex
	for index := lastApplied + 1; index <= commitIndex; index++ {
		if entry, exists := n.persistentState.Log.Get(index); exists {
			// Apply to state machine (simplified - just log the command)
			log.Printf("Node %s: Applied command to state machine: %v (index=%d, term=%d)",
				n.config.NodeID, entry.Command, entry.Index, entry.Term)

			// Update lastApplied
			n.volatileState.LastApplied = index
		}
	}
}

// startReplicationToFollower starts continuous replication to a specific follower
func (n *Node) startReplicationToFollower(followerID NodeID) {
	// Send initial replication
	n.sendAppendEntriesToFollower(followerID)
}

// AppendEntry appends a new entry to the leader's log and starts replication
func (n *Node) AppendEntry(command interface{}) error {
	n.mu.Lock()

	if n.volatileState.GetState() != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	// Create new log entry
	newIndex := n.persistentState.Log.LastIndex() + 1
	currentTerm := n.persistentState.GetCurrentTerm()

	entry := LogEntry{
		Index:   newIndex,
		Term:    currentTerm,
		Command: command,
	}

	// Append to leader's log
	n.persistentState.Log.Append(entry)

	log.Printf("Node %s: Appended entry to log: %v (index=%d, term=%d)",
		n.config.NodeID, command, newIndex, currentTerm)

	n.mu.Unlock()

	// Start replication to all followers
	for _, peerID := range n.config.Peers {
		go n.sendAppendEntriesToFollower(peerID)
	}

	return nil
}
