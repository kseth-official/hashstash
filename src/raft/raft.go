package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"lab5/labgob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
)

const ShouldLog bool = false

/*
MAGIC STRINGS FOR RPC CALLS
*/
const (
	RequestVote   = "Raft.RequestVote"
	AppendEntries = "Raft.AppendEntries"
)

/*
RAFT SERVER STATES
*/
const (
	Follower = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

/*
A struct to define each log entry.
Command: The command for the state machine.
Term: The term when entry was received by the leader (first index is 1).
*/
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logger *logger.Logger

	// Your data here (4B, 4C).

	// Persistent State of server
	currentTerm int        // latest term server has seen (0 init, monotonically increasing)
	votedFor    int        // stores the candidate ID that received vote in current term or null (-1) if none.
	log         []LogEntry // entries

	// Volatile State of server
	commitIndex int // index of highest log entry known to be committed (0 init, monotonically increasing)
	lastApplied int // index of highest log entry applied to state machine (0 init, monotonically increasing)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (init to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (0 init, monotonically increasing)

	state int // the server's current state. One of Follower, Candidate, and Leader

	lastHeartbeatTime time.Time // time of last heartbeat or vote

	applyCh      chan ApplyMsg // channel for Apply messages
	applyCond    *sync.Cond    // condition variable for applying commits
	applyTrigger chan struct{} // channel to trigger apply commits
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Modified to assume caller already holds lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	errEncodeCurrentTerm := e.Encode(rf.currentTerm)
	if errEncodeCurrentTerm != nil {
		rf.logger.Log(constants.LogEncodingError, "Error encoding currentTerm: %v", errEncodeCurrentTerm)
		return
	}
	errEncodeVotedFor := e.Encode(rf.votedFor)
	if errEncodeVotedFor != nil {
		rf.logger.Log(constants.LogEncodingError, "Error encoding votedFor: %v", errEncodeVotedFor)
		return
	}
	errEncodeLog := e.Encode(rf.log)
	if errEncodeLog != nil {
		rf.logger.Log(constants.LogEncodingError, "Error encoding raft log: %v", errEncodeLog)
		return
	}

	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var decodedCurrentTerm int
	var decodedVotedFor int
	var decodedLog []LogEntry

	rf.mu.Lock()
	defer rf.mu.Unlock()

	errDecodeCurrentTerm := d.Decode(&decodedCurrentTerm)
	if errDecodeCurrentTerm != nil {
		rf.logger.Log(constants.LogEncodingError, "Error decoding currentTerm: %v", errDecodeCurrentTerm)
		return
	}
	errDecodeVotedFor := d.Decode(&decodedVotedFor)
	if errDecodeVotedFor != nil {
		rf.logger.Log(constants.LogEncodingError, "Error decoding votedFor: %v", errDecodeVotedFor)
		return
	}
	errDecodeLog := d.Decode(&decodedLog)
	if errDecodeLog != nil {
		rf.logger.Log(constants.LogEncodingError, "Error decoding raft log: %v", errDecodeLog)
		return
	}

	rf.currentTerm = decodedCurrentTerm
	rf.votedFor = decodedVotedFor
	rf.log = decodedLog
}

type RequestVoteArgs struct {
	// Your data here (4B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (init 0)
	LastLogTerm  int // term of candidate's last log entry (init 0)
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // Leader's Term
	LeaderId     int        // To allow followers to redirect clients.
	PrevLogIndex int        // Index of log entry immediately preceding new ones (init 0)
	PrevLogTerm  int        // Term of PrevLogIndex entry (init 0)
	Entries      []LogEntry // log entries to store (empty for heartbeat; send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex && PrevLogTerm

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

/*
Server side handler of call made by sendRequestVote.
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reply false if candidateTerm is less than receiver term
	isCandidateTermLessThanReceiverTerm := args.Term < rf.currentTerm
	if isCandidateTermLessThanReceiverTerm {
		return
	}

	// If term > currentTerm, update term and convert to follower
	if args.Term > rf.currentTerm {
		rf.logger.Log(constants.LogTerm, "Discovered higher term %d from candidate %d, converting to follower", args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.lastHeartbeatTime = time.Now() // Resetting timeout on higher term
		rf.persist()                      // Persist state changes immediately
	}

	// Is votedFor null or candidateId?
	isVotedForNullOrCandidateId := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	// Check if candidate's log is at least as up-to-date as our log
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	isLogUpToDate := false

	// If the candidate's last log term is greater, the candidate has a more up-to-date log
	if args.LastLogTerm > lastLogTerm {
		isLogUpToDate = true
	} else if args.LastLogTerm == lastLogTerm {
		// If the terms are the same, the one with the longer log is more up-to-date
		isLogUpToDate = args.LastLogIndex >= lastLogIndex
	}

	if isVotedForNullOrCandidateId && isLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		// Reset election timeout when granting vote
		rf.lastHeartbeatTime = time.Now()
		rf.logger.Log(constants.LogVote, "Granting vote to candidate %d for term %d", args.CandidateId, args.Term)
		rf.persist() // Persist state after granting vote
	}
}

/*
Server side handler of call made by sendAppendEntries.
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Reply false if candidateTerm is less than receiver term
	isCandidateTermLessThanReceiverTerm := args.Term < rf.currentTerm
	if isCandidateTermLessThanReceiverTerm {
		return
	}
	// If we get a valid AppendEntries, reset election timeout
	rf.lastHeartbeatTime = time.Now()

	// If term > currentTerm, update term and convert to follower
	if args.Term > rf.currentTerm {
		rf.logger.Log(constants.LogTerm, "Discovered higher term %d from leader %d, converting to follower", args.Term, args.LeaderId)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()                      // Persist state changes immediately
		rf.lastHeartbeatTime = time.Now() // Resetting timeout on higher term
	}

	// If we're a candidate but we see a current/higher term leader, convert to follower
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.logger.Log(constants.LogCandidate, "Discovered valid leader %d for term %d, converting to follower", args.LeaderId, args.Term)
		rf.state = Follower
		rf.persist() // Persist state after changing role
	}

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) {
		// Case 3: Our log is too short
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log)
		reply.Success = false
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Case 1 or 2: Term mismatch at prevLogIndex
		reply.XTerm = rf.log[args.PrevLogIndex].Term

		// Find the index of the first entry with XTerm
		reply.XIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				break
			}
			reply.XIndex = i
		}
		reply.XLen = len(rf.log)
		reply.Success = false
		return
	}

	// At this point logs match at prevLogIndex

	// Improved handling of new entries
	if len(args.Entries) > 0 {
		i := 0
		for ; i < len(args.Entries); i++ {
			logIndex := args.PrevLogIndex + 1 + i
			if logIndex >= len(rf.log) {
				break // Reached end of follower's log, will append remaining entries
			}
			if rf.log[logIndex].Term != args.Entries[i].Term {
				// Found conflict, truncate log from here
				rf.log = rf.log[:logIndex]
				break
			}
		}
		// Append any new entries (either all entries starting from conflict point or all entries from end of log)
		if i < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.logger.Log(constants.LogEntryAdded, "Appended Entries. Peer ID: %d, New Log: %+v", rf.me, rf.log)
			rf.persist() // Persist after modifying log
		}
	}

	// Update commit index if needed
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minFromTwo(args.LeaderCommit, len(rf.log)-1)
		rf.signalApplier() // Signal that there are new commits to apply
	}

	reply.Success = true
}

/*
Signal that there are commits to apply.
*/
func (rf *Raft) signalApplier() {
	select {
	case rf.applyTrigger <- struct{}{}:
	default:
		// Channel already has a signal pending, no need to send another
	}
}

/*
Apply committed entries to the state machine.
*/
func (rf *Raft) applier() {
	for !rf.killed() {
		select {
		case <-rf.applyTrigger:
			rf.applyCommits()
		case <-time.After(100 * time.Millisecond):
			// Periodically check for commits even without explicit signal
			rf.applyCommits()
		}
	}
}

/*
Apply committed entries while holding the lock.
*/
func (rf *Raft) applyCommits() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		rf.logger.Log(constants.LogEntryAdded, "Applying commits from %d to %d", rf.lastApplied+1, rf.commitIndex)

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if i < len(rf.log) && i > 0 { // Don't apply the dummy entry at index 0
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg // Release lock while sending on channel to avoid deadlock
				rf.mu.Lock()
				rf.lastApplied = i
			}
		}
	}
}

/*
Don't wish to bump Go version to 1.21 to access builtin min function that works
out of the box for int, so here's a custom implementation to avoid casting
to float64 and back.
*/
func minFromTwo(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call(RequestVote, args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call(AppendEntries, args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If not leader, return false
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// Append entry to log
	index := len(rf.log)
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.logger.Log(constants.LogEntryAdded, "Leader appended entry. Index: %d, Term: %d, Log: %+v",
		index, rf.currentTerm, rf.log)

	// Persist the state change immediately
	rf.persist()
	rf.logger.Log(constants.LogPersist, "Leader appended entry to log. Persisted.")

	// Start agreement on this new entry
	go rf.broadcastAppendEntries()

	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
Broadcast AppendEntries to all followers.
*/
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLogEntriesToPeer(i)
		}
	}
	rf.mu.Unlock()
}

/*
Send log entries to a specific peer.
*/
func (rf *Raft) sendLogEntriesToPeer(peer int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// Prepare arguments
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// Get entries to send
	entries := make([]LogEntry, 0)
	if rf.nextIndex[peer] < len(rf.log) {
		entries = append(entries, rf.log[rf.nextIndex[peer]:]...)
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// Check if we're still leader and the term hasn't changed
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// Update nextIndex and matchIndex for successful AppendEntries
			newMatchIndex := prevLogIndex + len(entries)
			rf.nextIndex[peer] = newMatchIndex + 1
			rf.matchIndex[peer] = newMatchIndex

			// Check if we can commit more entries
			rf.updateCommitIndex()
		} else {
			// If AppendEntries failed
			if reply.Term > rf.currentTerm {
				// We're out of date - become follower
				rf.logger.Log(constants.LogTerm, "Discovered higher term %d from peer %d, converting to follower", reply.Term, peer)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.lastHeartbeatTime = time.Now()
				rf.persist() // Immediately persist state changes
			} else {
				// Optimize log inconsistency resolution based on reply info
				if reply.XTerm == -1 {
					// Case 3: Follower's log is too short
					rf.nextIndex[peer] = reply.XLen
				} else {
					// Find the last entry in leader's log with XTerm
					lastIndexWithXTerm := -1
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.XTerm {
							lastIndexWithXTerm = i
							break
						}
					}

					if lastIndexWithXTerm == -1 {
						// Case 1: Leader doesn't have XTerm
						rf.nextIndex[peer] = reply.XIndex
					} else {
						// Case 2: Leader has XTerm
						rf.nextIndex[peer] = lastIndexWithXTerm + 1
					}
				}

				// Ensure nextIndex is at least 1
				if rf.nextIndex[peer] < 1 {
					rf.nextIndex[peer] = 1
				}

				rf.logger.Log(constants.LogEntryAdded, "Optimized nextIndex for peer %d to %d",
					peer, rf.nextIndex[peer])

				// Retry immediately
				go rf.sendLogEntriesToPeer(peer)
			}
		}
	}
}

/*
Update the commit index based on matchIndex values.
Improved to find the highest index that can be committed.
*/
func (rf *Raft) updateCommitIndex() {
	// Iterate backward to find the highest index that can be committed
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// Only commit entries from current term
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		// Count servers (including leader) with this entry
		count := 1 // Start with 1 for the leader
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		// If we have a majority, update commit index
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.logger.Log(constants.LogEntryAdded, "Leader committed entry at index %d", n)
			rf.signalApplier() // Signal that there are new commits to apply
			break              // Found highest index that can be committed
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Sleep a short amount to avoid CPU spinning
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		// Check if election timeout has elapsed
		// Increased randomization range for better stability
		ms := 400 + (rand.Int63() % 400)
		electionTimeout := time.Duration(ms) * time.Millisecond
		timeoutElapsed := time.Since(rf.lastHeartbeatTime) > electionTimeout

		if timeoutElapsed {
			if rf.state == Follower {
				// Convert to candidate
				rf.logger.Log(constants.LogTimer, "Election timeout elapsed, converting to candidate")
				rf.state = Candidate
				rf.currentTerm++                  // Increment term
				rf.votedFor = rf.me               // Vote for self
				rf.lastHeartbeatTime = time.Now() // Reset timeout
				rf.persist()                      // Persist state changes immediately

				// Send Requestvote RPCs to all peers
				go rf.startElection(rf.currentTerm)
			} else if rf.state == Candidate {
				// Start a new election round
				rf.logger.Log(constants.LogTimer, "Election timeout as candidate, starting a new election")
				rf.currentTerm++                  // Increment term
				rf.votedFor = rf.me               // Vote for self
				rf.lastHeartbeatTime = time.Now() // Reset timeout
				rf.persist()                      // Persist state changes immediately

				// Send Requestvote RPCs to all peers
				go rf.startElection(rf.currentTerm)
			}
			// Leaders don't timeout - they send heartbeats
		}

		rf.mu.Unlock()
	}
}

/*
Start an election by sending RequestVote RPCs to all peers.
*/
func (rf *Raft) startElection(term int) {
	// If already killed, don't start a new election
	if rf.killed() {
		return
	}

	rf.mu.Lock()

	// If our state changed during the function call, just return
	if rf.currentTerm != term || rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	// Prepare RequestVote args
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	// Count votes (including self-vote)
	votesReceived := 1
	var voteMu sync.Mutex

	// Send RequestVote RPCs to all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			// Skip if killed
			if rf.killed() {
				return
			}

			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// If our term is outdated, update and become follower
				if reply.Term > rf.currentTerm {
					rf.logger.Log(constants.LogTerm, "Discovered higher term %d, converting to follower", reply.Term)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist() // Immediately persist state changes
					rf.lastHeartbeatTime = time.Now()
					return
				}

				// Count the vote if granted and we're still a candidate for the same term
				if reply.VoteGranted && rf.state == Candidate && args.Term == rf.currentTerm {
					voteMu.Lock()
					votesReceived++
					majority := len(rf.peers)/2 + 1
					rf.logger.Log(constants.LogVote, "Received vote from %d, now have %d votes of %d needed",
						server, votesReceived, majority)

					if votesReceived >= majority {
						voteMu.Unlock()
						// Won the election, become leader
						rf.becomeLeader()
					} else {
						voteMu.Unlock()
					}
				}
			}
		}(i)
	}
}

/*
Transition to leader state.
*/
func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return // Safety check
	}

	rf.state = Leader
	rf.logger.Log(constants.LogLeader, "Becoming leader for term %d", rf.currentTerm)

	// Initialize nextIndex and matchIndex
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// Update leader's own matchIndex
	rf.matchIndex[rf.me] = len(rf.log) - 1

	// Persist state after becoming leader
	rf.persist()

	// Start the heartbeat process
	go rf.sendHeartbeats()
}

/*
Send Heartbeats periodically as a leader
*/
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		// Send heartbeats (AppendEntries with no entries)
		rf.broadcastAppendEntries()

		// Sleep for a while before sending next heartbeat
		// Increased heartbeat interval for better stability
		time.Sleep(100 * time.Millisecond)

		// Check if we're still the leader
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.logger = logger.NewLogger(me+1, ShouldLog, fmt.Sprintf("raft-%d", me), constants.RaftLoggingMap)
	rf.applyCh = applyCh
	rf.applyTrigger = make(chan struct{}, 1) // Buffer to prevent blocking

	// Initialize volatile states
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Initialize Leader volatile state (though not a leader yet)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Start as follower
	rf.state = Follower

	// Initialize election timeout timer
	rf.lastHeartbeatTime = time.Now()

	// Your initialization code here (4A, 4B, 4C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.logger.Log(constants.LogReadPersist, "Raft server startup. Read persisted state.")

	// Only initialize these if readPersist didn't set them
	if len(rf.log) == 0 {
		// Initialize Persistent State
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1) // Start with dummy entry at index 0
		rf.log[0] = LogEntry{nil, 0}
		rf.logger.Log(constants.LogEntryAdded, "Appended Dummy Node. Peer ID: %d, New Log: %+v", rf.me, rf.log)
	}

	rf.logger.Log(constants.LogRaftStart, "Raft server started")

	// Seeding random number generator to prevent issues related to servers using similar random values
	rand.Seed(time.Now().UnixNano() + int64(me))

	// Start ticker goroutine to start elections
	go rf.ticker()

	// Start commit applier goroutine
	go rf.applier()

	return rf
}
