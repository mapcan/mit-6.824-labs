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

import "labrpc"
import "sync"
import "math/rand"
import "time"

import "fmt"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term           int
	votedFor       int
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	log            []Log
	role           State
	applyCh        chan ApplyMsg
	becomeLeaderCh chan int
	replicateCh    chan int
	heartbeatCh    chan int
	applyEntriesCh chan int
	granted        int
	killed         bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		return
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	go func() {
		rf.heartbeatCh <- 1
	}()

	reply.VoteGranted = false

	if args.Term < rf.term {
		reply.Term = rf.term
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.term

	lastLogIndex := len(rf.log) - 1
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((args.LastLogIndex == -1 && lastLogIndex == -1) || (lastLogIndex > -1 && (rf.log[lastLogIndex].Term < args.LastLogTerm || (rf.log[lastLogIndex].Term == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)))) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	//lastLogTerm := -1
	//if lastLogIndex >= 0 {
	//	lastLogTerm = rf.log[lastLogIndex].Term
	//}

	//if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	//	return
	//}
	//if lastLogTerm > args.LastLogTerm {
	//	return
	//}
	//if lastLogTerm == args.LastLogTerm && lastLogTerm > args.LastLogIndex {
	//	return
	//}
	//reply.VoteGranted = true

	//if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	//	if lastLogIndex < 0 && lastLogIndex == args.LastLogIndex {
	//		reply.VoteGranted = true
	//		return
	//	}
	//	if lastLogIndex >= 0 {
	//		if rf.log[lastLogIndex].Term < args.LastLogTerm {
	//			reply.VoteGranted = true
	//		} else if rf.log[lastLogIndex].Term == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {
	//			reply.VoteGranted = true
	//		}
	//	}
	//}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Index   int
	Success bool
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			nextIndex := rf.nextIndex[i]
			prevLogIndex := -1
			prevLogTerm := -1
			if nextIndex >= 0 {
				prevLogIndex = nextIndex - 1
			}
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}

			args := AppendEntriesArgs{
				Term:         rf.term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []Log{},
				LeaderCommit: rf.commitIndex,
			}

			i := i
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	go func() {
		rf.heartbeatCh <- 1
	}()

	reply.Term = rf.term
	reply.Index = 0
	reply.Success = false

	if args.Term < rf.term {
		DPrintf("Server %d term: %d reject AppendEntries: %v\n", rf.me, rf.term, args)
		return
	}
	if rf.role == Candidate && args.Term >= rf.term {
		rf.role = Follower
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
	}

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	logLen := len(rf.log)
	if logLen > 0 {
		logLen = rf.log[logLen-1].Index + 1
	}

	if prevLogIndex > logLen-1 {
		reply.Index = logLen
		return
	}

	base := 0
	if len(rf.log) > 0 {
		base = rf.log[0].Index
	}
	if prevLogIndex > base {
		term := rf.log[prevLogIndex-base].Term
		if term != prevLogTerm {
			for i := prevLogIndex - 1; i > base; i-- {
				if rf.log[i-base].Term != term {
					reply.Index = i + 1
					break
				}
			}
			return
		}
	}
	//if prevLogIndex > -1 && rf.log[prevLogIndex].Term != prevLogTerm {
	//	for i := prevLogIndex; i > 0; i-- {
	//		if rf.log[i].Term != rf.log[i-1].Term {
	//			reply.Index = i - 1
	//		}
	//	}
	//	return
	//}

	DPrintf("Server: %d, BeforeAppend commit: %d, args: %v, log: %v\n", rf.me, rf.commitIndex, args, rf.log)
	var i, j int
	for i, j = prevLogIndex-base+1, 0; i >= 0 && i < logLen && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i-base].Term != args.Entries[j].Term {
			break
		}
	}
	//var i, j int
	//for i, j = args.PrevLogIndex+1, 0; i < logLen && j < len(args.Entries); i, j = i+1, j+1 {
	//	if rf.log[i].Term != args.Entries[j].Term {
	//		break
	//	}
	//}
	if i < logLen && j < len(args.Entries) {
		rf.log = rf.log[:i-base]
	}
	//if i < logLen && j < len(args.Entries) {
	//	rf.log = rf.log[:i]
	//}
	if j < len(args.Entries) {
		for _, entry := range args.Entries[j:] {
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, prevLogIndex+len(args.Entries))
		rf.commitIndex = min(rf.commitIndex, logLen-1)
		go func() {
			rf.applyEntriesCh <- 1
		}()
	}

	DPrintf("Server: %d, AfterAppend commit: %d, args: %v, log: %v\n", rf.me, rf.commitIndex, args, rf.log)

	reply.Success = true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	term := rf.term
	for {
		if rf.killed || rf.term > term {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
		time.Sleep(time.Millisecond * 20)
		rf.mu.Lock()
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ok bool
	termUpdated := false
	for {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.role = Follower
			termUpdated = true
		}
		rf.mu.Unlock()
		if ok || termUpdated {
			break
		}
	}
	return ok
}

func (rf *Raft) makeAppendEntries(server int, args *AppendEntriesArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) == 0 {
		return false
	}
	base := rf.log[0].Index
	if base+len(rf.log)-1 < rf.nextIndex[server] {
		return false
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := -1
	if prevLogIndex > -1 {
		prevLogTerm = rf.log[prevLogIndex-base].Term
	}
	args.Term = rf.term
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = rf.log[prevLogIndex-base+1 : len(rf.log)]
	args.LeaderCommit = rf.commitIndex
	return true
}

func (rf *Raft) applyLogEntries() {
	for {
		if rf.killed {
			return
		}

		rf.mu.Lock()

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				Index:   rf.lastApplied + 1,
				Command: rf.log[rf.lastApplied].Command,
			}
			rf.applyCh <- msg
		}

		rf.mu.Unlock()

		select {
		case <-rf.applyEntriesCh:
		case <-time.After(time.Second):
		}
	}
}

func (rf *Raft) replicateLogEntriesToServer(server int, repCh chan int, term int) {
	terminate := false
	for {
		rf.mu.Lock()
		if rf.killed || rf.term != term || rf.role != Leader {
			terminate = true
		}
		rf.mu.Unlock()
		if terminate {
			return
		}
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		if rf.makeAppendEntries(server, &args) {
			rf.sendAppendEntries(server, &args, &reply)
			DPrintf("Leader %d sendAppendEntries to Server %d, args: %v\n", rf.me, server, args)
			if reply.Success {
				rf.mu.Lock()
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				//rf.nextIndex[server] = len(rf.log)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				DPrintf("Leader %d AppendEntries to Server %d success, before update commitIndex: %d, log: %v\n", rf.me, server, rf.commitIndex, rf.log)
				for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
					if rf.log[n].Term == rf.term {
						m := 0
						for _, i := range rf.matchIndex {
							if i >= n {
								m++
							}
						}
						if m >= len(rf.matchIndex)/2+1 {
							rf.commitIndex = n
							DPrintf("Leader %d replicated %d replicas, args: %v\n", rf.me, m, args)
							go func() {
								rf.applyEntriesCh <- 1
							}()
						}
					}
				}
				DPrintf("Leader %d AppendEntries to Server %d success, after update commitIndex: %d, log: %v\n", rf.me, server, rf.commitIndex, rf.log)
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				//if rf.nextIndex[server] >= 0 {
				//	rf.nextIndex[server]--
				//}
				DPrintf("Leader %d AppendEtries conflict, nextIndex[%d]: %d, try %d\n", rf.me, server, rf.nextIndex[server], reply.Index)
				rf.nextIndex[server] = reply.Index
				rf.mu.Unlock()
				continue
			}
		}
		select {
		case <-repCh:
		case <-time.After(time.Millisecond * 200):
		}
	}
}

func (rf *Raft) leaderWorker() {
	var repChs []chan int

	rf.mu.Lock()
	for i, _ := range rf.peers {
		if i != rf.me {
			ch := make(chan int)
			repChs = append(repChs, ch)
			go rf.replicateLogEntriesToServer(i, ch, rf.term)
		}
	}
	rf.mu.Unlock()

	for {
		if rf.killed {
			return
		}

		select {
		case <-rf.replicateCh:
			for i, repCh := range repChs {
				go func(ch chan int) {
					ch <- i
				}(repCh)
			}
		case <-time.After(time.Millisecond * 200):
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.persist()

	term = rf.term
	isLeader = rf.role == Leader
	if !isLeader {
		return index, term, isLeader
	}

	log := Log{
		Term:    rf.term,
		Index:   0,
		Command: command,
	}

	logLen := len(rf.log)
	if logLen > 0 {
		log.Index = rf.log[logLen-1].Index + 1
	}
	rf.log = append(rf.log, log)
	index = log.Index + 1
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

	go func() {
		rf.replicateCh <- 1
	}()

	DPrintf("Leader %d Start, commit: %d, logs: %v\n", rf.me, rf.commitIndex, rf.log)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.killed = true
	rf.mu.Unlock()
}

func (rf *Raft) vote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		return
	}

	rf.role = Candidate
	rf.term++
	rf.votedFor = rf.me
	rf.granted = 0

	for i, _ := range rf.peers {
		if i == rf.me {
			rf.granted++
			continue
		}

		lastLogIndex := len(rf.log) - 1
		lastLogTerm := -1
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
		}
		args := RequestVoteArgs{
			Term:         rf.term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		server := i

		go func() {
			var reply RequestVoteReply
			rf.sendRequestVote(server, &args, &reply)
			DPrintf("Server %d vote, term: %d, peer: %d, voteReply: %v\n", rf.me, rf.term, server, reply)
			if !reply.VoteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Candidate || rf.term > args.Term {
				return
			}

			rf.granted++
			if rf.granted == len(rf.peers)/2+1 {
				DPrintf("Server: %d, Granted: %d\n", rf.me, rf.granted)
				fmt.Printf("Server: %d, Granted: %d\n", rf.me, rf.granted)
				rf.role = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.matchIndex[i] = -1
					rf.nextIndex[i] = len(rf.log)
				}
				//rf.becomeLeaderCh <- 1
				go rf.heartbeat()
				go rf.leaderWorker()
				go func(term int) {
					ticker := time.NewTicker(110 * time.Millisecond)
					for range ticker.C {
						rf.mu.Lock()
						currentTerm := rf.term
						currentState := rf.role
						rf.mu.Unlock()
						if rf.killed || currentTerm > term || currentState != Leader {
							return
						}
						rf.heartbeat()
					}
				}(rf.term)
			}
		}()
	}
}

func (rf *Raft) CheckHeartbeat() {
	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	for {
		if rf.killed {
			return
		}
		electionTimeout := time.Millisecond * time.Duration(300+r.Intn(200))
		select {
		case <-rf.heartbeatCh:
			continue
		case <-time.After(electionTimeout):
			rf.vote()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.role = Follower
	rf.term = 0
	rf.votedFor = -1
	rf.heartbeatCh = make(chan int)
	rf.applyEntriesCh = make(chan int)
	rf.replicateCh = make(chan int)
	rf.granted = 0
	rf.killed = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Server %d Started term: %d, votedFor: %d, logs: %v\n", rf.me, rf.term, rf.votedFor, rf.log)
	go rf.CheckHeartbeat()
	go rf.applyLogEntries()

	return rf
}
