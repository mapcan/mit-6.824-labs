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

//import "fmt"

// import "bytes"
// import "encoding/gob"

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
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

	reply.VoteGranted = false
	reply.Term = rf.term

	if args.Term < rf.term {
		return
	}

	//fmt.Printf("Server %d term: %d received RequestVoteArgs: %v, log: %v\n", rf.me, rf.term, args, rf.log)
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	lastLogIndex := len(rf.log) - 1
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if lastLogIndex < 0 && lastLogIndex == args.LastLogIndex {
			reply.VoteGranted = true
		}
		if lastLogIndex >= 0 {
			if rf.log[lastLogIndex].Term <= args.LastLogTerm {
				reply.VoteGranted = true
			} else if rf.log[lastLogIndex].Term == args.Term && lastLogIndex <= args.LastLogIndex {
				reply.VoteGranted = true
			}
		}
	}
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

	reply.Term = rf.term
	reply.Success = false

	rf.heartbeatCh <- 1

	if args.Term < rf.term {
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
	}

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	logLen := len(rf.log)
	if prevLogIndex > logLen-1 || prevLogIndex > -1 && rf.log[prevLogIndex].Term != prevLogTerm {
		return
	}

	var i, j int
	for i, j = args.PrevLogIndex+1, 0; i < logLen && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}
	if i < logLen && j < len(args.Entries) {
		rf.log = rf.log[:i]
	}
	if j < len(args.Entries) {
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, prevLogIndex+len(args.Entries))
		go func() {
			rf.applyEntriesCh <- 1
		}()
	}

	//fmt.Printf("Server: %d, log: %v\n", rf.me, rf.log)

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//if len(args.Entries) > 0 {
	//fmt.Printf("Server %d sending entries to follower %d, %v\n", rf.me, server, args)
	//}

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
			//fmt.Printf("Leader %d change state to Follower, currentTerm: %d\n", rf.me, rf.term)
		}
		rf.mu.Unlock()
		if ok || termUpdated {
			if termUpdated {
				//fmt.Printf("Leader %d change state to Follower, currentTerm: %d, reply: %v\n", rf.me, rf.term, reply)
			}
			break
		}
	}

	//if len(args.Entries) > 0 {
	//fmt.Printf("Server %d sent entries to follower %d, %v, ok: %v, reply: %v\n", rf.me, server, args, ok, reply)
	//}
	return ok
}

func (rf *Raft) makeAppendEntries(server int, args *AppendEntriesArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("server: %d, peer: %d, nextIndex: %v\n", rf.me, server, rf.nextIndex)
	if len(rf.log)-1 < rf.nextIndex[server] {
		return false
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := -1
	if prevLogIndex > -1 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args.Term = rf.term
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	//fmt.Printf("Leader %d to Follower %d, prevLogIndex: %d, prevLogTerm: %d, log: %v\n", rf.me, server, prevLogIndex, prevLogTerm, rf.log)
	args.Entries = rf.log[rf.nextIndex[server]:len(rf.log)]
	args.LeaderCommit = rf.commitIndex
	return true
}

func (rf *Raft) applyLogEntries() {
	for {
		select {
		case <-rf.applyEntriesCh:
		case <-time.After(time.Second):
			rf.mu.Lock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				msg := ApplyMsg{
					Index:   rf.lastApplied + 1,
					Command: rf.log[rf.lastApplied].Command,
				}
				//fmt.Printf("Server: %d ApplyMsg: %v\n", rf.me, msg)
				rf.applyCh <- msg
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicateLogEntriesToServer(server int, repCh chan int, term int) {
	//fmt.Printf("server: %d role: %d, gorouting %d waiting on %v\n", rf.me, rf.role, server, repCh)
	terminate := false
	select {
	case <-repCh:
	}
	for {
		//fmt.Printf("server: %d, role: %d, AppendEntries To Server: %d\n", rf.me, rf.role, server)
		rf.mu.Lock()
		if rf.term != term || rf.role != Leader {
			terminate = true
		}
		rf.mu.Unlock()
		if terminate {
			return
		}
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		if !rf.makeAppendEntries(server, &args) {
			continue
		}
		rf.sendAppendEntries(server, &args, &reply)
		if reply.Success {
			rf.mu.Lock()
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			//fmt.Printf("server: %d, role: %d, rf.nextIndex[%d]: %d, rf.matchIndex[%d]: %d\n", rf.me, rf.role, server, rf.nextIndex[server], server, rf.matchIndex[server])
			for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
				if rf.log[n].Term == rf.term {
					m := 0
					for _, i := range rf.matchIndex {
						if i >= n {
							m++
						}
					}
					if m == len(rf.matchIndex)/2+1 {
						//fmt.Printf("m = %d, majority: %d\n", m, len(rf.matchIndex)/2)
						rf.commitIndex = n
						go func() {
							rf.applyEntriesCh <- 1
						}()
					}
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			if rf.nextIndex[server] >= 0 {
				rf.nextIndex[server]--
			}
			rf.mu.Unlock()
			continue
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
	//fmt.Printf("Server: %d, initial term: %d, role: %v\n", rf.me, term, rf.role)
	rf.mu.Unlock()

	for {
		select {
		case <-rf.replicateCh:
		case <-time.After(time.Millisecond * 200):
			for i, repCh := range repChs {
				go func(ch chan int) {
					ch <- i
				}(repCh)
			}
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

	term = rf.term
	isLeader = rf.role == Leader
	if !isLeader {
		return index, term, isLeader
	}

	log := Log{
		Term:    rf.term,
		Command: command,
	}
	rf.log = append(rf.log, log)
	index = len(rf.log)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

	go func() {
		rf.replicateCh <- 1
	}()

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
	//fmt.Printf("Server %d vote, term: %d\n", rf.me, rf.term)

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
			//fmt.Printf("Server %d vote, term: %d, peer: %d, voteReply: %v\n", rf.me, rf.term, server, reply)
			if !reply.VoteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.granted++
			if rf.granted == len(rf.peers)/2+1 {
				//fmt.Printf("Server: %d, Granted: %d\n", rf.me, rf.granted)
				rf.role = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.matchIndex[i] = -1
					rf.nextIndex[i] = len(rf.log)
				}
				//rf.becomeLeaderCh <- 1
				//fmt.Printf("Server %d become Leader\n", rf.me)
				go rf.heartbeat()
				go rf.leaderWorker()
				go func(term int) {
					ticker := time.NewTicker(110 * time.Millisecond)
					for range ticker.C {
						rf.mu.Lock()
						currentTerm := rf.term
						currentState := rf.role
						rf.mu.Unlock()
						if currentTerm != term || currentState != Leader {
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.CheckHeartbeat()
	go rf.applyLogEntries()

	//fmt.Printf("%d peers\n", len(rf.peers))

	return rf
}
