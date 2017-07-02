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

import "sync"
import "labrpc"

import "fmt"
import "log"
import "sort"
import "time"
import "errors"

//import "io/ioutil"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

const (
	Stopped      = "stopped"
	Initialized  = "initialized"
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
	Snapshotting = "snapshotting"
)

const (
	MaxLogEntriesPerRequest         = 2000
	NumberOfLogEntriesAfterSnapshot = 200
)

const (
	DefaultHeartbeatInterval = 50 * time.Millisecond
	DefaultEletionTimeout    = 150 * time.Millisecond
)

const ElectionTimeoutThresholdPercent = 0.8

var NotLeaderError = errors.New("Raft.Server: Not current leader")
var DuplicatePeerError = errors.New("Raft.Server: Duplicate peer")
var CommandTimeoutError = errors.New("Raft: Command timeout")
var StopError = errors.New("Raft: Has been stopped")
var NetworkTimeout = errors.New("Raft: Network Timeout")
var NetworkFailure = errors.New("Raft: Network Failure")

type LogWriter struct {
}

func (writer LogWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " " + string(bytes))
}

type ApplyMsg struct {
	Index       int
	Term        int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Command interface {
}

type NOPCommand struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.RWMutex        // Lock to protect shared access to this peer's state
	connections []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh                 chan ApplyMsg
	state                   string
	currentTerm             int
	commitIndex             int
	votedFor                int
	log                     *Log
	leader                  int
	peers                   map[int]*Peer
	syncedPeer              map[int]bool
	stopped                 chan bool
	c                       chan *Event
	electionTimeout         time.Duration
	heartbeatInterval       time.Duration
	routineGroup            sync.WaitGroup
	maxLogEntriesPerRequest int64
}

type Event struct {
	target      interface{}
	returnValue interface{}
	notifyStart chan interface{}
	c           chan error
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	d := w.Bytes()
	rf.persister.SaveRaftState(d)
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
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) ID() int {
	return rf.me
}

func (rf *Raft) Leader() int {
	return rf.leader
}

func (rf *Raft) State() string {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state
}

func (rf *Raft) setState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = state
	if state == Leader {
		rf.leader = rf.me
		rf.syncedPeer = make(map[int]bool)
	}
}

func (rf *Raft) Term() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) CommitIndex() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.commitIndex
}

func (rf *Raft) VotedFor() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.votedFor
}

func (rf *Raft) promotable() bool {
	//return rf.log.CurrentIndex() > 0
	return true
}

func (rf *Raft) MemberCount() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return len(rf.peers) + 1
}

func (rf *Raft) QuorumSize() int {
	return rf.MemberCount()/2 + 1
}

func (rf *Raft) ElectionTimeout() time.Duration {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.electionTimeout
}

func (rf *Raft) SetElectionTimeout(duration time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout = duration
}

func (rf *Raft) HeartBeatInterval() time.Duration {
	rf.mu.RLock()
	rf.mu.RUnlock()
	return rf.heartbeatInterval
}

func (rf *Raft) SetHeartbeatInterval(duration time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeatInterval = duration
	for _, peer := range rf.peers {
		peer.setHeartbeatInterval(duration)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ret, _ := rf.send(args)
	r, _ := ret.(*InstallSnapshotReply)
	if r != nil {
		*reply = *r
	}
}

func (rf *Raft) TakeSnapshot(state []byte, index int, term int) {
	args := &TakeSnapshotArgs{
		UpperLevelState: state,
		Index:           index,
		Term:            term,
	}
	rf.send(args)
}

func (rf *Raft) restoreSnapshot() {
	snapshot := rf.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	buffer := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(buffer)

	var lastIncludedTerm int
	var lastIncludedIndex int
	decoder.Decode(&lastIncludedIndex)
	decoder.Decode(&lastIncludedTerm)
	rf.log.startIndex = lastIncludedIndex
	rf.log.startTerm = lastIncludedTerm
	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    snapshot,
	}
	go func() {
		rf.applyCh <- msg
	}()
}

func (rf *Raft) makeInstallSnapshotRequest() *InstallSnapshotArgs {
	var lastIncludedIndex int
	var lastIncludedTerm int
	snapshot := rf.persister.ReadSnapshot()
	buffer := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&lastIncludedIndex)
	decoder.Decode(&lastIncludedTerm)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshot,
	}
	return args
}

func (rf *Raft) processInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	if args.Term < rf.currentTerm {
		return NewInstallSnapshotReply(rf.currentTerm), false
	}
	if args.Term > rf.currentTerm {
		rf.updateCurrentTerm(args.Term, args.LeaderID)
	}
	rf.persister.SaveSnapshot(args.Data)
	rf.log.Compact(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persist()

	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
	}
	rf.applyCh <- msg
	return NewInstallSnapshotReply(rf.currentTerm), true
}

func (rf *Raft) processTakeSnapshot(args *TakeSnapshotArgs) {
	state := args.UpperLevelState
	index := args.Index
	term := args.Term
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(index)
	encoder.Encode(term)
	data := buffer.Bytes()
	data = append(data, state...)
	rf.persister.SaveSnapshot(data)
	_, _, err := rf.log.Compact(index, term)
	if err != nil {
		return
	}
	rf.persist()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	ret, _ := rf.send(args)
	r, _ := ret.(*RequestVoteReply)
	if r != nil {
		*reply = *r
	}
}

func (rf *Raft) processRequestVote(args *RequestVoteArgs) (*RequestVoteReply, bool) {
	if args.Term < rf.Term() {
		return NewRequestVoteReply(rf.currentTerm, false), false
	}
	if args.Term > rf.Term() {
		rf.updateCurrentTerm(args.Term, -1)
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return NewRequestVoteReply(rf.currentTerm, false), false
	}

	lastIndex, lastTerm := rf.log.LastInfo()
	if lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		return NewRequestVoteReply(rf.currentTerm, false), false
	}

	rf.votedFor = args.CandidateID
	return NewRequestVoteReply(rf.currentTerm, true), true
}

func (rf *Raft) processRequestVoteReply(reply *RequestVoteReply) bool {
	if reply.VoteGranted && reply.Term == rf.currentTerm {
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.updateCurrentTerm(reply.Term, -1)
	}
	return false
}

func (rf *Raft) AddPeer(id int) error {
	if rf.peers[id] != nil {
		return nil
	}
	if rf.me != id {
		peer := NewPeer(rf, id, rf.heartbeatInterval)
		rf.peers[id] = peer
	}
	return nil
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
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	e, err := rf.start(command)
	if err != nil {
		return index, term, false
	}
	entry := e.(*LogEntry)
	return entry.Index, entry.Term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	if rf.State() == Stopped {
		return
	}

	close(rf.stopped)
	rf.log.Close()

	rf.routineGroup.Wait()
	rf.setState(Stopped)
}

func (rf *Raft) Running() bool {
	rf.mu.RLock()
	rf.mu.RUnlock()
	return (rf.state != Stopped && rf.state != Initialized)
}

func (rf *Raft) updateCurrentTerm(term int, leader int) {
	if rf.state == Leader {
		for _, peer := range rf.peers {
			peer.stopHeartbeat(false)
		}
	}
	if rf.state != Follower {
		rf.setState(Follower)
	}

	rf.mu.Lock()
	rf.currentTerm = term
	rf.leader = leader
	rf.votedFor = -1
	rf.mu.Unlock()
}

func (rf *Raft) loop() {
	state := rf.State()
	for state != Stopped {
		switch state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
		state = rf.State()
	}
}

func (rf *Raft) start(value interface{}) (interface{}, error) {
	if !rf.Running() {
		return nil, StopError
	}
	event := &Event{target: value, notifyStart: make(chan interface{}, 1), c: make(chan error, 1)}
	select {
	case rf.c <- event:
	case <-rf.stopped:
		return nil, StopError
	}
	select {
	case <-rf.stopped:
		return nil, StopError
	case err := <-event.c:
		return nil, err
	case entry := <-event.notifyStart:
		return entry, nil
	}
}

func (rf *Raft) send(value interface{}) (interface{}, error) {
	if !rf.Running() {
		return nil, StopError
	}
	event := &Event{target: value, notifyStart: nil, c: make(chan error, 1)}
	select {
	case rf.c <- event:
	case <-rf.stopped:
		return nil, StopError
	}
	select {
	case <-rf.stopped:
		return nil, StopError
	case err := <-event.c:
		return event.returnValue, err
	}
}

func (rf *Raft) sendAsync(value interface{}) {
	if !rf.Running() {
		return
	}
	event := &Event{target: value, notifyStart: nil, c: make(chan error, 1)}
	select {
	case rf.c <- event:
		return
	default:
	}
	rf.routineGroup.Add(1)
	go func() {
		defer rf.routineGroup.Done()
		select {
		case rf.c <- event:
		case <-rf.stopped:
		}
	}()
}

func (rf *Raft) PersistStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) followerLoop() {
	timeoutChan := afterBetween(rf.ElectionTimeout(), rf.ElectionTimeout()*2)

	for rf.State() == Follower {
		var err error
		update := false
		select {
		case <-rf.stopped:
			rf.setState(Stopped)
			return
		case e := <-rf.c:
			switch args := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, update = rf.processAppendEntries(args)
			case *TakeSnapshotArgs:
				rf.processTakeSnapshot(args)
			case *InstallSnapshotArgs:
				e.returnValue, _ = rf.processInstallSnapshot(args)
			case *RequestVoteArgs:
				e.returnValue, _ = rf.processRequestVote(args)
			default:
				err = NotLeaderError
			}
			e.c <- err
		case <-timeoutChan:
			if rf.promotable() {
				rf.setState(Candidate)
			} else {
				update = true
			}
		}
		if update {
			timeoutChan = afterBetween(rf.ElectionTimeout(), rf.ElectionTimeout()*2)
		}
	}
}

func (rf *Raft) candidateLoop() {
	rf.leader = -1
	lastLogIndex, lastLogTerm := rf.log.LastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var replyChan chan *RequestVoteReply

	for rf.State() == Candidate {
		if doVote {
			rf.currentTerm++
			rf.votedFor = rf.ID()
			replyChan = make(chan *RequestVoteReply, len(rf.peers))
			for _, peer := range rf.peers {
				rf.routineGroup.Add(1)
				go func(peer *Peer) {
					defer rf.routineGroup.Done()
					peer.sendRequestVote(NewRequestVoteArgs(rf.currentTerm, rf.ID(), lastLogIndex, lastLogTerm), replyChan)
				}(peer)
			}
			votesGranted = 1
			timeoutChan = afterBetween(rf.ElectionTimeout(), rf.ElectionTimeout()*2)
			doVote = false
		}

		if votesGranted == rf.QuorumSize() {
			rf.setState(Leader)
			return
		}

		select {
		case <-rf.stopped:
			rf.setState(Stopped)
			return
		case reply := <-replyChan:
			if success := rf.processRequestVoteReply(reply); success {
				votesGranted++
			}
		case e := <-rf.c:
			var err error
			switch args := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = rf.processAppendEntries(args)
			case *TakeSnapshotArgs:
				rf.processTakeSnapshot(args)
			case *InstallSnapshotArgs:
				e.returnValue, _ = rf.processInstallSnapshot(args)
			case *RequestVoteArgs:
				e.returnValue, _ = rf.processRequestVote(args)
			case interface{}:
				err = NotLeaderError
			}
			e.c <- err
		case <-timeoutChan:
			doVote = true
		}
	}
}

func (rf *Raft) leaderLoop() {
	logIndex, _ := rf.log.LastInfo()
	for _, peer := range rf.peers {
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}

	for rf.State() == Leader {
		var err error
		select {
		case <-rf.stopped:
			for _, peer := range rf.peers {
				peer.stopHeartbeat(false)
			}
			rf.setState(Stopped)
			return
		case e := <-rf.c:
			switch args := e.target.(type) {
			case *AppendEntriesArgs:
				rf.processAppendEntries(args)
			case *AppendEntriesReply:
				rf.processAppendEntriesReply(args)
			case *RequestVoteArgs:
				e.returnValue, _ = rf.processRequestVote(args)
			case *TakeSnapshotArgs:
				rf.processTakeSnapshot(args)
			case *InstallSnapshotArgs:
				e.returnValue, _ = rf.processInstallSnapshot(args)
			case interface{}:
				rf.processCommand(args, e)
				continue
			}
			e.c <- err
		}
	}
	rf.syncedPeer = nil
}

func (rf *Raft) Do(command interface{}) (interface{}, error) {
	return rf.send(command)
}

func (rf *Raft) processCommand(command interface{}, e *Event) (*LogEntry, error) {
	//defer rf.persist()

	entry := rf.log.CreateEntry(rf.currentTerm, command, e)
	if err := rf.log.AppendEntry(entry); err != nil {
		if e != nil {
			e.c <- err
		}
		return entry, err
	}
	rf.persist()
	rf.syncedPeer[rf.me] = true
	if len(rf.peers) == 0 {
		commitIndex := rf.log.CurrentIndex()
		rf.log.SetCommitIndex(commitIndex)
	}
	if e.notifyStart != nil {
		e.notifyStart <- entry
	}
	for _, peer := range rf.peers {
		select {
		case peer.heartbeatChan <- true:
		default:
		}
	}
	return entry, nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//defer rf.persist()
	ret, _ := rf.send(args)
	r, _ := ret.(*AppendEntriesReply)
	if r != nil {
		*reply = *r
	}
}

func (rf *Raft) processAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	if args.Term < rf.currentTerm {
		return NewAppendEntriesReply(rf.currentTerm, false, rf.log.CurrentIndex(), rf.log.CommitIndex()), false
	}
	if args.Term == rf.currentTerm {
		if rf.state == Candidate {
			rf.setState(Follower)
		}
		rf.leader = args.LeaderID
	} else {
		rf.updateCurrentTerm(args.Term, args.LeaderID)
	}
	if err := rf.log.Truncate(args.PrevLogIndex, args.PrevLogTerm); err != nil {
		return NewAppendEntriesReply(rf.currentTerm, false, rf.log.CurrentIndex(), rf.log.CommitIndex()), true
	}
	if err := rf.log.AppendEntries(args.Entries); err != nil {
		return NewAppendEntriesReply(rf.currentTerm, false, rf.log.CurrentIndex(), rf.log.CommitIndex()), true
	}
	rf.persist()
	if err := rf.log.SetCommitIndex(args.CommitIndex); err != nil {
		return NewAppendEntriesReply(rf.currentTerm, false, rf.log.CurrentIndex(), rf.log.CommitIndex()), true
	}
	return NewAppendEntriesReply(rf.currentTerm, true, rf.log.CurrentIndex(), rf.log.CommitIndex()), true
}

func (rf *Raft) processAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > rf.Term() {
		rf.updateCurrentTerm(reply.Term, -1)
	}
	if !reply.Success {
		return
	}
	if reply.Append == true {
		rf.syncedPeer[reply.Peer] = true
	}

	var indices []int
	indices = append(indices, rf.log.CurrentIndex())
	for _, peer := range rf.peers {
		indices = append(indices, peer.getPrevLogIndex())
	}
	sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	commitIndex := indices[rf.QuorumSize()-1]
	committedIndex := rf.log.commitIndex
	if commitIndex > committedIndex {
		rf.log.SetCommitIndex(commitIndex)
	}
}

func (rf *Raft) Init() {
	log.SetFlags(log.Lshortfile)
	log.SetOutput(new(LogWriter))
	//log.SetOutput(ioutil.Discard)

	rf.log.apply = func(entry *LogEntry) (interface{}, error) {
		switch entry.Command.(type) {
		case NOPCommand:
			return entry, nil
		default:
			applyMsg := ApplyMsg{
				Term:        entry.Term,
				Index:       entry.Index,
				Command:     entry.Command,
				UseSnapshot: false,
			}
			rf.applyCh <- applyMsg
		}
		return entry, nil
	}
	rf.log.Open()
	rf.restoreSnapshot()
	rf.readPersist(rf.persister.ReadRaftState())
	_, rf.currentTerm = rf.log.LastInfo()

	rf.setState(Follower)

	for i := range rf.connections {
		rf.AddPeer(i)
	}

	rf.routineGroup.Add(1)
	go func() {
		defer rf.routineGroup.Done()
		rf.loop()
	}()

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
func Make(connections []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	gob.Register(NOPCommand{})

	rf := &Raft{
		connections:       connections,
		peers:             make(map[int]*Peer),
		persister:         persister,
		me:                me,
		c:                 make(chan *Event, 256),
		electionTimeout:   DefaultEletionTimeout,
		heartbeatInterval: DefaultHeartbeatInterval,
		log:               NewLog(),
		state:             Initialized,
		stopped:           make(chan bool),
		leader:            -1,
		applyCh:           applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	rf.Init()

	return rf
}
