package raft

import "fmt"
import "log"
import "sync"
import "time"
import "labrpc"

type Peer struct {
	server            *Raft
	ID                int
	prevLogIndex      int
	stopChan          chan bool
	heartbeatChan     chan bool
	heartbeatInterval time.Duration
	lastActivity      time.Time
	connection        *labrpc.ClientEnd
	sync.RWMutex
}

func NewPeer(server *Raft, id int, heartbeatInterval time.Duration) *Peer {
	return &Peer{
		server:            server,
		ID:                id,
		connection:        server.connections[id],
		heartbeatInterval: heartbeatInterval,
		heartbeatChan:     make(chan bool),
	}
}

func (p *Peer) setHeartbeatInterval(duration time.Duration) {
	p.Lock()
	defer p.Unlock()
	p.heartbeatInterval = duration
}

func (p *Peer) getPrevLogIndex() int {
	p.RLock()
	defer p.RUnlock()
	return p.prevLogIndex
}

func (p *Peer) setPrevLogIndex(value int) {
	p.Lock()
	defer p.Unlock()
	p.prevLogIndex = value
}

func (p *Peer) setLastActivity(now time.Time) {
	p.Lock()
	defer p.Unlock()
	p.lastActivity = now
}

func (p *Peer) startHeartbeat() {
	p.stopChan = make(chan bool)
	c := make(chan bool)

	p.setLastActivity(time.Now())

	p.server.routineGroup.Add(1)
	go func() {
		defer p.server.routineGroup.Done()
		p.heartbeat(c)
	}()
	<-c
}

func (p *Peer) stopHeartbeat(flush bool) {
	p.setLastActivity(time.Time{})
	p.stopChan <- flush
}

func (p *Peer) LastActivity() time.Time {
	p.RLock()
	defer p.RUnlock()
	return p.lastActivity
}

func (p *Peer) clone() *Peer {
	p.RLock()
	defer p.RUnlock()
	return &Peer{
		ID:           p.ID,
		prevLogIndex: p.prevLogIndex,
		lastActivity: p.lastActivity,
	}
}

func (p *Peer) heartbeat(c chan bool) {
	stopChan := p.stopChan

	c <- true

	//log.Printf("Server %d Peer %d heartbeat started ticker %v time %v\n", p.server.me, p.ID, p.heartbeatInterval, time.Now())
	p.flush()

	ticker := time.Tick(p.heartbeatInterval)

	for {
		select {
		case flush := <-stopChan:
			//log.Printf("Server %d Peer %d heartbeat stopped time %v\n", p.server.me, p.ID, time.Now())
			if flush {
				p.flush()
				return
			} else {
				return
			}
		case <-p.heartbeatChan:
			p.flush()
		case <-ticker:
			//log.Printf("Peer %d Send heartbeat\n", p.ID)
			p.flush()
			//log.Printf("Peer %d Sent heartbeat\n", p.ID)
		}
	}
}

func (p *Peer) flush() {
	prevLogIndex := p.getPrevLogIndex()
	term := p.server.currentTerm

	entries, prevLogTerm := p.server.log.GetEntriesAfter(prevLogIndex)
	log.Printf("Server %d Peer %d Flush Entries: %+v\n", p.server.me, p.ID, entries)
	if entries != nil {
		p.sendAppendEntriesRequest(NewAppendEntriesArgs(term, prevLogIndex, prevLogTerm, p.server.log.CommitIndex(), p.server.ID(), entries))
	} else {
		p.sendInstallSnapshotRequest(p.server.makeInstallSnapshotRequest())
	}
}

func (p *Peer) sendInstallSnapshotRequest(args *InstallSnapshotArgs) error {
	var reply InstallSnapshotReply
	ch := make(chan bool)
	fmt.Printf("Server %d Peer %d InstallSnapshot, Term: %d, LastIncludedIndex: %d, LastIncludedTerm: %d, Data: %d\n", p.server.me, p.ID, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
	go func(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
		ok := p.connection.Call("Raft.InstallSnapshot", args, reply)
		ch <- ok
	}(args, &reply)
	select {
	case ok := <-ch:
		if !ok {
			return NetworkFailure
		}
	case <-time.After(100 * time.Millisecond):
		return NetworkTimeout
	}
	p.Lock()
	fmt.Printf("Server %d Peer %d InstallSnapshot return, Term: %d, prevLogIndex: %d, args.LastIncludedIndex: %d\n", p.server.me, p.ID, reply.Term, p.prevLogIndex, args.LastIncludedIndex)
	p.prevLogIndex = args.LastIncludedIndex
	p.Unlock()
	return nil
}

func (p *Peer) sendAppendEntriesRequest(args *AppendEntriesArgs) error {
	var reply AppendEntriesReply
	ch := make(chan bool)
	//p.server.sendAppendEntries(p.ID, args, &reply)
	go func(rc chan bool, a *AppendEntriesArgs, r *AppendEntriesReply) {
		ok := p.connection.Call("Raft.AppendEntries", a, r)
		rc <- ok
	}(ch, args, &reply)
	select {
	case ok := <-ch:
		if !ok {
			return NetworkFailure
		}
	case <-time.After(100 * time.Millisecond):
		return NetworkTimeout
	}
	p.setLastActivity(time.Now())

	fmt.Printf("Server %d Peer %d prevLogIndex: %d, term: %d, AppendEntriesReply: %+v\n", p.server.me, p.ID, p.prevLogIndex, p.server.currentTerm, reply)
	p.Lock()
	if reply.Success {
		if len(args.Entries) > 0 {
			entry := args.Entries[len(args.Entries)-1]
			p.prevLogIndex = entry.Index
			if entry.Term == p.server.Term() {
				reply.Append = true
			}
		}
	} else {
		if reply.Term > p.server.Term() {
		} else if reply.Term == args.Term && reply.CommitIndex >= p.prevLogIndex {
			p.prevLogIndex = reply.CommitIndex
		} else if p.prevLogIndex > 0 {
			p.prevLogIndex--
			if p.prevLogIndex > reply.Index {
				p.prevLogIndex = reply.Index
			}
		}
	}
	p.Unlock()
	reply.Peer = p.ID
	p.server.sendAsync(&reply)
	return nil
}

func (p *Peer) sendRequestVote(args *RequestVoteArgs, c chan *RequestVoteReply) error {
	var reply RequestVoteReply
	ch := make(chan bool)
	args.peer = p
	log.Printf("Server %d Peer %d Send RequestVoteArgs: %+v\n", p.server.me, p.ID, args)
	for i := 0; i < 3; i++ {
		go func(rc chan bool, a *RequestVoteArgs, r *RequestVoteReply) {
			ok := p.connection.Call("Raft.RequestVote", a, r)
			log.Printf("Server %d Peer %d sendRequestVote, return %v, %+v\n", p.server.me, p.ID, ok, r)
			rc <- ok
		}(ch, args, &reply)
		select {
		case ok := <-ch:
			log.Printf("Server %d Peer %d ch received %v\n", p.server.me, p.ID, ok)
			if ok {
				break
			}
			if i == 2 {
				return NetworkFailure
			}
			continue
		case <-time.After(100 * time.Millisecond):
			log.Printf("Server %d Peer %d ch timedout %v\n", p.server.me, p.ID, args)
			if i == 2 {
				return NetworkTimeout
			}
		}
	}
	log.Printf("Server %d Peer %d return RequestVoteReply to ch: %+v\n", p.server.me, p.ID, reply)
	c <- &reply
	return nil
}
