package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Stopped = "stopped"
	Running = "running"
)

type Event struct {
	Target      interface{}
	returnValue interface{}
	c           chan error
}

type RaftKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv           *KV
	recorder     *Recorder
	stopped      chan bool
	c            chan *Event
	state        string
	routineGroup sync.WaitGroup
	persister    *raft.Persister
}

func (rs *RaftKV) setState(state string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.state = state
}

func (rs *RaftKV) Running() bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state != Stopped
}

func (rs *RaftKV) Get(args *GetArgs, reply *GetReply) {
	reply.WrongLeader = true
	rs.send(args, reply)
}

func (rs *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.WrongLeader = true
	rs.send(args, reply)
}

func (rs *RaftKV) State() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rs *RaftKV) Kill() {
	// Your code here, if desired.
	if rs.State() == Stopped {
		return
	}
	close(rs.stopped)
	rs.routineGroup.Wait()
	rs.setState(Stopped)
	rs.rf.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rs *RaftKV) send(value1 interface{}, value2 interface{}) error {
	if !rs.Running() {
		return raft.StopError
	}
	event := &Event{
		Target:      value1,
		returnValue: value2,
		c:           make(chan error, 1),
	}
	select {
	case rs.c <- event:
	case <-rs.stopped:
		return raft.StopError
	}
	select {
	case err := <-event.c:
		return err
	case <-rs.stopped:
		return raft.StopError
	case <-time.After(1000 * time.Millisecond):
		return raft.CommandTimeoutError
	}
}

func (rs *RaftKV) save() ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(rs.kv)
	encoder.Encode(rs.recorder)
	data := buffer.Bytes()
	return data, nil
}

func (rs *RaftKV) recovery(snapshot []byte) error {
	var lastIncludedIndex int
	var lastIncludedTerm int
	buffer := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&lastIncludedIndex)
	decoder.Decode(&lastIncludedTerm)
	kv := NewKV()
	recorder := NewRecorder()
	decoder.Decode(&kv)
	decoder.Decode(&recorder)
	rs.kv = kv
	rs.recorder = recorder
	return nil
}

func (rs *RaftKV) processGetArgs(a interface{}, r interface{}) {
	var reply *GetReply
	if a == nil {
		return
	}
	args := a.(*GetArgs)
	if r != nil {
		reply = r.(*GetReply)
		reply.WrongLeader = false
	} else {
		reply = nil
	}
	cid, key, seq := args.ClientID, args.Key, args.Sequence
	if reply != nil {
		reply.Value = rs.kv.Get(key)
		reply.Err = OK
	}
	rs.recorder.Record(cid, seq)
}

func (rs *RaftKV) processPutAppendArgs(a interface{}, r interface{}) {
	var reply *PutAppendReply
	if a == nil {
		return
	}
	args := a.(*PutAppendArgs)
	if r != nil {
		reply = r.(*PutAppendReply)
		reply.WrongLeader = false
	} else {
		reply = nil
	}
	key, value, op, cid, seq := args.Key, args.Value, args.Op, args.ClientID, args.Sequence
	if rs.recorder.Recorded(cid, seq) {
		if reply != nil {
			reply.Err = OK
		}
	} else {
		switch op {
		case "Put":
			rs.kv.Set(key, value)
		case "Append":
			v := rs.kv.Get(key)
			rs.kv.Set(key, v+value)
		}
		if reply != nil {
			reply.Err = OK
		}
		rs.recorder.Record(cid, seq)
	}
}

func (rs *RaftKV) processApplyMsg(msg *raft.ApplyMsg) {
	if msg.UseSnapshot {
		rs.recovery(msg.Snapshot)
	} else {
		event := msg.Command.(*Event)
		switch args := event.Target.(type) {
		case *GetArgs:
			rs.processGetArgs(args, event.returnValue)
		case *PutAppendArgs:
			rs.processPutAppendArgs(args, event.returnValue)
		}
		if event.c != nil {
			event.c <- nil
		}
		if rs.maxraftstate != 1 && rs.persister.RaftStateSize() > rs.maxraftstate && rs.rf != nil {
			data, _ := rs.save()
			rs.rf.TakeSnapshot(data, msg.Index, msg.Term)
		}
	}
}

func (rs *RaftKV) processEvent(event *Event) {
	_, _, isLeader := rs.rf.Start(event)
	if isLeader {
		return
	}
	switch event.Target.(type) {
	case *GetArgs:
		reply := event.returnValue.(*GetReply)
		reply.WrongLeader = true
	case *PutAppendArgs:
		reply := event.returnValue.(*PutAppendReply)
		reply.WrongLeader = true
	}
	event.c <- raft.NotLeaderError
}

func (rs *RaftKV) applyLoop() {
	for {
		select {
		case <-rs.stopped:
			rs.setState(Stopped)
			return
		case msg := <-rs.applyCh:
			rs.processApplyMsg(&msg)
		}
	}
}

func (rs *RaftKV) eventLoop() {
	for {
		select {
		case <-rs.stopped:
			rs.setState(Stopped)
			return
		case event := <-rs.c:
			rs.processEvent(event)
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(&Event{})
	gob.Register(&PutAppendArgs{})
	gob.Register(&GetArgs{})

	rs := &RaftKV{
		me:           me,
		maxraftstate: maxraftstate,
		kv:           NewKV(),
		recorder:     NewRecorder(),
		applyCh:      make(chan raft.ApplyMsg, 256),
		stopped:      make(chan bool),
		state:        Running,
		c:            make(chan *Event, 256),
		persister:    persister,
	}

	rs.routineGroup.Add(1)
	go func() {
		defer rs.routineGroup.Done()
		rs.eventLoop()
	}()
	rs.routineGroup.Add(1)
	go func() {
		defer rs.routineGroup.Done()
		rs.applyLoop()
	}()

	rs.rf = raft.Make(servers, me, persister, rs.applyCh)
	return rs
}
