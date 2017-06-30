package raftkv

import (
	"bytes"
	"encoding/gob"
	//"fmt"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  string
	Sequence  int64
	Operation string
	Key       string
	Value     string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database     map[string]string
	done         map[string]int64
	notifyCommit map[int]chan Op
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: "Get",
		Key:       args.Key,
	}
	ok := kv.appendLog(cmd)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.database[cmd.Key]
		DPrintf("Server %d, Get Key: %s, Get Value: %s, database: %v", kv.me, cmd.Key, reply.Value, kv.database)
		kv.done[cmd.ClientID] = cmd.Sequence
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) detectDuplicate(clientID string, sequence int64) bool {
	seq, ok := kv.done[clientID]
	return ok && seq >= sequence
}

func (kv *RaftKV) appendLog(cmd Op) bool {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.notifyCommit[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.notifyCommit[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.notifyCommit, index)
		kv.mu.Unlock()
		return cmd == op
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	ok := kv.appendLog(cmd)
	if ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
func (kv *RaftKV) apply(cmd Op) {
	switch cmd.Operation {
	case "Put":
		kv.database[cmd.Key] = cmd.Value
	case "Append":
		//fmt.Printf("Append Key: %s, Value: %s\n", cmd.Key, cmd.Value)
		kv.database[cmd.Key] += cmd.Value
	}
	kv.done[cmd.ClientID] = cmd.Sequence
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.done = make(map[string]int64)
	kv.database = make(map[string]string)
	kv.notifyCommit = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			DPrintf("Server %d, ApplyMsg: %v", kv.me, msg)
			if msg.UseSnapshot {
				var lastIncludedIndex int
				var lastIncludedTerm int
				buffer := bytes.NewBuffer(msg.Snapshot)
				decoder := gob.NewDecoder(buffer)
				decoder.Decode(&lastIncludedIndex)
				decoder.Decode(&lastIncludedTerm)
				database := make(map[string]string)
				done := make(map[string]int64)
				decoder.Decode(&database)
				decoder.Decode(&done)
				kv.mu.Lock()
				kv.database = database
				kv.done = done
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op)
				kv.mu.Lock()
				if !kv.detectDuplicate(op.ClientID, op.Sequence) {
					kv.apply(op)
				}
				ch, ok := kv.notifyCommit[msg.Index]
				if ok {
					ch <- op
					DPrintf("Server %d, Send index %d to Channel %v", kv.me, msg.Index, ch)
				}
				if kv.maxraftstate != -1 && persister.RaftStateSize() > kv.maxraftstate && kv.rf != nil {
					buffer := new(bytes.Buffer)
					encoder := gob.NewEncoder(buffer)
					encoder.Encode(kv.database)
					encoder.Encode(kv.done)
					data := buffer.Bytes()
					go kv.rf.TakeSnapshot(data, msg.Index, msg.Term)
				}
				kv.mu.Unlock()
			}
		}
	}()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
