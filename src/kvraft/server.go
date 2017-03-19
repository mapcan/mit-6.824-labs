package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

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
	detectDup    map[string]int64
	notifyCommit map[int]chan int
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: "Get",
		Key:       args.Key,
	}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		return
	}
	DPrintf("Server %d, Get Key: %s", kv.me, args.Key)
	kv.mu.Lock()
	ch, ok := kv.notifyCommit[index]
	if !ok {
		ch = make(chan int, 1)
		kv.notifyCommit[index] = ch
	}
	kv.mu.Unlock()
	select {
	case commitIndex := <-ch:
		if commitIndex == index {
			kv.mu.Lock()
			reply.Value = kv.database[cmd.Key]
			DPrintf("Server %d, Get Key: %s, Get Value: %s, database: %v", kv.me, cmd.Key, reply.Value, kv.database)
			delete(kv.notifyCommit, index)
			kv.mu.Unlock()
		} else {
			reply.Err = "Commit Out of Order"
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = "Operation Timedout"
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
	//DPrintf("PutAppend Args: %v", args)
	index, _, isLeader := kv.rf.Start(cmd)
	//DPrintf("index: %d, isLeader: %v", index, isLeader)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		return
	}
	DPrintf("Server %d, PutAppend Key: %s, Value: %s, Index: %d, Args: %v", kv.me, args.Key, args.Value, index, args)
	kv.mu.Lock()
	ch, ok := kv.notifyCommit[index]
	if !ok {
		ch = make(chan int, 1)
		kv.notifyCommit[index] = ch
	}
	kv.mu.Unlock()
	select {
	case commitIndex := <-ch:
		DPrintf("Receive index %d from Channel %v", commitIndex, ch)
		if commitIndex == index {
			reply.Err = OK
			kv.mu.Lock()
			v, ok := kv.detectDup[cmd.ClientID]
			if !ok || v < cmd.Sequence {
				switch {
				case cmd.Operation == "Append":
					kv.database[cmd.Key] += cmd.Value
					DPrintf("Server %d, Append Key: %s, Value: %s, database: %v", kv.me, cmd.Key, cmd.Value, kv.database)
				case cmd.Operation == "Put":
					kv.database[cmd.Key] = cmd.Value
					DPrintf("Server %d, Put Key: %s, Value: %s, database: %v", kv.me, cmd.Key, cmd.Value, kv.database)
				default:
					reply.Err = "Invalid Operation"
				}
				kv.detectDup[cmd.ClientID] = cmd.Sequence
			} else {
				DPrintf("Detected Duplicate, ok: %v, v: %d, cmd.Sequence: %d", ok, v, cmd.Sequence)
			}
			delete(kv.notifyCommit, index)
			kv.mu.Unlock()
		} else {
			reply.Err = "Commit Out of Order"
		}
		//case <-time.After(2000 * time.Millisecond):
		//	DPrintf("Server: %d, Operation Timedout, PutAppend Key: %s, Value: %s, Index: %d, Args: %v", kv.me, args.Key, args.Value, index, args)
		//	reply.Err = "Operation Timedout"
	}
	//DPrintf("PutAppend reply: %v", reply)
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.detectDup = make(map[string]int64)
	kv.database = make(map[string]string)
	kv.notifyCommit = make(map[int]chan int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			DPrintf("Server %d, ApplyMsg: %v", kv.me, msg)
			index := msg.Index
			kv.mu.Lock()
			ch, ok := kv.notifyCommit[index]
			if !ok {
				ch = make(chan int, 1)
				kv.notifyCommit[index] = ch
			}
			kv.mu.Unlock()
			DPrintf("Server %d, Send index %d to Channel %v", kv.me, index, ch)
			ch <- index
		}
	}()

	return kv
}
