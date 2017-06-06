package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "sync/atomic"
import "encoding/gob"
import "time"
import "bytes"

//import "fmt"

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database     map[string]string
	done         map[string]int64
	notifyCommit map[int]chan Op
	alive        int32
	config       shardmaster.Config
}

func (kv *ShardKV) detectDuplicate(clientID string, sequence int64) bool {
	seq, ok := kv.done[clientID]
	return ok && seq >= sequence
}

func (kv *ShardKV) appendLog(cmd Op) bool {
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
	case <-time.After(10000 * time.Millisecond):
		return false
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: "Get",
		Key:       args.Key,
	}
	if kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
	} else {
		ok := kv.appendLog(cmd)
		if !ok {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.database[cmd.Key]
			kv.done[cmd.ClientID] = cmd.Sequence
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	if kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
	} else {
		ok := kv.appendLog(cmd)
		if ok {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = "WrongLeader"
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.alive, 0)
}

func (kv *ShardKV) Alive() bool {
	return atomic.LoadInt32(&kv.alive) == 1
}

func (kv *ShardKV) apply(cmd Op) {
	switch cmd.Operation {
	case "Put":
		kv.database[cmd.Key] = cmd.Value
	case "Append":
		kv.database[cmd.Key] += cmd.Value
	}
	kv.done[cmd.ClientID] = cmd.Sequence
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.done = make(map[string]int64)
	kv.database = make(map[string]string)
	kv.notifyCommit = make(map[int]chan Op)
	atomic.StoreInt32(&kv.alive, 1)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for kv.Alive() {
			msg := <-kv.applyCh
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
				}
				if kv.maxraftstate != -1 && kv.rf.PersistStateSize() > kv.maxraftstate {
					buffer := new(bytes.Buffer)
					encoder := gob.NewEncoder(buffer)
					encoder.Encode(kv.database)
					encoder.Encode(kv.done)
					data := buffer.Bytes()
					go kv.rf.TakeSnapshot(data, msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	go func() {
		for kv.Alive() {
			ck := shardmaster.MakeClerk(masters)
			config := ck.Query(-1)
			kv.mu.Lock()
			kv.config = config
			kv.mu.Unlock()
		}
	}()

	return kv
}
