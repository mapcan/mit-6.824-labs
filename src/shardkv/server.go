package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "sync/atomic"
import "encoding/gob"
import "time"
import "bytes"

import "fmt"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  string
	Sequence  int64
	Operation string
	Key       string
	Value     string
	Config    shardmaster.Config
}

func (op *Op) Equals(o *Op) bool {
	if op.Operation == o.Operation {
		if op.Operation == "Reconfig" {
			return op.Sequence == o.Sequence
		}
		return op.ClientID == o.ClientID && op.Sequence == op.Sequence
	}
	return false
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
	pulled       [shardmaster.NShards]int
	pushed       [shardmaster.NShards]int
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

	//fmt.Printf("Server %d GID %d Num %d, Try Acquire AppendLog Mutex\n", kv.me, kv.gid, kv.config.Num)
	kv.mu.Lock()
	ch, ok := kv.notifyCommit[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.notifyCommit[index] = ch
	}
	kv.mu.Unlock()
	//fmt.Printf("Server %d GID %d Num %d, Release AppendLog Mutex\n", kv.me, kv.gid, kv.config.Num)

	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.notifyCommit, index)
		kv.mu.Unlock()
		return cmd.Equals(&op)
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

	kv.mu.Lock()
	if kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	ok := kv.appendLog(cmd)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.database[cmd.Key]
		kv.done[cmd.ClientID] = cmd.Sequence
		fmt.Printf("Server %d GID %d Num %d GET Key: %s, Value: %s\n", kv.me, kv.gid, kv.config.Num, cmd.Key, reply.Value)
		kv.mu.Unlock()
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

	kv.mu.Lock()
	if kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	ok := kv.appendLog(cmd)
	if ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
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

func (kv *ShardKV) PushState(args *PushStateArgs, reply *PushStateReply) {
	op := Op{
		ClientID:  "Local.PushState",
		Sequence:  int64(args.ConfigNum),
		Operation: "Push",
	}
	if !kv.appendLog(op) {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	//if args.ConfigNum < kv.config.Num {
	//	fmt.Printf("Server %d GID %d Num %d, WrongConfigNum, args.ConfigNum: %d, kv.config.Num: %d\n", kv.me, kv.gid, kv.config.Num, args.ConfigNum, kv.config.Num)
	//	//reply.Err = ErrWrongConfigNum
	//	reply.Err = OK
	//	return
	//}

	for key, value := range args.Database {
		kv.database[key] = value
	}
	for clientID, sequence := range args.Done {
		if kv.done[clientID] < sequence {
			kv.done[clientID] = sequence
		}
	}
	reply.Err = OK
}

func (kv *ShardKV) PullState(args *PullStateArgs, reply *PullStateReply) {
	op := Op{
		ClientID:  "Local.PullState",
		Sequence:  int64(args.ConfigNum),
		Operation: "Pull",
	}
	//fmt.Printf("Server %d GID %d Num %d, Begin AppendLog %d\n", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
	if !kv.appendLog(op) {
		reply.WrongLeader = true
		//fmt.Printf("Server %d GID %d Num %d, End AppendLog %d WrongLeader\n", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
		return
	}
	//fmt.Printf("Server %d GID %d Num %d, End AppendLog %d\n", kv.me, kv.gid, kv.config.Num, args.ConfigNum)

	//fmt.Printf("Server %d GID %d Num %d, Begin PullState %d\n", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//if kv.config.Num < args.ConfigNum {
	//	//reply.Err = ErrWrongConfigNum
	//	reply.Err = OK
	//	return
	//}

	reply.Database = make(map[string]string)
	for key, value := range kv.database {
		if key2shard(key) == args.Shard {
			reply.Database[key] = value
			fmt.Printf("Server %d GID %d Num %d, Migrate To Shard: %d, Num: %d, Key: %s, Value: %s, Key %s belongs to Shard %d\n", kv.me, kv.gid, kv.config.Num, args.Shard, args.ConfigNum, key, value, key, args.Shard)
		}
	}

	reply.Done = make(map[string]int64)
	for clientID, sequence := range kv.done {
		reply.Done[clientID] = sequence
	}
	reply.Err = OK
	//fmt.Printf("Server %d GID %d Num %d, End PullState %d\n", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
}

func (kv *ShardKV) push(gid int, shard int, config *shardmaster.Config) bool {
	if kv.pushed[shard] >= config.Num {
		return true
	}

	args := PushStateArgs{
		ConfigNum: config.Num,
		Shard:     shard,
	}
	args.Database = make(map[string]string)
	args.Done = make(map[string]int64)

	for key, value := range kv.database {
		if key2shard(key) == shard {
			args.Database[key] = value
		}
	}
	for clientID, sequence := range kv.done {
		args.Done[clientID] = sequence
	}

	for {
		for _, server := range config.Groups[gid] {
			ck := kv.make_end(server)
			reply := PushStateReply{}
			fmt.Printf("Server %d GID %d Num %d, Start migrate to Server %v, Shard: %d, Database: %v\n", kv.me, kv.gid, config.Num, server, shard, args.Database)
			ok := ck.Call("ShardKV.PushState", &args, &reply)
			fmt.Printf("Server %d GID %d Num %d, Finish migrate to Server %v, Shard: %d, Reply: %v\n", kv.me, kv.gid, config.Num, server, shard, reply)
			if ok && reply.WrongLeader {
				continue
			}
			if ok && reply.Err == OK {
				kv.pushed[shard] = config.Num
				return true
			}
		}
	}
}

func (kv *ShardKV) pull(gid int, shard int) bool {
	if kv.pulled[shard] >= kv.config.Num {
		return true
	}
	for {
		for _, server := range kv.config.Groups[gid] {
			ck := kv.make_end(server)
			args := PullStateArgs{
				ConfigNum: kv.config.Num,
				Shard:     shard,
			}
			reply := PullStateReply{}
			fmt.Printf("Server %d GID %d Num %d, Start migrate from Server %v, Shard: %d\n", kv.me, kv.gid, kv.config.Num, server, shard)
			ok := ck.Call("ShardKV.PullState", &args, &reply)
			fmt.Printf("Server %d GID %d Num %d, Finish migrate from Server %v, Shard: %d, Reply: %v\n", kv.me, kv.gid, kv.config.Num, server, shard, reply.Database)
			fmt.Printf("Server %d GID %d Num %d, PullState Reply: %v\n", kv.me, kv.gid, kv.config.Num, reply)
			if ok && reply.WrongLeader {
				continue
			}
			if ok && reply.Err == OK {
				for key, value := range reply.Database {
					fmt.Printf("Server %d GID %d Num %d, Overwrite database, Key: %s, Old Value: %s, New Value: %s\n", kv.me, kv.gid, kv.config.Num, key, kv.database[key], value)
					kv.database[key] = value
				}
				for clientID, sequence := range reply.Done {
					if sequence > kv.done[clientID] {
						kv.done[clientID] = sequence
					}
				}
				kv.pulled[shard] = kv.config.Num
				return true
			}
		}
	}
}

func (kv *ShardKV) apply(cmd Op) {
	switch cmd.Operation {
	case "Put":
		kv.database[cmd.Key] = cmd.Value
	case "Append":
		kv.database[cmd.Key] += cmd.Value
		fmt.Printf("Server %d GID %d Num %d applied Append, Key: %s, Value: %s\n", kv.me, kv.gid, kv.config.Num, cmd.Key, kv.database[cmd.Key])
	case "Reconfig":
		fmt.Printf("Server %d, GID: %d, Old Config: %v, New Config: %v\n", kv.me, kv.gid, kv.config.Shards, cmd.Config.Shards)
		for shard, gid := range kv.config.Shards {
			if gid == 0 || gid == cmd.Config.Shards[shard] {
				continue
			}
			if kv.gid != gid && kv.gid == cmd.Config.Shards[shard] {
				if !kv.pull(gid, shard) {
					return
				}
			}
			//if kv.gid == gid && kv.gid != cmd.Config.Shards[shard] {
			//	if !kv.push(cmd.Config.Shards[shard], shard, &cmd.Config) {
			//		return
			//	}
			//}
		}
		kv.config = cmd.Config
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
	for i := 0; i < shardmaster.NShards; i++ {
		kv.pulled[i] = 0
		kv.pushed[i] = 0
	}

	fmt.Printf("Server %d GID %d started\n", kv.me, kv.gid)

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
			curNum := kv.config.Num
			kv.mu.Unlock()
			for n := curNum + 1; n <= config.Num; n++ {
				c := ck.Query(n)
				cmd := Op{
					ClientID:  "Local.Reconf",
					Sequence:  int64(n),
					Operation: "Reconfig",
					Config:    c,
				}
				kv.appendLog(cmd)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return kv
}
