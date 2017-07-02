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
	Config    shardmaster.Config
	Database  map[string]string
	Done      map[string]int64
	c         chan error
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
	id             string
	database       map[string]string
	done           map[string]int64
	alive          int32
	sequence       int64
	config         shardmaster.Config
	shardConfigNum [shardmaster.NShards]int
	stopCh         chan bool
	routineGroup   sync.WaitGroup
}

func (kv *ShardKV) detectDuplicate(clientID string, sequence int64) bool {
	seq, ok := kv.done[clientID]
	return ok && seq >= sequence
}

func (kv *ShardKV) appendLog(cmd Op) bool {
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return false
	}

	select {
	case <-cmd.c:
		return true
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: "Get",
		Key:       args.Key,
		c:         make(chan error, 1),
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
		c:         make(chan error, 1),
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
	// Your code here, if desired.
	if !kv.Alive() {
		return
	}
	close(kv.stopCh)
	kv.routineGroup.Wait()
	atomic.StoreInt32(&kv.alive, 0)
	kv.rf.Kill()
}

func (kv *ShardKV) setDead() {
	atomic.StoreInt32(&kv.alive, 0)
}

func (kv *ShardKV) Alive() bool {
	return atomic.LoadInt32(&kv.alive) == 1
}

func (kv *ShardKV) PushState(args *PushStateArgs, reply *PushStateReply) {
	op := Op{
		ClientID:  args.ClientID,
		Sequence:  args.Sequence,
		Operation: "PushState",
		c:         make(chan error, 1),
	}
	op.Database = make(map[string]string)
	for key, value := range args.Database {
		op.Database[key] = value
	}
	op.Done = make(map[string]int64)
	for clientID, sequence := range args.Done {
		op.Done[clientID] = sequence
	}
	if !kv.appendLog(op) {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
}

func (kv *ShardKV) makePushStateArgs(shard int, config *shardmaster.Config) *PushStateArgs {
	args := PushStateArgs{}
	args.ClientID = kv.id
	args.Sequence = kv.sequence
	kv.sequence++
	args.ConfigNum = config.Num
	args.Shard = shard
	kv.makeStateToPush(shard, &args)
	return &args
}

func (kv *ShardKV) makeStateToPush(shard int, args *PushStateArgs) {
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
}

func (kv *ShardKV) push(args *PushStateArgs, gid int, shard int, config *shardmaster.Config) bool {
	for {
		for _, server := range config.Groups[gid] {
			ck := kv.make_end(server)
			reply := PushStateReply{}
			ok := ck.Call("ShardKV.PushState", args, &reply)
			if ok && reply.WrongLeader {
				continue
			}
			if ok && reply.Err == OK {
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
	case "PushState":
		for key, value := range cmd.Database {
			kv.database[key] = value
		}
		for clientID, sequence := range cmd.Done {
			if kv.done[clientID] < sequence {
				kv.done[clientID] = sequence
			}
		}
	case "Reconfig":
		for shard, gid := range kv.config.Shards {
			if gid == 0 || gid == cmd.Config.Shards[shard] {
				continue
			}
			if kv.gid == gid && kv.gid != cmd.Config.Shards[shard] {
				args := kv.makePushStateArgs(shard, &cmd.Config)
				kv.push(args, cmd.Config.Shards[shard], shard, &cmd.Config)
			}
		}
		kv.config = cmd.Config
	}
	kv.done[cmd.ClientID] = cmd.Sequence
}

func (kv *ShardKV) applyLoop() {
	for {
		select {
		case <-kv.stopCh:
			kv.setDead()
			return
		case msg := <-kv.applyCh:
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
				if op.c != nil {
					op.c <- nil
				}
				if kv.maxraftstate != -1 && kv.rf != nil && kv.rf.PersistStateSize() > kv.maxraftstate {
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
	}
}

func (kv *ShardKV) configLoop() {
	ticker := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-kv.stopCh:
			kv.setDead()
			return
		case <-ticker:
			ck := shardmaster.MakeClerk(kv.masters)
			config := ck.Query(-1)
			kv.mu.Lock()
			curNum := kv.config.Num
			kv.mu.Unlock()
			for n := curNum + 1; n <= config.Num; n++ {
				c := ck.Query(n)
				cmd := Op{
					ClientID:  "Server.Reconfig",
					Sequence:  int64(n),
					Operation: "Reconfig",
					Config:    c,
					c:         make(chan error, 1),
				}
				if !kv.appendLog(cmd) {
					break
				}
			}
		}
	}
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
	//kv.maxraftstate = maxraftstate
	kv.maxraftstate = -1
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.done = make(map[string]int64)
	kv.database = make(map[string]string)
	kv.id, _ = newUUID()
	kv.sequence = 0
	atomic.StoreInt32(&kv.alive, 1)
	kv.stopCh = make(chan bool)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardConfigNum[i] = 0
	}

	kv.routineGroup.Add(1)
	go func() {
		defer kv.routineGroup.Done()
		kv.applyLoop()
	}()

	kv.routineGroup.Add(1)
	go func() {
		defer kv.routineGroup.Done()
		kv.configLoop()
	}()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
