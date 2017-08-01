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

type Event struct {
	Target      interface{}
	returnValue interface{}
	c           chan error
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	id           string
	sequence     int64
	shardManager *ShardManager
	alive        int32
	config       shardmaster.Config
	stopCh       chan bool
	routineGroup sync.WaitGroup
	persister    *raft.Persister
	eventCh      chan *Event
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.WrongLeader = true
	kv.send(args, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.WrongLeader = true
	kv.send(args, reply)
}

func (kv *ShardKV) save() ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(kv.shardManager)
	return buffer.Bytes(), nil
}

func (kv *ShardKV) recovery(snapshot []byte) error {
	var lastIncludedIndex int
	var lastIncludedTerm int
	buffer := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&lastIncludedIndex)
	decoder.Decode(&lastIncludedTerm)
	shardManager := NewShardManager()
	decoder.Decode(&shardManager)
	kv.shardManager = shardManager
	return nil
}

func (kv *ShardKV) processGetArgs(a interface{}, r interface{}) {
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
	sn := key2shard(key)
	shard := kv.shardManager.GetShard(sn)
	if shard != nil && reply != nil {
		reply.Value = shard.Get(key)
		reply.Err = OK
		shard.recorder.Record(cid, seq)
	}
}

func (kv *ShardKV) processPutAppendArgs(a interface{}, r interface{}) {
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
	sn := key2shard(key)
	if kv.gid != kv.config.Shards[sn] {
		reply.Err = ErrWrongGroup
		return
	}
	shard := kv.shardManager.GetShard(sn)
	if shard == nil {
		newShard := NewShard(sn)
		newClock := NewShardLogicalClock(kv.config.Num, kv.rf.Term())
		kv.shardManager.AddShard(sn, newClock, newShard)
		shard = newShard
	}
	if reply != nil {
		switch op {
		case "Put":
			shard.Set(key, value)
		case "Append":
			v := shard.Get(key)
			shard.Set(key, v+value)
		}
		newClock := NewShardLogicalClock(kv.config.Num, kv.rf.Term())
		kv.shardManager.SetClock(sn, newClock)
		reply.Err = OK
	}
	shard.recorder.Record(cid, seq)
}

func (kv *ShardKV) updateConfig() {
	ck := shardmaster.MakeClerk(kv.masters)
	config := ck.Query(-1)
	curNum := kv.config.Num
	for n := curNum + 1; n <= config.Num; n++ {
		c := ck.Query(n)
		args := &ConfigChangeArgs{
			Config: c,
		}
		event := &Event{
			Target:      args,
			returnValue: nil,
			c:           nil,
		}
		kv.eventCh <- event
	}
}

func (kv *ShardKV) applyLoop() {
	ticker := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-kv.stopCh:
			kv.setDead()
			return
		case <-ticker:
			kv.updateConfig()
		case msg := <-kv.applyCh:
			kv.processApplyMsg(&msg)
		}
	}
}

func (kv *ShardKV) processConfigChangeArgs(a interface{}) {
	args := a.(*ConfigChangeArgs)
	config := args.Config
	for shard, gid := range kv.config.Shards {
		if gid == 0 || gid == args.Config.Shards[shard] {
			continue
		}
		if kv.gid == gid && kv.gid != args.Config.Shards[shard] {
			args := kv.makePushStateArgs(shard, &args.Config)
			kv.push(args, config.Shards[shard], shard, &config)
		}
	}
	kv.config = args.Config
}

func (kv *ShardKV) processApplyMsg(msg *raft.ApplyMsg) {
	if msg.UseSnapshot {
		kv.recovery(msg.Snapshot)
	} else {
		event := msg.Command.(*Event)
		switch args := event.Target.(type) {
		case *GetArgs:
			kv.processGetArgs(args, event.returnValue)
		case *PutAppendArgs:
			kv.processPutAppendArgs(args, event.returnValue)
		case *ConfigChangeArgs:
			kv.processConfigChangeArgs(args)
		case *PushStateArgs:
			kv.shardManager.AddShard(args.Sn, args.Clock, args.Shard)
		}
		if event.c != nil {
			event.c <- nil
		}
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.rf != nil {
			data, _ := kv.save()
			kv.rf.TakeSnapshot(data, msg.Index, msg.Term)
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
	reply.WrongLeader = true
	kv.send(args, reply)
}

func (kv *ShardKV) makePushStateArgs(shard int, config *shardmaster.Config) *PushStateArgs {
	args := &PushStateArgs{
		Sn:       shard,
		Clock:    NewShardLogicalClock(kv.config.Num, kv.rf.Term()),
		Shard:    kv.shardManager.GetShard(shard),
		ClientID: kv.id,
		Sequence: kv.sequence,
	}
	kv.sequence++
	return args
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
			if ok && reply.Clock.LaterThan(args.Clock) {
				return false
			}
		}
	}
}

func (kv *ShardKV) send(value1 interface{}, value2 interface{}) error {
	if !kv.Alive() {
		return raft.StopError
	}
	event := &Event{
		Target:      value1,
		returnValue: value2,
		c:           make(chan error, 1),
	}
	select {
	case kv.eventCh <- event:
	case <-kv.stopCh:
		return raft.StopError
	}
	select {
	case err := <-event.c:
		return err
	case <-kv.stopCh:
		return raft.StopError
	case <-time.After(1000 * time.Millisecond):
		return raft.CommandTimeoutError
	}
}

func (kv *ShardKV) eventLoop() {
	for {
		select {
		case <-kv.stopCh:
			kv.setDead()
			return
		case event := <-kv.eventCh:
			switch args := event.Target.(type) {
			case *PushStateArgs:
				if !kv.shardManager.CanUpdateShard(args.Sn, args.Clock) {
					reply := event.returnValue.(*PushStateReply)
					reply.WrongLeader = false
					reply.Err = ErrOldShard
					reply.Sn = args.Sn
					reply.Clock = kv.shardManager.GetClock(args.Sn)
					event.c <- nil
					continue
				}
			case *GetArgs:
				sn := key2shard(args.Key)
				if kv.gid != kv.config.Shards[sn] {
					reply := event.returnValue.(*GetReply)
					reply.WrongLeader = false
					reply.Err = ErrWrongGroup
					event.c <- nil
					continue
				}
			case *PutAppendArgs:
				sn := key2shard(args.Key)
				if kv.gid != kv.config.Shards[sn] {
					reply := event.returnValue.(*PutAppendReply)
					reply.WrongLeader = false
					reply.Err = ErrWrongGroup
					event.c <- nil
					continue
				}
			}
			_, _, isLeader := kv.rf.Start(event)
			if isLeader {
				continue
			}
			if event.c != nil {
				event.c <- raft.NotLeaderError
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
	gob.Register(&Shard{})
	gob.Register(&Event{})
	gob.Register(&GetArgs{})
	gob.Register(&PutAppendArgs{})
	gob.Register(&PushStateArgs{})

	// Your initialization code here.

	shardkv := &ShardKV{
		me:           me,
		maxraftstate: maxraftstate,
		make_end:     make_end,
		masters:      masters,
		gid:          gid,
		id:           NewUUID(),
		sequence:     0,
		alive:        1,
		shardManager: NewShardManager(),
		stopCh:       make(chan bool),
		eventCh:      make(chan *Event, 256),
		applyCh:      make(chan raft.ApplyMsg, 256),
		persister:    persister,
	}

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	shardkv.routineGroup.Add(1)
	go func() {
		defer shardkv.routineGroup.Done()
		shardkv.applyLoop()
	}()

	shardkv.routineGroup.Add(1)
	go func() {
		defer shardkv.routineGroup.Done()
		shardkv.eventLoop()
	}()

	shardkv.rf = raft.Make(servers, me, persister, shardkv.applyCh)

	return shardkv
}
