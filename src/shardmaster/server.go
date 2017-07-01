package shardmaster

//import "fmt"
import "raft"
import "labrpc"
import "sync"
import "time"
import "sync/atomic"
import "encoding/gob"

type ShardMaster struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	done         map[string]int64
	configs      []Config // indexed by config num
	stopCh       chan bool
	stopped      int32
	routineGroup sync.WaitGroup
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	ClientID string
	Sequence int64
	Command  string
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
	c        chan error
}

func (sm *ShardMaster) Stopped() bool {
	return atomic.LoadInt32(&sm.stopped) == 1
}
func (sm *ShardMaster) SetStop() {
	atomic.StoreInt32(&sm.stopped, 1)
}

func (sm *ShardMaster) appendLog(op Op) bool {
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}
	select {
	case err := <-op.c:
		return err == nil
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) makeNewConfig() Config {
	var newConfig Config
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig.Num = lastConfig.Num + 1
	newConfig.Shards = lastConfig.Shards
	newConfig.Groups = map[int][]string{}
	for GID, servers := range lastConfig.Groups {
		newConfig.Groups[GID] = servers
	}
	return newConfig
}

func (sm *ShardMaster) rebalanceHelper(config *Config, gidshard *map[int][]int, maxGID *int, minGID *int) {
	(*gidshard) = make(map[int][]int)
	for shard, GID := range config.Shards {
		(*gidshard)[GID] = append((*gidshard)[GID], shard)
	}
	maxShards := 0
	*maxGID = 0
	minShards := NShards + 1
	*minGID = 0
	for GID, _ := range config.Groups {
		shards := len((*gidshard)[GID])
		if maxShards < shards {
			maxShards, *maxGID = shards, GID
		}
		if minShards > shards {
			minShards, *minGID = shards, GID
		}
	}
}

func (sm *ShardMaster) rebalance(config *Config, op string, gid int) {
	if len(config.Groups) == 1 {
		for GID, _ := range config.Groups {
			for i := 0; i < NShards; i++ {
				config.Shards[i] = GID
			}
		}
		return
	}
	gidshard := map[int][]int{}
	maxGID := 0
	minGID := 0

	if op == Join {
		n := NShards / len(config.Groups)
		for i := 0; i < n; i++ {
			sm.rebalanceHelper(config, &gidshard, &maxGID, &minGID)
			if maxGID == gid {
				break
			}
			shard := gidshard[maxGID][i]
			config.Shards[shard] = gid
		}
	} else if op == Leave {
		sm.rebalanceHelper(config, &gidshard, &maxGID, &minGID)
		for _, shard := range gidshard[gid] {
			config.Shards[shard] = minGID
		}
	}
}

func (sm *ShardMaster) apply(op Op) {
	switch op.Command {
	case Join:
		var args JoinArgs
		args.Servers = op.Servers
		sm.doJoin(&args)
	case Move:
		var args MoveArgs
		args.Shard = op.Shard
		args.GID = op.GID
		sm.doMove(&args)
	case Leave:
		var args LeaveArgs
		args.GIDs = op.GIDs
		sm.doLeave(&args)
	}
	sm.done[op.ClientID] = op.Sequence
}

func (sm *ShardMaster) doJoin(args *JoinArgs) {
	newConfig := sm.makeNewConfig()
	for GID, servers := range args.Servers {
		_, ok := newConfig.Groups[GID]
		if !ok {
			newConfig.Groups[GID] = servers
			sm.rebalance(&newConfig, Join, GID)
		}
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	cmd := Op{
		ClientID: args.ClientID,
		Sequence: args.Sequence,
		Command:  Join,
		Servers:  args.Servers,
		c:        make(chan error),
	}
	ok := sm.appendLog(cmd)
	if ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
	}
}

func (sm *ShardMaster) doLeave(args *LeaveArgs) {
	newConfig := sm.makeNewConfig()
	keys := []int{}
	for _, GID := range args.GIDs {
		_, ok := newConfig.Groups[GID]
		if ok {
			keys = append(keys, GID)
		}
	}
	for _, key := range keys {
		delete(newConfig.Groups, key)
		sm.rebalance(&newConfig, Leave, key)
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	cmd := Op{
		ClientID: args.ClientID,
		Sequence: args.Sequence,
		Command:  Leave,
		GIDs:     args.GIDs,
		c:        make(chan error),
	}
	ok := sm.appendLog(cmd)
	if ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
	}
}

func (sm *ShardMaster) doMove(args *MoveArgs) {
	newConfig := sm.makeNewConfig()
	newConfig.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	cmd := Op{
		ClientID: args.ClientID,
		Sequence: args.Sequence,
		Command:  Move,
		Shard:    args.Shard,
		GID:      args.GID,
		c:        make(chan error),
	}
	ok := sm.appendLog(cmd)
	if ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
	}
}

func (sm *ShardMaster) doQuery(args *QueryArgs, reply *QueryReply) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.done[args.ClientID] = args.Sequence
	if args.Num < -1 {
		return
	}
	if args.Num == -1 || args.Num > len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
		return
	}
	reply.Config = sm.configs[args.Num]
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	cmd := Op{
		ClientID: args.ClientID,
		Sequence: args.Sequence,
		Command:  Query,
		Num:      args.Num,
		c:        make(chan error),
	}
	ok := sm.appendLog(cmd)
	if ok {
		reply.Err = OK
		sm.doQuery(args, reply)
	} else {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	// Your code here, if desired.
	if sm.Stopped() {
		return
	}
	close(sm.stopCh)
	sm.routineGroup.Wait()
	sm.SetStop()
	sm.rf.Kill()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) detectDuplicate(clientID string, sequence int64) bool {
	seq, ok := sm.done[clientID]
	return ok && seq >= sequence
}

func (sm *ShardMaster) loop() {
	for {
		select {
		case <-sm.stopCh:
			sm.SetStop()
			return
		case msg := <-sm.applyCh:
			op := msg.Command.(Op)
			sm.mu.Lock()
			if !sm.detectDuplicate(op.ClientID, op.Sequence) {
				sm.apply(op)
			}
			sm.mu.Unlock()
			if op.c != nil {
				op.c <- nil
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Num = 0
	sm.configs[0].Groups = map[int][]string{}
	sm.done = make(map[string]int64)

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.stopCh = make(chan bool)

	sm.routineGroup.Add(1)
	go func() {
		defer sm.routineGroup.Done()
		sm.loop()
	}()

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	return sm
}
