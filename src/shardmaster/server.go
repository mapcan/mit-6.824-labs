package shardmaster

//import "fmt"
import "raft"
import "labrpc"
import "sync"
import "time"
import "encoding/gob"

type ShardMaster struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	done         map[string]int64
	notifyCommit map[int]chan Op
	configs      []Config // indexed by config num
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
}

func (sm *ShardMaster) equalOp(op1 Op, op2 Op) bool {
	//if op1.Sequence != op2.Sequence {
	//	return false
	//}
	if op1.Command != op2.Command {
		return false
	}
	if op1.Shard != op2.Shard {
		return false
	}
	if op1.GID != op2.GID {
		return false
	}
	if op1.Num != op2.Num {
		return false
	}
	if len(op1.GIDs) != len(op2.GIDs) {
		return false
	}
	for i, GID1 := range op1.GIDs {
		if GID1 != op2.GIDs[i] {
			return false
		}
	}
	if len(op1.Servers) != len(op2.Servers) {
		return false
	}
	for k1, v1 := range op1.Servers {
		v2, ok := op2.Servers[k1]
		if !ok {
			return false
		}
		if len(v1) != len(v2) {
			return false
		}
		for i, vv1 := range v1 {
			if vv1 != v2[i] {
				return false
			}
		}
	}
	return true
}

//func (sm *ShardMaster) getSequence() int64 {
//	sm.mu.Lock()
//	defer sm.mu.Unlock()
//	seq := sm.sequence
//	sm.sequence += 1
//	return seq
//}

func (sm *ShardMaster) appendLog(cmd Op) bool {
	index, _, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	ch, ok := sm.notifyCommit[index]
	if !ok {
		ch = make(chan Op, 1)
		sm.notifyCommit[index] = ch
	}
	sm.mu.Unlock()

	select {
	case op := <-ch:
		sm.mu.Lock()
		delete(sm.notifyCommit, index)
		sm.mu.Unlock()
		return sm.equalOp(cmd, op)
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
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) detectDuplicate(clientID string, sequence int64) bool {
	seq, ok := sm.done[clientID]
	return ok && seq >= sequence
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

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.notifyCommit = make(map[int]chan Op)
	sm.done = make(map[string]int64)

	go func() {
		for {
			msg := <-sm.applyCh
			if msg.UseSnapshot {
			} else {
				op := msg.Command.(Op)
				sm.mu.Lock()
				if !sm.detectDuplicate(op.ClientID, op.Sequence) {
					sm.apply(op)
				}
				ch, ok := sm.notifyCommit[msg.Index]
				if ok {
					ch <- op
				}
				sm.mu.Unlock()
			}
		}
	}()

	return sm
}
