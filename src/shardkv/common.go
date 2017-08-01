package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrOldShard       = "ErrOldShard"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongConfigNum = "ErrWrongConfigNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID string
	Sequence int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID string
	Sequence int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type ConfigChangeArgs struct {
	Config shardmaster.Config
}

type PullStateArgs struct {
	ConfigNum int
	Shard     int
	ClientID  string
	Sequence  int64
}

type PullStateReply struct {
	WrongLeader bool
	Err         Err
	Database    map[string]string
	Done        map[string]int64
}

type PushStateArgs struct {
	Sn       int
	Clock    *ShardLogicalClock
	Shard    *Shard
	ClientID string
	Sequence int64
}

type PushStateReply struct {
	WrongLeader bool
	Err         Err
	Sn          int
	Clock       *ShardLogicalClock
}
