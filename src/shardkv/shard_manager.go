package shardkv

import "bytes"
import "encoding/gob"

type ShardLogicalClock struct {
	ConfigNum int
	Term      int
}

func NewShardLogicalClock(num int, term int) *ShardLogicalClock {
	return &ShardLogicalClock{
		ConfigNum: num,
		Term:      term,
	}
}

func (svc *ShardLogicalClock) GobEncode() ([]byte, error) {
	var err error
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	if err = encoder.Encode(svc.ConfigNum); err != nil {
		return nil, err
	}
	if err = encoder.Encode(svc.Term); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (svc *ShardLogicalClock) GobDecode(data []byte) error {
	var err error
	var c, t int
	if data == nil || len(data) < 1 {
		return nil
	}
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	if err = decoder.Decode(&c); err != nil {
		return err
	}
	if err = decoder.Decode(&t); err != nil {
		return err
	}
	svc.ConfigNum = c
	svc.Term = t
	return nil
}

func (svc *ShardLogicalClock) LaterThan(rhs *ShardLogicalClock) bool {
	return svc.Term > rhs.Term && svc.ConfigNum > rhs.ConfigNum
}

type ShardManager struct {
	shards map[int]*Shard
	clocks map[int]*ShardLogicalClock
}

func NewShardManager() *ShardManager {
	return &ShardManager{
		shards: make(map[int]*Shard),
		clocks: make(map[int]*ShardLogicalClock),
	}
}

func (sm *ShardManager) GobEncode() ([]byte, error) {
	var err error
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	if err = encoder.Encode(sm.shards); err != nil {
		return nil, err
	}
	if err = encoder.Encode(sm.clocks); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (sm *ShardManager) GobDecode(data []byte) error {
	var err error
	var shards map[int]*Shard
	var clocks map[int]*ShardLogicalClock

	if data == nil || len(data) < 1 {
		return nil
	}

	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	if err = decoder.Decode(&shards); err != nil {
		return err
	}
	if err = decoder.Decode(&clocks); err != nil {
		return err
	}
	sm.shards = shards
	sm.clocks = clocks
	return nil
}

func (sm *ShardManager) CanUpdateShard(sn int, clock *ShardLogicalClock) bool {
	_, ok := sm.clocks[sn]
	return !ok || clock.LaterThan(sm.clocks[sn])
}

func (sm *ShardManager) UpdateShard(sn int, clock *ShardLogicalClock, shard *Shard) {
	sm.clocks[sn] = clock
	sm.shards[sn] = shard
}

func (sm *ShardManager) GetClock(sn int) *ShardLogicalClock {
	clock, ok := sm.clocks[sn]
	if ok {
		return clock
	}
	return nil
}

func (sm *ShardManager) SetClock(sn int, clock *ShardLogicalClock) {
	sm.clocks[sn] = clock
}

func (sm *ShardManager) GetShard(sn int) *Shard {
	shard, ok := sm.shards[sn]
	if ok {
		return shard
	}
	return nil
}

func (sm *ShardManager) AddShard(sn int, clock *ShardLogicalClock, shard *Shard) {
	sm.clocks[sn] = clock
	sm.shards[sn] = shard
}
