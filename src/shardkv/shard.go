package shardkv

import "bytes"
import "encoding/gob"

import "kvraft"

//shard
//--sn:shard##
//--kv
//--recorder
//--Get
//--Set
//--GobEncode
//--GobDecode

type Shard struct {
	sn       int
	kv       *raftkv.KV
	recorder *raftkv.Recorder
}

func NewShard(sn int) *Shard {
	return &Shard{
		sn:       sn,
		kv:       raftkv.NewKV(),
		recorder: raftkv.NewRecorder(),
	}
}

func (s *Shard) Get(key string) string {
	return s.kv.Get(key)
}

func (s *Shard) Set(key string, value string) {
	s.kv.Set(key, value)
}

func (s *Shard) Record(cid string, seq int64) {
	s.recorder.Record(cid, seq)
}

func (s *Shard) Recorded(cid string, seq int64) bool {
	return s.recorder.Recorded(cid, seq)
}

func (s *Shard) GobEncode() ([]byte, error) {
	var err error
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	if err = encoder.Encode(s.sn); err != nil {
		return nil, err
	}
	if err = encoder.Encode(s.kv); err != nil {
		return nil, err
	}
	if err = encoder.Encode(s.recorder); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *Shard) GobDecode(data []byte) error {
	var err error
	var sn int
	var kv raftkv.KV
	var recorder raftkv.Recorder

	if data == nil || len(data) < 1 {
		return nil
	}
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	if err = decoder.Decode(&sn); err != nil {
		return err
	}
	if err = decoder.Decode(&kv); err != nil {
		return err
	}
	if err = decoder.Decode(&recorder); err != nil {
		return err
	}
	s.sn = sn
	s.kv = &kv
	s.recorder = &recorder
	return nil
}
