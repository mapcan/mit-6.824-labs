package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"
import "io"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id       string
	sequence int64
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id, _ = newUUID()
	ck.sequence = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	var request GetArgs
	ck.mu.Lock()
	request.ClientID = ck.id
	request.Sequence = ck.sequence
	ck.sequence++
	request.Key = key
	ck.mu.Unlock()
	for {
		for _, server := range ck.servers {
			var reply GetReply
			ok := server.Call("RaftKV.Get", &request, &reply)
			if ok && !reply.WrongLeader {
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var request PutAppendArgs
	ck.mu.Lock()
	request.ClientID = ck.id
	request.Sequence = ck.sequence
	ck.sequence++
	request.Key = key
	request.Value = value
	request.Op = op
	ck.mu.Unlock()
	for {
		for i, server := range ck.servers {
			var reply PutAppendReply
			//DPrintf("Server: %d, PutAppendArgs: %v", i, request)
			ok := server.Call("RaftKV.PutAppend", &request, &reply)
			//DPrintf("Server: PutAppendReply: %v", i, reply)
			if ok && !reply.WrongLeader {
				DPrintf("Server %d, Key: %s, Value: %s, PutAppendArgs: %v", i, request.Key, request.Value, request)
				DPrintf("Server %d, Key: %s, Value: %s, PutAppendReply: %v", i, request.Key, request.Value, reply)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
