package shardmaster

//
// Shardmaster clerk.
//

//import "fmt"
import "io"
import "fmt"
import "sync"
import "labrpc"
import "math/big"
import "crypto/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.id, _ = newUUID()
	ck.sequence = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	var args QueryArgs
	ck.mu.Lock()
	args.ClientID = ck.id
	args.Sequence = ck.sequence
	args.Num = num
	ck.sequence++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	var args JoinArgs
	ck.mu.Lock()
	args.ClientID = ck.id
	args.Sequence = ck.sequence
	args.Servers = servers
	ck.sequence++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	var args LeaveArgs
	ck.mu.Lock()
	args.ClientID = ck.id
	args.Sequence = ck.sequence
	args.GIDs = gids
	ck.sequence++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	var args MoveArgs
	ck.mu.Lock()
	args.ClientID = ck.id
	args.Sequence = ck.sequence
	args.Shard = shard
	args.GID = gid
	ck.sequence++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}
