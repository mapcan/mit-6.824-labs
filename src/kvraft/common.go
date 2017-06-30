package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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

func NewPutAppendReply(wrongLeader bool, err Err) *PutAppendReply {
	return &PutAppendReply{
		WrongLeader: wrongLeader,
		Err:         err,
	}
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

func NewGetReply(wrongLeader bool, err Err, value string) *GetReply {
	return &GetReply{
		WrongLeader: wrongLeader,
		Err:         err,
		Value:       value,
	}
}
