package raft

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int
	LeaderID     int
	Entries      []*LogEntry
}

type AppendEntriesReply struct {
	Term        int
	Index       int
	CommitIndex int
	Success     bool
	Peer        int
	Append      bool
}

func NewAppendEntriesArgs(term int, prevLogIndex int, prevLogTerm int, commitIndex int, leaderID int, entries []*LogEntry) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderID:     leaderID,
		Entries:      entries,
	}
	return args
}

func NewAppendEntriesReply(term int, success bool, index int, commitIndex int) *AppendEntriesReply {
	return &AppendEntriesReply{
		Term:        term,
		Index:       index,
		Success:     success,
		CommitIndex: commitIndex,
	}
}
