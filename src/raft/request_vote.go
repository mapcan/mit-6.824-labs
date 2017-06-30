package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	peer         *Peer
	Term         int
	LastLogIndex int
	LastLogTerm  int
	CandidateID  int
}

func NewRequestVoteArgs(term int, candidateID int, lastLogIndex int, lastLogTerm int) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         term,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		CandidateID:  candidateID,
	}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	peer        *Peer
	Term        int
	VoteGranted bool
}

func NewRequestVoteReply(term int, voteGranted bool) *RequestVoteReply {
	return &RequestVoteReply{
		Term:        term,
		VoteGranted: voteGranted,
	}

}
