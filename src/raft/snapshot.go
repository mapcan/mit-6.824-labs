package raft

type TakeSnapshotArgs struct {
	UpperLevelState []byte
	Index           int
	Term            int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func NewInstallSnapshotArgs(term int, leaderID int, lastIncludedIndex int, lastIncludedTerm int, data []byte) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
}

func NewInstallSnapshotReply(term int) *InstallSnapshotReply {
	return &InstallSnapshotReply{
		Term: term,
	}
}

type InstallSnapshotReply struct {
	Term int
}
