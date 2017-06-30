package raft

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
	log     *Log
	event   *Event
}

func NewLogEntry(log *Log, event *Event, index int, term int, command interface{}) *LogEntry {
	entry := LogEntry{}
	entry.Term = term
	entry.Index = index
	entry.Command = command
	entry.log = log
	entry.event = event
	return &entry
}
