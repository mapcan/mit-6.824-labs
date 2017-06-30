package raft

import "fmt"
import "sync"
import "bytes"
import "errors"
import "encoding/gob"

type Log struct {
	apply       func(entry *LogEntry) (interface{}, error)
	entries     []*LogEntry
	commitIndex int
	startIndex  int
	startTerm   int
	sync.RWMutex
}

func NewLog() *Log {
	return &Log{
		entries: make([]*LogEntry, 0),
	}
}

func (log *Log) GobEncode() ([]byte, error) {
	log.Lock()
	defer log.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(log.entries)
	e.Encode(log.commitIndex)
	e.Encode(log.startIndex)
	e.Encode(log.startTerm)
	d := w.Bytes()
	return d, nil
}

func (log *Log) GobDecode(data []byte) error {
	log.Lock()
	defer log.Unlock()
	if data == nil || len(data) < 1 {
		return nil
	}
	var startIndex int
	var startTerm int
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&log.entries)
	d.Decode(&log.commitIndex)
	d.Decode(&startIndex)
	if startIndex > log.startIndex {
		log.startIndex = startIndex
	}
	d.Decode(&startTerm)
	if startTerm > log.startTerm {
		log.startTerm = startTerm
	}
	for _, entry := range log.entries {
		if entry.Index > log.startIndex && entry.Index <= log.commitIndex {
			log.apply(entry)
		}
	}
	return nil
}

func (log *Log) CommitIndex() int {
	log.RLock()
	defer log.RUnlock()
	return log.commitIndex
}

func (log *Log) CurrentIndex() int {
	log.RLock()
	defer log.RUnlock()
	return log.InternalCurrentIndex()
}

func (log *Log) InternalCurrentIndex() int {
	if len(log.entries) == 0 {
		return log.startIndex
	}
	return log.entries[len(log.entries)-1].Index
}

func (log *Log) NextIndex() int {
	return log.InternalCurrentIndex() + 1
}

func (log *Log) Empty() bool {
	log.RLock()
	defer log.RUnlock()
	return len(log.entries) == 0 && log.startIndex == 0
}

func (log *Log) CurrentTerm() int {
	log.RLock()
	defer log.RUnlock()
	if len(log.entries) == 0 {
		return log.startTerm
	}
	return log.entries[len(log.entries)-1].Term
}

func (log *Log) CreateEntry(term int, command interface{}, event *Event) *LogEntry {
	return NewLogEntry(log, event, log.NextIndex(), term, command)
}

func (log *Log) GetEntry(index int) *LogEntry {
	log.RLock()
	defer log.RUnlock()
	if index <= log.startIndex || index > log.startIndex+len(log.entries) {
		return nil
	}
	return log.entries[index-log.startIndex-1]
}

func (log *Log) ContainsEntry(index int, term int) bool {
	entry := log.GetEntry(index)
	return entry != nil && entry.Term == term
}

func (log *Log) GetEntriesAfter(index int) ([]*LogEntry, int) {
	log.RLock()
	defer log.RUnlock()

	if index < log.startIndex {
		return nil, 0
	}
	if index > log.startIndex+len(log.entries) {
		index = log.startIndex + len(log.entries)
	}
	if index == log.startIndex {
		return log.entries, log.startTerm
	}
	entries := log.entries[index-log.startIndex:]
	return entries, log.entries[index-log.startIndex-1].Term
}

func (log *Log) CommitInfo() (index int, term int) {
	log.RLock()
	defer log.RUnlock()

	if log.commitIndex == 0 {
		return 0, 0
	}
	if log.commitIndex == log.startIndex {
		return log.startIndex, log.startTerm
	}
	entry := log.entries[log.commitIndex-log.startIndex-1]
	return entry.Index, entry.Term
}

func (log *Log) LastInfo() (index int, term int) {
	log.RLock()
	defer log.RUnlock()

	if len(log.entries) == 0 {
		return log.startIndex, log.startTerm
	}
	entry := log.entries[len(log.entries)-1]
	return entry.Index, entry.Term
}

func (log *Log) UpdateCommitIndex(index int) {
	log.Lock()
	defer log.Unlock()

	if index > log.commitIndex {
		log.commitIndex = index
	}
}

func (log *Log) SetCommitIndex(index int) error {
	log.Lock()
	defer log.Unlock()

	if index > log.startIndex+len(log.entries) {
		index = log.startIndex + len(log.entries)
	}
	if index < log.commitIndex {
		return nil
	}
	for i := log.commitIndex + 1; i <= index; i++ {
		entry := log.entries[i-log.startIndex-1]
		log.commitIndex = entry.Index
		returnValue, err := log.apply(entry)
		if entry.event != nil {
			entry.event.returnValue = returnValue
			entry.event.c <- err
		}
	}
	return nil
}

func (log *Log) Truncate(index int, term int) error {
	log.Lock()
	defer log.Unlock()

	if index < log.commitIndex {
		return fmt.Errorf("Raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", log.commitIndex, index, term)
	}

	if index > log.startIndex+len(log.entries) {
		return fmt.Errorf("Raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(log.entries), index, term)
	}

	if index == log.startIndex {
		for _, entry := range log.entries {
			if entry.event != nil {
				entry.event.c <- errors.New("Command failed to be commited due to node failure")
			}
		}
		log.entries = []*LogEntry{}
	} else {
		entry := log.entries[index-log.startIndex-1]
		if len(log.entries) > 0 && entry.Term != term {
			return fmt.Errorf("Raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term)
		}

		if index < log.startIndex+len(log.entries) {
			for i := index - log.startIndex; i < len(log.entries); i++ {
				entry := log.entries[i]
				if entry.event != nil {
					entry.event.c <- errors.New("Command failed to be commited due to node failure")
				}
			}
		}
		log.entries = log.entries[0 : index-log.startIndex]
	}

	return nil
}

func (log *Log) AppendEntries(entries []*LogEntry) error {
	log.Lock()
	defer log.Unlock()

	for _, entry := range entries {
		if len(log.entries) > 0 {
			lastEntry := log.entries[len(log.entries)-1]
			if entry.Term < lastEntry.Term {
				return fmt.Errorf("Raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
			} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
				return fmt.Errorf("Raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
			}
		}
		log.entries = append(log.entries, entry)
	}
	return nil
}

func (log *Log) AppendEntry(entry *LogEntry) error {
	log.Lock()
	defer log.Unlock()

	if len(log.entries) > 0 {
		lastEntry := log.entries[len(log.entries)-1]
		if entry.Term < lastEntry.Term {
			return fmt.Errorf("Raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
			return fmt.Errorf("Raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		}
	}

	log.entries = append(log.entries, entry)

	return nil
}

func (log *Log) Compact(index int, term int) (int, int, error) {
	log.Lock()
	defer log.Unlock()

	var entries []*LogEntry
	//fmt.Printf("Server LogCompact, Index: %d, log.startIndex: %d, log.lastIndex: %d\n", index, log.startIndex, log.entries[len(log.entries)-1].Index)
	if index >= log.InternalCurrentIndex() {
		entries = make([]*LogEntry, 0)
	} else if index < log.startIndex {
		return 0, 0, fmt.Errorf("Raft.Log: Compact, Index does not exist, (%v,%v)", index, log.startIndex)
	} else {
		entries = log.entries[index-log.startIndex:]
	}
	log.startIndex = index
	log.startTerm = term
	log.entries = entries
	if log.commitIndex < index {
		log.commitIndex = index
	}
	return log.startTerm, log.startIndex, nil
}

func (log *Log) Open() {
}

func (log *Log) Close() {
}
