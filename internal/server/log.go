package server

import (
	"fmt"
	"sync"
)

type Offset uint64
type LogValue []byte

type Record struct {
	Value  LogValue `json:"value"`
	Offset Offset   `json:"offset"`
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func ErrOffsetNotFound(o Offset, maxOffset Offset) error {
	return fmt.Errorf("given offset=%d not found, largest offset=%d", o, maxOffset)
}

func (l *Log) Append(r Record) (Offset, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.Offset = Offset(len(l.records))
	l.records = append(l.records, r)
	return r.Offset, nil
}

func (l *Log) Read(o Offset) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var maxOffset Offset = Offset(len(l.records)) - 1
	if o > maxOffset {
		return Record{}, ErrOffsetNotFound(o, maxOffset)
	}
	return l.records[o], nil
}
