package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

// Append appends the byte slice to the store.
// It returns the number of bytes written and the position of the record.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = uint64(s.size)
	// Write te length of the record to the head of the record.
	// This tells us how many bytes to read to get the record value.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// Write the record itself.
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Ensure the buffer is empty.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// Create a buffer of size lenWidth to read the record size which is a uint64.
	size := make([]byte, lenWidth)
	// Load the bytes at pos into the buffer named `size`.
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// Since size contains bytes that represent the record size, we can parse it using the
	// encoding `enc` that was used at the time of writing.
	// Thus `b` is an empty buffer of record size.
	b := make([]byte, enc.Uint64(size))

	// Since each record starts with lenWidth (the uint64 size of the record).
	// We add that to pos to get the pos at which the actual record is stored.
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
