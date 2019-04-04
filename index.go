package dieci

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	blockSize = 24   // 4 + 4 + 16
	pageSize  = 4080 // 4096 - (4096 mod 24)
)

// Indexer is the interface for Datalog's index
type Indexer interface {
	Read(score Score) (Addr, bool)
	Write(score Score, a Addr) error
}

// Addr is data position and size in datalog
type Addr struct {
	pos  int
	size int
}

// cache is in memory lookup store
type cache map[Score]Addr

// Index represents an index of a datalog file
type Index struct {
	cache cache
	rw    io.ReadWriter
}

// NewIndex returns a new index structure with the given name
func NewIndex(rw io.ReadWriter) (*Index, error) {
	cache := make(cache)
	r := bufio.NewReaderSize(rw, pageSize)
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, eof bool) (int, []byte, error) {
		if eof && len(data) == 0 {
			return 0, nil, io.EOF
		}
		return blockSize, data, nil
	})
	buf := make([]byte, blockSize)
	scanner.Buffer(buf, blockSize)
	for scanner.Scan() {
		block := scanner.Bytes()
		pos := binary.BigEndian.Uint32(block[0:])
		size := binary.BigEndian.Uint32(block[4:])
		var score Score
		copy(score[:], block[8:])
		cache[score] = Addr{pos: int(pos), size: int(size)}
	}
	if scanner.Err() != nil {
		return nil, scanner.Err()
	}
	return &Index{cache: cache, rw: rw}, nil
}

// Read reads address of data for a given score
func (idx *Index) Read(score Score) (a Addr, ok bool) {
	a, ok = idx.cache[score]
	return
}

// Write writes given score into index file and adds it to the cache
func (idx *Index) Write(score Score, a Addr) error {
	if _, ok := idx.cache[score]; ok {
		return nil
	}
	buf := make([]byte, blockSize)
	binary.BigEndian.PutUint32(buf[0:], uint32(a.pos))
	binary.BigEndian.PutUint32(buf[4:], uint32(a.size))
	copy(buf[8:], score[:])
	_, err := idx.rw.Write(buf)
	if err != nil {
		return fmt.Errorf("index write failed: %s", err)
	}
	idx.cache[score] = a
	return nil
}
