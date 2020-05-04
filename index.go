package dieci

import (
	"bufio"
	"encoding/binary"
	"io"
)

const (
	blockSize = 16 // 4 + 4 + 8
)

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
	cur   int
}

// NewIndex returns a new index structure with the given name
func NewIndex() *Index {
	cache := make(cache)
	idx := &Index{cache: cache, cur: 0}
	return idx
}

// Load reads given reader of datalog and fills index with its scores
func (idx *Index) Load(reader io.Reader) error {
	idx.cache = make(cache)
	idx.cur = 0
	scanner := bufio.NewScanner(reader)
	blockSize := intSize + scoreSize
	scanner.Split(func(data []byte, eof bool) (int, []byte, error) {
		if eof {
			return 0, nil, io.EOF
		}
		if len(data) < blockSize {
			return 0, nil, nil
		}
		advance := intSize + int(binary.BigEndian.Uint32(data[:intSize]))
		if len(data) < advance {
			return 0, nil, nil
		}
		return advance, data[:blockSize], nil
	})

	for scanner.Scan() {
		block := scanner.Bytes()
		size := int(binary.BigEndian.Uint32(block[:intSize]))
		var score Score
		copy(score[:], block[intSize:])
		idx.Put(score, size+4)
	}
	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
}

// Get reads address of data for a given score
func (idx *Index) Get(score Score) (a Addr, ok bool) {
	a, ok = idx.cache[score]
	return
}

// Put writes given score into index file and adds it to the cache
func (idx *Index) Put(score Score, size int) {
	if _, ok := idx.cache[score]; !ok {
		addr := Addr{pos: idx.cur, size: size}
		idx.cache[score] = addr
		idx.cur = addr.pos + addr.size
	}
}

// Len returns current length of cache
func (idx *Index) Len() int {
	return len(idx.cache)
}
