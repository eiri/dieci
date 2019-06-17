package dieci

import (
	"bufio"
	"encoding/binary"
	"fmt"
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
	rw    io.ReadWriter
}

// NewIndex returns a new index structure with the given name
func NewIndex(rw io.ReadWriter) (*Index, error) {
	cache := make(cache)
	idx := &Index{cache: cache, rw: rw}
	scanner := bufio.NewScanner(rw)
	scanner.Split(func(data []byte, eof bool) (int, []byte, error) {
		if eof && len(data) == 0 {
			return 0, nil, io.EOF
		}
		return blockSize, data, nil
	})
	for scanner.Scan() {
		block := scanner.Bytes()
		score, addr := idx.Decode(block)
		cache[score] = addr
		idx.cur = addr.pos + addr.size
	}
	if scanner.Err() != nil {
		return nil, scanner.Err()
	}
	return idx, nil
}

// Rebuild index by scaning given datalog reader
func (idx *Index) Rebuild(reader io.Reader) error {
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

	var err error
	for scanner.Scan() {
		block := scanner.Bytes()
		size := int(binary.BigEndian.Uint32(block[:intSize]))
		var score Score
		copy(score[:], block[intSize:])
		err = idx.Write(score, size)
		if err != nil {
			break
		}
	}
	if err == nil {
		err = scanner.Err()
	}
	return err
}

// Read reads address of data for a given score
func (idx *Index) Read(score Score) (a Addr, ok bool) {
	a, ok = idx.cache[score]
	return
}

// Write writes given score into index file and adds it to the cache
func (idx *Index) Write(score Score, size int) error {
	if _, ok := idx.cache[score]; ok {
		return nil
	}
	addr := Addr{pos: idx.cur, size: size}
	idx.cache[score] = addr
	idx.cur = addr.pos + addr.size
	buf := idx.Encode(score, addr)
	if _, err := idx.rw.Write(buf); err != nil {
		return fmt.Errorf("index: write failed: %s", err)
	}
	return nil
}

// Decode serialized bytes to score and addess
func (idx *Index) Decode(block []byte) (score Score, addr Addr) {
	copy(score[:], block[:])
	pos := binary.BigEndian.Uint32(block[scoreSize:])
	size := binary.BigEndian.Uint32(block[scoreSize+4:])
	addr = Addr{pos: int(pos), size: int(size)}
	return score, addr
}

// Encode serialize map entry into slice of bytes suitable to write on disk
func (idx *Index) Encode(score Score, addr Addr) []byte {
	buf := make([]byte, blockSize)
	copy(buf[:], score[:])
	binary.BigEndian.PutUint32(buf[scoreSize:], uint32(addr.pos))
	binary.BigEndian.PutUint32(buf[scoreSize+4:], uint32(addr.size))
	return buf
}

// Len returns current length of cache
func (idx *Index) Len() int {
	return len(idx.cache)
}
