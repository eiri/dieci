package dieci

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	intSize       = 4
	doubleIntSize = 8
	bufSize       = 24
	pageSize      = 4080 // ~4K to match memory page
)

// Indexer is the interface for Datalog's index
type Indexer interface {
	Load() error
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
func NewIndex(rw io.ReadWriter) *Index {
	return &Index{cache: make(cache), rw: rw}
}

// Load cache from giver Reader
func (idx *Index) Load() error {
	idx.cache = make(cache)
	for {
		page := make([]byte, pageSize, pageSize)
		n, err := idx.rw.Read(page)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		br := bytes.NewReader(page[:n])
		for {
			var score Score
			buf := make([]byte, bufSize, bufSize)
			_, err := br.Read(buf)
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				break
			}
			pos := binary.BigEndian.Uint32(buf[0:intSize])
			size := binary.BigEndian.Uint32(buf[intSize:doubleIntSize])
			copy(score[:], buf[doubleIntSize:bufSize])
			idx.cache[score] = Addr{pos: int(pos), size: int(size)}
		}
	}
	return nil
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
	buf := make([]byte, bufSize, bufSize)
	binary.BigEndian.PutUint32(buf[0:intSize], uint32(a.pos))
	binary.BigEndian.PutUint32(buf[intSize:doubleIntSize], uint32(a.size))
	copy(buf[doubleIntSize:bufSize], score[:])
	_, err := idx.rw.Write(buf)
	if err != nil {
		return fmt.Errorf("Index write failed: %s", err)
	}
	idx.cache[score] = a
	return nil
}
