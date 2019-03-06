package dieci

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	intSize       = 4
	doubleIntSize = 8
	bufSize       = 24
	pageSize      = 4080 // ~4K to match memory page
)

// Indexer is the interface for Datalog's index
type Indexer interface {
	Open() error
	Name() string
	Read(score Score) (int, int, bool)
	Write(score Score, p, l int) error
	Close() error
}

// addr is index's address type alias
type addr [2]int

// cache is in memory lookup store
type cache map[Score]addr

// Index represents an index of a datalog file
type Index struct {
	name  string
	cache cache
	rwc   *os.File
}

// NewIndex returns a new index structure with the given name
func NewIndex(name string) *Index {
	cache := make(cache, 0)
	return &Index{name: name, cache: cache}
}

// Open opens the named index
func (idx *Index) Open() error {
	fileName := fmt.Sprintf("%s.idx", idx.name)
	cache, err := loadCache(fileName)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	idx.cache = cache
	idx.rwc = f
	if len(cache) == 0 {
		err = idx.Rebuild()
	}
	return err
}

// Name returns name of index file
func (idx *Index) Name() string {
	return idx.rwc.Name()
}

// load reads index file if presented into memory
func loadCache(fileName string) (cache, error) {
	cache := make(cache)
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return cache, err
	}
	for {
		var readError error
		page := make([]byte, pageSize, pageSize)
		n, readError := f.Read(page)
		if readError != nil {
			err = readError
			break
		}
		var parseErr error
		r := bytes.NewReader(page[:n])
		for {
			var score Score
			buf := make([]byte, bufSize, bufSize)
			_, parseErr := r.Read(buf)
			if parseErr != nil {
				break
			}
			p := binary.BigEndian.Uint32(buf[0:intSize])
			l := binary.BigEndian.Uint32(buf[intSize:doubleIntSize])
			copy(score[:], buf[doubleIntSize:bufSize])
			cache[score] = addr{int(p), int(l)}
		}
		if parseErr != nil && parseErr != io.EOF {
			err = parseErr
			break
		}
	}
	if err == io.EOF {
		return cache, nil
	}
	return cache, err
}

// Rebuild is essentially scans datalog and build index and cache again
func (idx *Index) Rebuild() error {
	f, err := os.Open(fmt.Sprintf("%s.data", idx.name))
	if err != nil {
		return err
	}
	defer f.Close()
	p := intSize
	lBuf := make([]byte, intSize)
	for {
		if _, err = f.Read(lBuf); err == io.EOF {
			err = nil
			break
		}
		l := int(binary.BigEndian.Uint32(lBuf))
		buf := make([]byte, l)
		if _, err = f.Read(buf); err == io.EOF {
			err = nil
			break
		}
		score := MakeScore(buf)
		err = idx.Write(score, p, l)
		if err != nil {
			break
		}
		p += l + intSize
	}
	return err
}

// Read reads address of data for a given score
func (idx *Index) Read(score Score) (p, l int, ok bool) {
	addr, ok := idx.cache[score]
	if !ok {
		return
	}
	p, l = addr[0], addr[1]
	return
}

// Write writes given score into index file and adds it to the cache
func (idx *Index) Write(score Score, p, l int) error {
	if _, ok := idx.cache[score]; ok {
		return nil
	}
	buf := make([]byte, bufSize, bufSize)
	binary.BigEndian.PutUint32(buf[0:intSize], uint32(p))
	binary.BigEndian.PutUint32(buf[intSize:doubleIntSize], uint32(l))
	copy(buf[doubleIntSize:bufSize], score[:])
	_, err := idx.rwc.Write(buf)
	if err != nil {
		return fmt.Errorf("Index write failed: %s", err)
	}
	idx.cache[score] = addr{p, l}
	return err
}

// Close closes the index and resets the cache
func (idx *Index) Close() error {
	idx.cache = make(cache)
	return idx.rwc.Close()
}
