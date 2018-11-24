// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type indexer interface {
	get(score Score) (int, int, bool)
	put(score Score, pos, len int) error
	close() error
	delete() error
}

// addr is index's address type alias
type addr [2]int

// cache is in memory lookup store
type cache map[Score]addr

// index helps to locate a block in the data log given its score.
type index struct {
	cache cache
	*os.File
}

// openIndex opens the named index and loads its content in the memory cache
func openIndex(name string) (i *index, err error) {
	fileName := fmt.Sprintf("%s.idx", name)
	c, err := loadIndex(fileName)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	i = &index{c, f}
	if len(c) == 0 {
		err = rebuildIndex(name, i)
	}
	return
}

// load reads index file if presented into memory
func loadIndex(fileName string) (c cache, err error) {
	c = make(cache)
	f, err := os.Open(fileName)
	if err != nil {
		return
	}
	defer f.Close()
	bufSize := 2*IntSize + ScoreSize
	buf := make([]byte, bufSize, bufSize)
	for {
		if _, err = f.Read(buf); err == io.EOF {
			break
		}
		var score Score
		pos := binary.BigEndian.Uint32(buf[0:IntSize])
		len := binary.BigEndian.Uint32(buf[IntSize : 2*IntSize])
		copy(score[:], buf[2*IntSize:bufSize])
		c[score] = addr{int(pos), int(len)}
	}
	if err == io.EOF {
		return c, nil
	}
	return
}

// rebuild reads data log and recreates index for it
// FIXME! I'll need an iterator on data log here
func rebuildIndex(name string, i *index) error {
	f, err := os.Open(fmt.Sprintf("%s.data", name))
	if err != nil {
		return err
	}
	defer f.Close()
	pos := IntSize
	lenBuf := make([]byte, IntSize)
	for {
		if _, err = f.Read(lenBuf); err == io.EOF {
			err = nil
			break
		}
		len := int(binary.BigEndian.Uint32(lenBuf))
		buf := make([]byte, len)
		if _, err = f.Read(buf); err == io.EOF {
			err = nil
			break
		}
		score := makeScore(buf)
		err = i.put(score, pos, len)
		if err != nil {
			break
		}
		pos += len + IntSize
	}
	return err
}

// get returns an address for a given score if it's known
func (i *index) get(score Score) (pos, len int, ok bool) {
	addr, ok := i.cache[score]
	if !ok {
		return
	}
	pos, len = addr[0], addr[1]
	return
}

// put stores a given score and address
func (i *index) put(score Score, pos, len int) error {
	if _, ok := i.cache[score]; ok {
		return nil
	}
	bufSize := 2*IntSize + ScoreSize
	buf := make([]byte, bufSize, bufSize)
	binary.BigEndian.PutUint32(buf[0:IntSize], uint32(pos))
	binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], uint32(len))
	copy(buf[2*IntSize:bufSize], score[:])
	_, err := i.Write(buf)
	if err != nil {
		return err
	}
	i.cache[score] = addr{pos, len}
	return err
}

// close releases cache and closes an index file handler
func (i *index) close() error {
	i.cache = make(cache)
	return i.Close()
}

// delete releases cache and closes and erases an index file
func (i *index) delete() error {
	err := i.close()
	if err != nil {
		return err
	}
	return os.Remove(i.Name())
}
