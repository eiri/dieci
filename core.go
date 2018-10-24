// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"os"
)

// ScoreSize is the size of score in bytes
const ScoreSize = 16

// Score is type alias for score representation
type Score [ScoreSize]byte

// addr is index's address type alias
type addr [2]int

// index is index type alias
type index map[Score]addr

// Store represents a data store.
type Store struct {
	idx index
	eof int
	*os.File
}

// New creates a new empty storage
func New() (s *Store, err error) {
	buf := make([]byte, 16)
	_, err = rand.Read(buf)
	if err != nil {
		return
	}
	storeName := fmt.Sprintf("%x.data", buf)
	f, err := os.OpenFile(storeName, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return
	}
	f.Close()
	return Open(storeName)
}

// Open opens provided storage
func Open(storeName string) (s *Store, err error) {
	f, err := os.OpenFile(storeName, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	idx := buildIndex(f)
	// reset fd and make Store
	_, err = f.Seek(0, 0)
	s = &Store{idx, 0, f}
	return
}

func buildIndex(f *os.File) index {
	idx := make(index)
	pos := 0
	buf := [ScoreSize + 1]byte{}
	for {
		if _, err := f.Read(buf[:]); err == io.EOF {
			break
		}
		var score Score
		copy(score[:], buf[:ScoreSize])
		len := int(buf[ScoreSize])
		idx[score] = addr{len, pos + ScoreSize + 1}
		pos += ScoreSize + 1 + len
		_, err := f.Seek(int64(len), 1)
		if err != nil {
			break
		}
	}
	return idx
}

// Read a data for a given score
func (s *Store) Read(score Score) (b []byte, err error) {
	addr, ok := s.idx[score]
	if !ok {
		err = fmt.Errorf("Unknown score %x", score)
		return
	}
	len, pos := addr[0], addr[1]
	_, err = s.File.Seek(int64(pos), 0)
	if err != nil {
		return
	}
	b = make([]byte, len)
	_, err = s.File.Read(b)
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score Score, err error) {
	score = md5.Sum(b)
	if _, ok := s.idx[score]; ok {
		return
	}
	len := len(b)
	pos := s.eof + ScoreSize + 1
	blockSize := ScoreSize + 1 + len
	buf := make([]byte, 0, blockSize)
	buf = append(buf, score[:]...)
	buf = append(buf, byte(len))
	buf = append(buf, b...)
	_, err = s.File.Write(buf)
	if err != nil {
		return
	}
	s.idx[score] = addr{len, pos}
	s.eof += blockSize
	return
}

// Delete provided storage
func (s *Store) Delete() error {
	s.Close()
	return os.Remove(s.Name())
}
