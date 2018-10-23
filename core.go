// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"os"
)

// ScoreSize is the size of score in bytes
const ScoreSize = 16

// Store represents a data store.
type Store struct {
	idx map[[ScoreSize]byte][2]int
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
	// read idx
	idx := make(map[[ScoreSize]byte][2]int)
	pos := 0
	buf := [ScoreSize + 1]byte{}
	for {
		if _, err := f.Read(buf[:]); err == io.EOF {
			break
		}
		var score [ScoreSize]byte
		copy(score[:], buf[:ScoreSize])
		len := int(buf[ScoreSize])
		idx[score] = [2]int{len, pos + ScoreSize + 1}
		pos += ScoreSize + 1 + len
		_, err = f.Seek(int64(len), 1)
		if err != nil {
			break
		}
	}
	// reset fd and make Store
	_, err = f.Seek(0, 0)
	s = &Store{idx, 0, f}
	return
}

// Read a data for a given score
func (s *Store) Read(score [ScoreSize]byte) (b []byte, err error) {
	val, ok := s.idx[score]
	if !ok {
		err = fmt.Errorf("Unknown score %x", score)
		return
	}
	len := val[0]
	pos := val[1]
	_, err = s.File.Seek(int64(pos), 0)
	if err != nil {
		return
	}
	b = make([]byte, len)
	_, err = s.File.Read(b)
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score [ScoreSize]byte, err error) {
	score = md5.Sum(b)
	if _, ok := s.idx[score]; ok {
		return
	}
	len := len(b)
	// use buffer for now
	buf := new(bytes.Buffer)
	buf.Write(score[:])
	buf.WriteByte(byte(len))
	buf.Write(b)
	_, err = buf.WriteTo(s.File)
	if err != nil {
		return
	}
	s.idx[score] = [2]int{len, s.eof + ScoreSize + 1}
	s.eof += ScoreSize + 1 + len
	return
}

// Delete provided storage
func (s *Store) Delete() error {
	s.Close()
	return os.Remove(s.Name())
}
