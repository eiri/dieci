// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/md5"
	"crypto/rand"
	_ "encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// ScoreSize is the size of score in bytes
const ScoreSize = 16

// Score is type alias for score representation
type Score [ScoreSize]byte

func (s Score) String() string {
	return hex.EncodeToString(s[:])
}

// DataFile is a handler of data file
type DataFile struct {
	eof int
	*os.File
}

// addr is index's address type alias
type addr [2]int

// index is index type alias
type index map[Score]addr

// Store represents a data store.
type Store struct {
	idx  index
	data DataFile
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
	idx, err := buildIndex(f)
	if err != nil {
		return
	}
	// reset fd and make Store
	_, err = f.Seek(0, 0)
	data := DataFile{0, f}
	s = &Store{idx: idx, data: data}
	return
}

func buildIndex(f *os.File) (idx index, err error) {
	var pos int
	idx = make(index)
	len := make([]byte, 1)
	for {
		if _, err := f.Read(len); err == io.EOF {
			break
		}
		buf := make([]byte, len[0])
		if _, err := f.Read(buf); err == io.EOF {
			break
		}
		score := makeScore(buf)
		idx[score] = addr{pos + 1, int(len[0])}
		pos += int(len[0]) + 1
	}
	return
}

func makeScore(b []byte) Score {
	score := md5.Sum(b)
	return score
}

// Name returns name of a store
func (s *Store) Name() string {
	return s.data.Name()
}

// MakeScore generated a score for a given data
func (s *Store) MakeScore(b []byte) Score {
	return makeScore(b)
}

// Read a data for a given score
func (s *Store) Read(score Score) (b []byte, err error) {
	addr, ok := s.idx[score]
	if !ok {
		err = fmt.Errorf("Unknown score %s", score)
		return
	}
	pos, len := addr[0], addr[1]
	_, err = s.data.Seek(int64(pos), 0)
	if err != nil {
		return
	}
	b = make([]byte, len)
	_, err = s.data.Read(b)
	if err != nil {
		return
	}
	if score != s.MakeScore(b) {
		b = []byte{}
		err = fmt.Errorf("Checksum failure")
	}
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score Score, err error) {
	score = s.MakeScore(b)
	if _, ok := s.idx[score]; ok {
		return
	}
	len := len(b)
	buf := make([]byte, 0, len+1)
	buf = append(buf, byte(len))
	buf = append(buf, b...)
	n, err := s.data.Write(buf)
	if err != nil {
		return
	}
	s.idx[score] = addr{s.data.eof + 1, n - 1}
	s.data.eof += n
	return
}

// Close provided storage
func (s *Store) Close() error {
	return s.data.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	err := s.data.Close()
	if err == nil {
		return os.Remove(s.data.Name())
	}
	return err
}
