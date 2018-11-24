// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

// ScoreSize is the size of score in bytes
const ScoreSize = 16

// IntSize is the binary size of integer we write on disk
const IntSize = 4

// Score is type alias for score representation
type Score [ScoreSize]byte

func (s Score) String() string {
	return hex.EncodeToString(s[:])
}

// Store represents a data store.
type Store struct {
	name  string
	eof   int
	data  *os.File
	index indexer
}

// New creates a new empty storage
func New() (s *Store, err error) {
	buf := make([]byte, 16)
	_, err = rand.Read(buf)
	if err != nil {
		return
	}
	storeName := hex.EncodeToString(buf)
	dataFileName := fmt.Sprintf("%s.data", storeName)
	f, err := os.OpenFile(dataFileName, os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return
	}
	f.Close()
	return Open(storeName)
}

// Open opens provided storage
func Open(storeName string) (s *Store, err error) {
	dataFileName := fmt.Sprintf("%s.data", storeName)
	dfh, err := os.OpenFile(dataFileName, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	i, err := dfh.Stat()
	if err != nil {
		return
	}
	idx, err := openIndex(storeName)
	if err != nil {
		return
	}
	s = &Store{
		name:  storeName,
		eof:   int(i.Size()),
		data:  dfh,
		index: idx,
	}
	return
}

func makeScore(b []byte) Score {
	score := md5.Sum(b)
	return score
}

// Name returns name of a store
func (s *Store) Name() string {
	return s.name
}

// MakeScore generated a score for a given data
func (s *Store) MakeScore(b []byte) Score {
	return makeScore(b)
}

// Read a data for a given score
func (s *Store) Read(score Score) (b []byte, err error) {
	pos, len, ok := s.index.get(score)
	if !ok {
		err = fmt.Errorf("Unknown score %s", score)
		return
	}
	b = make([]byte, len)
	_, err = s.data.ReadAt(b, int64(pos))
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
	if _, _, ok := s.index.get(score); ok {
		return
	}
	len := len(b)
	buf := make([]byte, len+IntSize, len+IntSize)
	binary.BigEndian.PutUint32(buf[:IntSize], uint32(len))
	copy(buf[IntSize:], b)
	n, err := s.data.Write(buf)
	if err != nil {
		return
	}
	err = s.data.Sync()
	if err != nil {
		return
	}
	newPos := s.eof + IntSize
	newLen := n - IntSize
	err = s.index.put(score, newPos, newLen)
	if err != nil {
		return
	}
	s.eof += n
	return
}

// Close provided storage
func (s *Store) Close() error {
	err := s.index.close()
	if err != nil {
		return err
	}
	return s.data.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	if err := s.index.delete(); err != nil {
		return err
	}
	dataFileName := fmt.Sprintf("%s.data", s.name)
	return os.Remove(dataFileName)
}
