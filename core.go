// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
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

// addr is index's address type alias
type addr [2]int

// index is index type alias
type index map[Score]addr

// Store represents a data store.
type Store struct {
	name string
	idx  index
	eof  int
	data *os.File
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
	idx := make(index)
	indexFileName := fmt.Sprintf("%s.idx", storeName)
	ifh, err := os.OpenFile(indexFileName, os.O_RDONLY, 0600)
	if err == nil {
		bufSize := 2*IntSize + ScoreSize
		buf := make([]byte, bufSize, bufSize)
		for {
			if _, err := ifh.Read(buf); err == io.EOF {
				break
			}
			var score Score
			pos := int(binary.BigEndian.Uint32(buf[0:IntSize]))
			len := int(binary.BigEndian.Uint32(buf[IntSize : 2*IntSize]))
			copy(score[:], buf[2*IntSize:])
			idx[score] = addr{pos, len}
		}
		ifh.Close()
	}
	flags := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	ifh, err = os.OpenFile(indexFileName, flags, 0600)
	if err != nil {
		return
	}
	if len(idx) == 0 {
		idx = buildIndex(dfh, ifh)
	}
	ifh.Close()
	s = &Store{
		name: storeName,
		idx:  idx,
		eof:  int(i.Size()),
		data: dfh,
	}
	return
}

func buildIndex(f *os.File, i *os.File) index {
	pos := IntSize
	idx := make(index)
	lenBuf := make([]byte, IntSize)
	for {
		if _, err := f.Read(lenBuf); err == io.EOF {
			break
		}
		len := int(binary.BigEndian.Uint32(lenBuf))
		buf := make([]byte, len)
		if _, err := f.Read(buf); err == io.EOF {
			break
		}
		score := makeScore(buf)
		idx[score] = addr{pos, len}
		// write to index
		iBuf := make([]byte, 2*IntSize, 2*IntSize+ScoreSize)
		binary.BigEndian.PutUint32(buf[0:IntSize], uint32(pos))
		binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], uint32(len))
		iBuf = append(iBuf, score[:]...)
		i.Write(buf)
		//
		pos += len + IntSize
	}
	return idx
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
	addr, ok := s.idx[score]
	if !ok {
		err = fmt.Errorf("Unknown score %s", score)
		return
	}
	pos, len := addr[0], addr[1]
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
	if _, ok := s.idx[score]; ok {
		return
	}
	len := len(b)
	buf := make([]byte, 4, len+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(len))
	buf = append(buf, b...)
	n, err := s.data.Write(buf)
	if err != nil {
		return
	}
	err = s.data.Sync()
	if err != nil {
		return
	}
	s.idx[score] = addr{s.eof + 4, n - 4}
	s.eof += n
	return
}

// Close provided storage
func (s *Store) Close() error {
	indexFileName := fmt.Sprintf("%s.idx", s.name)
	flags := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	if f, err := os.OpenFile(indexFileName, flags, 0600); err == nil {
		bufSize := 2*IntSize + ScoreSize
		buf := make([]byte, bufSize, bufSize)
		for score, addr := range s.idx {
			binary.BigEndian.PutUint32(buf[0:IntSize], uint32(addr[0]))
			binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], uint32(addr[1]))
			copy(buf[2*IntSize:], score[:])
			_, err := f.Write(buf)
			if err != nil {
				return err
			}
		}
		f.Close()
	}
	return s.data.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	err := s.data.Close()
	if err == nil {
		indexFileName := fmt.Sprintf("%s.idx", s.name)
		err = os.Remove(indexFileName)
	}
	if err == nil {
		dataFileName := fmt.Sprintf("%s.data", s.name)
		err = os.Remove(dataFileName)
	}
	return err
}
