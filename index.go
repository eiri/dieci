package dieci

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/plar/go-adaptive-radix-tree"
)

const (
	intSize       = 4
	doubleIntSize = 8
	bufSize       = 24
	pageSize      = 4080 // ~4K to match memory page
)

type indexer interface {
	get(score Score) (int, int, bool)
	put(score Score, p, l int) error
	close() error
	delete() error
}

// addr is index's address type alias
type addr [2]int

// index helps to locate a block in the data log given its score.
type index struct {
	art.Tree
	*os.File
}

// openIndex opens the named index and loads its content in the memory cache
func openIndex(name string) (i *index, err error) {
	fileName := fmt.Sprintf("%s.idx", name)
	idx, err := loadIndex(fileName)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	i = &index{idx, f}
	if idx.Size() == 0 {
		err = rebuildIndex(name, i)
	}
	return
}

// load reads index file if presented into memory
func loadIndex(fileName string) (art.Tree, error) {
	idx := art.New()
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return idx, err
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
			idx.Insert(score[:], addr{int(p), int(l)})
		}
		if parseErr != nil && parseErr != io.EOF {
			err = parseErr
			break
		}
	}
	if err == io.EOF {
		return idx, nil
	}
	return idx, err
}

// rebuild reads data log and recreates index for it
// FIXME! I'll need an iterator on data log here
func rebuildIndex(name string, i *index) error {
	f, err := os.Open(fmt.Sprintf("%s.data", name))
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
		err = i.put(score, p, l)
		if err != nil {
			break
		}
		p += l + intSize
	}
	return err
}

// get returns an address for a given score if it's known
func (i *index) get(score Score) (p, l int, ok bool) {
	a, ok := i.Search(score[:])
	if !ok {
		return
	}
	address := a.(addr)
	p, l = address[0], address[1]
	return
}

// put stores a given score and address
func (i *index) put(score Score, p, l int) error {
	if _, ok := i.Search(score[:]); ok {
		return nil
	}
	buf := make([]byte, bufSize, bufSize)
	binary.BigEndian.PutUint32(buf[0:intSize], uint32(p))
	binary.BigEndian.PutUint32(buf[intSize:doubleIntSize], uint32(l))
	copy(buf[doubleIntSize:bufSize], score[:])
	_, err := i.Write(buf)
	if err != nil {
		return err
	}
	i.Insert(score[:], addr{p, l})
	return err
}

// close releases cache and closes an index file handler
func (i *index) close() error {
	i.Tree = art.New()
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
