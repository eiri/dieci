package dieci

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	intSize = 4
)

// Datalog represents a datalog file
type Datalog struct {
	index  *Index
	reader ReadReaderAt
	writer io.Writer
}

type ReadReaderAt interface {
	io.Reader
	io.ReaderAt
}

// NewDatalog returns a new datalog with the given name
func NewDatalog(r ReadReaderAt, w io.Writer) *Datalog {
	return &Datalog{reader: r, writer: w}
}

// Open opens the named datalog
func (d *Datalog) Open(irw io.ReadWriter) error {
	idx, err := NewIndex(irw)
	if err != nil {
		return err
	}
	d.index = idx
	if d.index.Len() == 0 {
		err = d.RebuildIndex()
	}
	return err
}

// RebuildIndex by scaning datalog and writing cache again
func (d *Datalog) RebuildIndex() error {
	scanner := bufio.NewScanner(d.reader)
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
	offset := 0
	for scanner.Scan() {
		block := scanner.Bytes()
		size := int(binary.BigEndian.Uint32(block[:intSize]))
		var score Score
		copy(score[:], block[intSize:])
		addr := Addr{pos: offset + intSize, size: size}
		err = d.index.Write(score, addr)
		if err != nil {
			break
		}
		offset += intSize + size
	}
	if err == nil {
		err = scanner.Err()
	}
	return err
}

// Read reads data for a given position and length
func (d *Datalog) Read(score Score) ([]byte, error) {
	a, ok := d.index.Read(score)
	if !ok {
		err := fmt.Errorf("Unknown score %s", score)
		return nil, err
	}
	data := make([]byte, a.size-scoreSize)
	n, err := d.reader.ReadAt(data, int64(a.pos+scoreSize))
	if err != nil {
		return nil, err
	}
	if n != a.size-scoreSize {
		return nil, fmt.Errorf("Read failed")
	}
	return data, nil
}

// Write writes given data into datalog and returns it's position and length
func (d *Datalog) Write(data []byte) (Score, error) {
	score := MakeScore(data)
	if _, ok := d.index.Read(score); ok {
		return score, nil
	}
	buf := d.Encode(score, data)
	n, err := d.writer.Write(buf)
	if err != nil {
		return Score{}, err
	}
	pos := d.index.Cur() + intSize
	size := n - intSize
	err = d.index.Write(score, Addr{pos: pos, size: size})
	if err != nil {
		return Score{}, err
	}
	return score, nil
}

// Encode data and its score into slice of bytes suitable to write on disk
func (d *Datalog) Encode(score Score, data []byte) []byte {
	size := scoreSize + len(data)
	buf := make([]byte, intSize+size)
	binary.BigEndian.PutUint32(buf, uint32(size))
	copy(buf[intSize:], score[:])
	copy(buf[intSize+scoreSize:], data)
	return buf
}
