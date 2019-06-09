package dieci

import (
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
	reader io.ReaderAt
	writer io.Writer
}

// NewDatalog returns a new datalog with the given name
func NewDatalog(r io.ReaderAt, w io.Writer, idx *Index) *Datalog {
	return &Datalog{reader: r, writer: w, index: idx}
}

// Read reads data for a given position and length
func (d *Datalog) Read(score Score) ([]byte, error) {
	a, ok := d.index.Read(score)
	if !ok {
		err := fmt.Errorf("Unknown score %s", score)
		return nil, err
	}
	block := make([]byte, a.size)
	if _, err := d.reader.ReadAt(block, int64(a.pos)); err != nil {
		return nil, err
	}
	_, data := d.Decode(block)
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

// Encode score and data into slice of bytes
func (d *Datalog) Encode(score Score, data []byte) []byte {
	size := scoreSize + len(data)
	buf := make([]byte, intSize+size)
	binary.BigEndian.PutUint32(buf, uint32(size))
	copy(buf[intSize:], score[:])
	copy(buf[intSize+scoreSize:], data)
	return buf
}

// Decode score and data from given slice of bytes
func (d *Datalog) Decode(block []byte) (score Score, data []byte) {
	copy(score[:], block[:scoreSize])
	return score, block[scoreSize:]
}
