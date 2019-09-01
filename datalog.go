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
	writer := NewWriter(w)
	return &Datalog{reader: r, writer: writer, index: idx}
}

// Get reads data for a given position and length
func (d *Datalog) Get(score Score) ([]byte, error) {
	a, ok := d.index.Read(score)
	if !ok {
		err := fmt.Errorf("datalog: unknown score %s", score)
		return nil, err
	}
	block := make([]byte, a.size)
	if _, err := d.reader.ReadAt(block, int64(a.pos)); err != nil {
		return nil, err
	}
	score2 := Score{}
	copy(score2[:], block[intSize:])
	if score != score2 {
		err := fmt.Errorf("datalog: invalid checksum")
		return nil, err
	}
	data := make([]byte, a.size-intSize-scoreSize)
	copy(data[:], block[intSize+scoreSize:])
	return data, nil
}

// Put writes given data into datalog and returns it's position and length
func (d *Datalog) Put(data []byte) (Score, error) {
	score := MakeScore(data)
	if _, ok := d.index.Read(score); ok {
		return score, nil
	}
	size, err := d.writer.Write(data)
	if err != nil {
		return Score{}, err
	}
	d.index.Write(score, size)
	return score, nil
}

// Writer is io.Writer
// Writes to a Writer are encoded and written to w
type Writer struct {
	w     io.Writer
	score Score
	err   error
}

// NewWriter returns a new Writer
func NewWriter(w io.Writer) *Writer {
	dw := new(Writer)
	*dw = Writer{w: w}
	return dw
}

// Write is an implementation of Writer interface
func (dw *Writer) Write(data []byte) (int, error) {
	if dw.err != nil {
		return 0, dw.err
	}
	var n int
	dw.score = MakeScore(data)
	size := scoreSize + len(data)
	buf := make([]byte, intSize+size)
	binary.BigEndian.PutUint32(buf, uint32(size))
	copy(buf[intSize:], dw.score[:])
	copy(buf[intSize+scoreSize:], data)
	n, dw.err = dw.w.Write(buf)
	return n, dw.err
}
