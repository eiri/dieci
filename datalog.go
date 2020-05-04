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
	reader io.ReaderAt
	writer io.Writer
	err    error
}

// NewDatalog returns a new datalog with the given name
func NewDatalog(r io.ReaderAt, w io.Writer) *Datalog {
	return &Datalog{reader: r, writer: w}
}

// ReadAt is an implementation of ReaderAt interface
func (dl *Datalog) ReadAt(data []byte, off int64) (int, error) {
	n, err := dl.reader.ReadAt(data, off)
	if err != nil {
		return 0, err
	}

	size := int(binary.BigEndian.Uint32(data[:intSize]))
	if intSize+size != n {
		err := fmt.Errorf("datalog: invalid read length")
		return 0, err
	}

	// FIXME maybe push on dieci side to make checksum an optional
	score, stored := dl.Deserialize(data)
	check := MakeScore(stored)
	if score != check {
		err := fmt.Errorf("datalog: invalid checksum")
		return 0, err
	}

	return n, nil
}

// Write is an implementation of Writer interface
func (dl *Datalog) Write(data []byte) (int, error) {
	if dl.err != nil {
		return 0, dl.err
	}
	score := MakeScore(data)
	buf := dl.Serialize(score, data)
	n, err := dl.writer.Write(buf)
	if err != nil {
		dl.err = err
	}
	return n, err
}

// Serialize score and its data to storable block
func (dl *Datalog) Serialize(score Score, data []byte) []byte {
	size := scoreSize + len(data)
	buf := make([]byte, intSize+size)
	binary.BigEndian.PutUint32(buf, uint32(size))
	copy(buf[intSize:], score[:])
	copy(buf[intSize+scoreSize:], data)
	return buf
}

// Deserialize read block to score and its data
func (dl *Datalog) Deserialize(data []byte) (Score, []byte) {
	score := Score{}
	copy(score[:], data[intSize:])
	return score, data[intSize+scoreSize:]
}
