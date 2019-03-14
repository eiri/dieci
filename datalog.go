package dieci

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Datalogger is the interface for Datalog
type Datalogger interface {
	Open() error
	Name() string
	Read(pos, size int) ([]byte, error)
	Write(score Score, data []byte) (pos, size int, err error)
	Close() error
}

// Datalog represents a datalog file
type Datalog struct {
	name string
	cur  int
	rwc  *os.File
}

// NewDatalog returns a new datalog with the given name
func NewDatalog(name string) *Datalog {
	return &Datalog{name: name, cur: 0}
}

// Open opens the named datalog
func (d *Datalog) Open() error {
	fileName := fmt.Sprintf("%s.data", d.name)
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	d.rwc = f
	d.cur = int(stat.Size())
	return nil
}

// Name returns name of datalog file
func (d *Datalog) Name() string {
	return d.rwc.Name()
}

// Read reads data for a given position and length
func (d *Datalog) Read(pos, size int) ([]byte, error) {
	data := make([]byte, size-scoreSize)
	n, err := d.rwc.ReadAt(data, int64(pos+scoreSize))
	if err != nil {
		return nil, err
	}
	if n != size-scoreSize {
		return nil, fmt.Errorf("Read failed")
	}
	return data, nil
}

// Write writes given data into datalog and returns it's position and length
func (d *Datalog) Write(score Score, data []byte) (pos, size int, err error) {
	size = len(data) + scoreSize
	buf := make([]byte, intSize+size)
	binary.BigEndian.PutUint32(buf, uint32(size))
	copy(buf[intSize:], score[:])
	copy(buf[intSize+scoreSize:], data)
	n, err := d.rwc.Write(buf)
	if err != nil {
		return 0, 0, err
	}
	pos = int(d.cur) + intSize
	size = n - intSize
	d.cur += n
	return
}

// Close closes the datalog
func (d *Datalog) Close() error {
	d.cur = 0
	return d.rwc.Close()
}
