// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"encoding/binary"
	"fmt"
	"os"
)

type datalogger interface {
	get(p, l int) ([]byte, error)
	put([]byte) (int, int, error)
	close() error
	delete() error
}

// datalog holds, in sequential order, the contents of every written block
type datalog struct {
	cur int
	*os.File
}

// openIndex opens the named index and loads its content in the memory cache
func openDataLog(name string) (d *datalog, err error) {
	fileName := fmt.Sprintf("%s.data", name)
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	i, err := f.Stat()
	if err != nil {
		return
	}
	d = &datalog{int(i.Size()), f}
	return
}

// get returns a data block of the given length read from the given position
func (d *datalog) get(pos, len int) ([]byte, error) {
	b := make([]byte, len)
	n, err := d.ReadAt(b, int64(pos))
	if err != nil {
		return b, err
	}
	if n != len {
		err = fmt.Errorf("Read failed")
	}
	return b, err
}

// put stores the given data block and returns its length and position
func (d *datalog) put(b []byte) (p, l int, err error) {
	l = len(b)
	bufSize := IntSize + l
	buf := make([]byte, bufSize, bufSize)
	binary.BigEndian.PutUint32(buf[0:IntSize], uint32(l))
	copy(buf[IntSize:bufSize], b)
	n, err := d.Write(buf)
	if err != nil {
		return
	}
	err = d.Sync()
	if err != nil {
		return
	}
	p = d.cur + IntSize
	l = n - IntSize
	d.cur += n
	return
}

// close releases cache and closes an index file handler
func (d *datalog) close() error {
	d.cur = 0
	return d.Close()
}

// delete releases cache and closes and erases an index file
func (d *datalog) delete() error {
	err := d.close()
	if err != nil {
		return err
	}
	return os.Remove(d.Name())
}
