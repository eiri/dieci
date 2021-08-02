package dieci

import (
	"github.com/bits-and-blooms/bloom/v3"
)

// Datalog represents a datastore's datalog
type Datalog struct {
	filter  *bloom.BloomFilter
	backend Backend
}

// NewDatalog returns a new datalog for a given transaction
func NewDatalog(b Backend) *Datalog {
	f := bloom.New(20000, 5)
	return &Datalog{filter: f, backend: b}
}

// Read is a read callback
func (dl *Datalog) Read(score Score) ([]byte, error) {
	data, err := dl.backend.Read(score)
	if err == nil && !dl.filter.Test(score) {
		dl.filter.Add(score)
	}
	return data, err
}

// Write is a write callback
func (dl *Datalog) Write(data []byte) (Score, error) {
	score := NewScore(data)
	// if score is not in filter it's definitely not in backend
	// if it is in filer then it's maybe in backend,
	// so worth of read to confirm
	if dl.filter.Test(score) {
		if ok, err := dl.backend.Exists(score); ok {
			return score, nil
		} else if err != nil {
			return Score{}, err
		}
	}
	// this might be an idempotent update if this is a fresh start
	// and score is not yet in filter
	err := dl.backend.Write(score, data)
	if err != nil {
		return Score{}, err
	}

	dl.filter.Add(score)
	return score, nil
}
