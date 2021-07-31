package dieci

import (
	art "github.com/plar/go-adaptive-radix-tree"
)

// cache is in memory lookup store
type cache art.Tree

// Index represents an index of a datalog file
type Index struct {
	cache   cache
	backend Backend
}

// NewIndex returns a new index
func NewIndex(b Backend) *Index {
	cache := art.New()
	return &Index{cache: cache, backend: b}
}

// Read is a read callback
func (idx *Index) Read(k key) (Score, error) {
	if score, ok := idx.cache.Search(art.Key(k)); ok {
		return score.([]byte), nil
	}

	score, err := idx.backend.Read(k)
	if err == nil {
		idx.cache.Insert(art.Key(k), score)
	}
	return score, err
}

// Write is a write callback
func (idx *Index) Write(score Score) (key, error) {
	k := newKey()
	err := idx.backend.Write(k, score)
	if err != nil {
		return key{}, err
	}
	idx.cache.Insert(art.Key(k), score)
	return k, nil
}
