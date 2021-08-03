package dieci

import (
	lru "github.com/hashicorp/golang-lru"
)

// cache is in memory lookup store
type cache struct {
	lru *lru.TwoQueueCache
}

func newCache(size int) *cache {
	l, _ := lru.New2Q(size)
	return &cache{lru: l}
}

func (c *cache) Add(key Key, score Score) {
	cacheKey := key.String()
	c.lru.Add(cacheKey, score)
}

func (c *cache) Get(key Key) (score Score, ok bool) {
	cacheKey := key.String()
	value, ok := c.lru.Get(cacheKey)
	if ok {
		score = value.(Score)
	}
	return score, ok
}

// Index represents an index of a datalog file
type Index struct {
	cache   *cache
	backend Backend
}

// NewIndex returns a new index
func NewIndex(b Backend) *Index {
	cache := newCache(20000)
	return &Index{cache: cache, backend: b}
}

// Read is a read callback
func (idx *Index) Read(key Key) (Score, error) {
	if score, ok := idx.cache.Get(key); ok {
		return score, nil
	}

	score, err := idx.backend.Read(key)
	if err == nil {
		idx.cache.Add(key, score)
	}
	return score, err
}

// Write is a write callback
func (idx *Index) Write(score Score) (Key, error) {
	key := NewKey()
	err := idx.backend.Write(key, score)
	if err != nil {
		return Key{}, err
	}
	idx.cache.Add(key, score)
	return key, nil
}
