package dieci

import (
	"testing"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
)

// TestIndex for compliance to Indexer
func TestIndex(t *testing.T) {
	assert := require.New(t)

	values := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
		[]byte("delta"),
		[]byte("echo"),
		[]byte("foxtrot"),
		[]byte("golf"),
		[]byte("hotel"),
	}

	keys := make([]Key, 2*len(values))

	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(err)
	defer db.Close()

	t.Run("write", func(t *testing.T) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		b := NewBadgerBackend(txn)
		idx := NewIndex(b)
		for i, value := range values {
			score := NewScore(value)
			key1, err := idx.Write(score)
			assert.NoError(err)
			keys[i] = key1
			// test new key on same score
			key2, err := idx.Write(score)
			assert.NoError(err)
			assert.NotEqual(key1, key2, "Should return different keys")
			keys[i+len(values)] = key2
		}
		err = txn.Commit()
		assert.NoError(err)
	})

	t.Run("read", func(t *testing.T) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		b := NewBadgerBackend(txn)
		idx := NewIndex(b)
		for i, value := range values {
			expectedScore := NewScore(value)
			score1, err := idx.Read(keys[i])
			assert.NoError(err)
			assert.Equal(expectedScore, score1)
			// second read from cache
			score2, err := idx.Read(keys[i])
			assert.NoError(err)
			assert.Equal(expectedScore, score2)
			// read from double entry
			score3, err := idx.Read(keys[i+len(values)])
			assert.NoError(err)
			assert.Equal(expectedScore, score3)
		}
	})
}
