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

	keys := make([]key, 2*len(values))

	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(err)
	defer db.Close()

	t.Run("write", func(t *testing.T) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		idx := newIndex(txn)
		for i, value := range values {
			sc := newScore(value)
			key1, err := idx.write(sc)
			assert.NoError(err)
			keys[i] = key1
			// test new key on same score
			key2, err := idx.write(sc)
			assert.NoError(err)
			assert.NotEqual(key1, key2, "Should return condifferent keys")
			keys[i+len(values)] = key2
		}
		err = txn.Commit()
		assert.NoError(err)
	})

	t.Run("read", func(t *testing.T) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		idx := newIndex(txn)
		for i, value := range values {
			expectedScore := newScore(value)
			sc1, err := idx.read(keys[i])
			assert.NoError(err)
			assert.Equal(expectedScore, sc1)
			// second read from cache
			sc2, err := idx.read(keys[i])
			assert.NoError(err)
			assert.Equal(expectedScore, sc2)
			// read from double entry
			sc3, err := idx.read(keys[i+len(values)])
			assert.NoError(err)
			assert.Equal(expectedScore, sc3)
		}
	})
}
