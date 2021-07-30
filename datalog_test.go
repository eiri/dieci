package dieci

import (
	"testing"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
)

// TestDataLog for compliance to Datalogger
func TestDataLog(t *testing.T) {
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

	scores := make([]score, len(values))

	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	assert.NoError(err)
	defer db.Close()

	t.Run("write", func(t *testing.T) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		b := NewBadgerBackend(txn)
		dl := NewDatalog(b)
		for i, value := range values {
			score1, err := dl.write(value)
			assert.NoError(err)
			scores[i] = score1
			// test deduplication
			score2, err := dl.write(value)
			assert.NoError(err)
			assert.Equal(score1, score2, "Should return consistent score")
		}
		err = txn.Commit()
		assert.NoError(err)
	})

	t.Run("read", func(t *testing.T) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		b := NewBadgerBackend(txn)
		dl := NewDatalog(b)
		for i, s := range scores {
			value, err := dl.read(s)
			assert.NoError(err)
			assert.Equal(values[i], value)
		}
	})
}
