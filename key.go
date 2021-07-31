package dieci

import (
	"encoding/hex"

	"github.com/muyo/sno"
)

// Key is an alias for key representaion
type Key []byte

// NewKey generates and returns new key
func NewKey() Key {
	return sno.New(0).Bytes()
}

// String returns a string representation for the key
// to comply with Stringer interface
func (k Key) String() string {
	return hex.EncodeToString(k)
}
