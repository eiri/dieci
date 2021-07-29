package dieci

import (
	"encoding/hex"

	"github.com/muyo/sno"
)

// key is an alias for key representaion
type key []byte

func newKey() key {
	return sno.New(0).Bytes()
}

func (k key) String() string {
	return hex.EncodeToString(k)
}
