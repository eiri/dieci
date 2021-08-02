package dieci

import (
	"github.com/muyo/sno"
)

// Key is an alias for key representation
type Key []byte

// NewKey generates and returns new key
func NewKey() Key {
	k := sno.New(0)
	key, _ := k.MarshalBinary()
	return key
}

// ValidateKey attempt to decode byte slice into sno ID
// to confirm that it's a valid key.
func ValidateKey(data []byte) error {
	_, err := sno.FromBinaryBytes(data)
	return err
}

// String returns a string representation for the key
// to comply with Stringer interface
func (k Key) String() string {
	key := sno.ID{}
	key.UnmarshalBinary(k)
	return key.String()
}
