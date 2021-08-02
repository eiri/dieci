package dieci

import (
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/nacl/secretbox"
)

const (
	keySize   = 32
	nonceSize = 24
)

var (
	// ErrEncrypt is a shield error for any encyption failure
	ErrEncrypt = errors.New("encryption failed")
	// ErrDecrypt is a shield error for any decyption failure
	ErrDecrypt = errors.New("decryption failed")
)

// PKI represent an interface to PKI service (e.g Valut)
type PKI struct {
	key *[keySize]byte
}

// NewPKI returns an instance of PKI interface
func NewPKI(b Backend) *PKI {
	kkey := []byte("00key")
	if ok, _ := b.Exists(kkey); !ok {
		key, _ := generateKey()
		b.Write(kkey, key[:])
	}
	data, _ := b.Read(kkey)
	var key [keySize]byte
	copy(key[:], data)
	return &PKI{key: &key}
}

// GenerateKey creates a random encryption key
func generateKey() (*[keySize]byte, error) {
	key := new([keySize]byte)
	_, err := io.ReadFull(rand.Reader, key[:])
	if err != nil {
		return nil, err
	}
	return key, nil
}

//GenerateNonce creates a new random nonce
func (pki *PKI) GenerateNonce() (*[nonceSize]byte, error) {
	nonce := new([nonceSize]byte)
	_, err := io.ReadFull(rand.Reader, nonce[:])
	if err != nil {
		return nil, err
	}
	return nonce, nil
}

// Encrypt given data with PKI's encryption key
func (pki *PKI) Encrypt(data []byte) ([]byte, error) {
	nonce, err := pki.GenerateNonce()
	if err != nil {
		return nil, ErrEncrypt
	}

	out := make([]byte, len(nonce))
	copy(out, nonce[:])
	out = secretbox.Seal(out, data, nonce, pki.key)
	return out, nil
}

// Decrypt given ciphertext with PKI's encryption key
func (pki *PKI) Decrypt(data []byte) ([]byte, error) {
	if len(data) < (nonceSize + secretbox.Overhead) {
		return nil, ErrDecrypt
	}

	var nonce [nonceSize]byte
	copy(nonce[:], data[:nonceSize])
	out, ok := secretbox.Open(nil, data[nonceSize:], &nonce, pki.key)
	if !ok {
		return nil, ErrDecrypt
	}

	return out, nil
}
