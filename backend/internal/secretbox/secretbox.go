// Package secretbox provides authenticated symmetric encryption (AES-256-GCM)
// for secrets that must be recoverable, such as user-provided S3 credentials the
// API has to sign presign requests with. Unlike node tokens and API keys (which
// are one-way SHA-256 hashes), these secrets are decrypted on use, so they are
// encrypted at rest with a master key from the STORAGE_CRED_KEY environment.
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// Cipher encrypts and decrypts secrets with a fixed AES-256-GCM key.
type Cipher struct {
	aead cipher.AEAD
}

// NewFromBase64Key builds a Cipher from a base64 (standard encoding) 32-byte key.
// Generate one with: openssl rand -base64 32
func NewFromBase64Key(b64 string) (*Cipher, error) {
	key, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("decode storage credential key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("storage credential key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

// Encrypt returns nonce||ciphertext, suitable for storage in a BYTEA column.
func (c *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("read nonce: %w", err)
	}
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt reverses Encrypt.
func (c *Cipher) Decrypt(sealed []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(sealed) < ns {
		return nil, errors.New("ciphertext too short")
	}
	nonce, ciphertext := sealed[:ns], sealed[ns:]
	out, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return out, nil
}
