package secretbox

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func newTestCipher(t *testing.T) *Cipher {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("read key: %v", err)
	}
	c, err := NewFromBase64Key(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	return c
}

func TestEncryptDecryptRoundtrip(t *testing.T) {
	c := newTestCipher(t)
	secret := []byte("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	sealed, err := c.Encrypt(secret)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if bytes.Contains(sealed, secret) {
		t.Fatal("ciphertext leaks plaintext")
	}
	got, err := c.Decrypt(sealed)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatalf("roundtrip mismatch: got %q", got)
	}
}

func TestEncryptNonceIsRandom(t *testing.T) {
	c := newTestCipher(t)
	a, _ := c.Encrypt([]byte("same"))
	b, _ := c.Encrypt([]byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("expected distinct ciphertexts for repeated plaintext")
	}
}

func TestDecryptRejectsTampered(t *testing.T) {
	c := newTestCipher(t)
	sealed, _ := c.Encrypt([]byte("secret"))
	sealed[len(sealed)-1] ^= 0xff
	if _, err := c.Decrypt(sealed); err == nil {
		t.Fatal("expected decrypt to reject tampered ciphertext")
	}
}

func TestNewFromBase64KeyRejectsWrongLength(t *testing.T) {
	if _, err := NewFromBase64Key(base64.StdEncoding.EncodeToString([]byte("too-short"))); err == nil {
		t.Fatal("expected error for non-32-byte key")
	}
}
