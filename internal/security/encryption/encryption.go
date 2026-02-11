// Package encryption provides field-level encryption using AES-256-GCM with
// HKDF key derivation, suitable for encrypting sensitive fields before storage.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/crypto/hkdf"
)

// hkdfInfo is the context string used in HKDF key derivation to bind the
// derived key to the FlowForge encryption purpose.
var hkdfInfo = []byte("flowforge-field-encryption-v1")

// hkdfSalt is the default salt for HKDF key derivation. Override via
// SetHKDFSalt during service initialization if a custom salt is needed.
var hkdfSalt = []byte("flowforge-hkdf-salt-v1")

// SetHKDFSalt overrides the default HKDF salt. Must be called before any
// Encryptor is created. Typically set from FLOWFORGE_HKDF_SALT env var.
func SetHKDFSalt(salt []byte) {
	if len(salt) > 0 {
		hkdfSalt = salt
	}
}

// Encryptor provides AES-256-GCM encryption and decryption with HKDF-derived keys.
// It is safe for concurrent use.
type Encryptor struct {
	mu  sync.RWMutex
	gcm cipher.AEAD
	key []byte // raw derived 256-bit key, kept for re-keying
}

// NewEncryptor creates an Encryptor by deriving an AES-256-GCM key from the
// provided key material using HKDF-SHA256.
func NewEncryptor(key string) (*Encryptor, error) {
	if key == "" {
		return nil, errors.New("encryption key must not be empty")
	}

	derivedKey, err := deriveKey([]byte(key))
	if err != nil {
		return nil, err
	}

	gcm, err := newGCM(derivedKey)
	if err != nil {
		return nil, err
	}

	return &Encryptor{
		gcm: gcm,
		key: derivedKey,
	}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM with a random nonce.
// The returned ciphertext has the nonce prepended:  [nonce || ciphertext || tag].
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}

	ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts ciphertext produced by Encrypt. It extracts the nonce
// from the first bytes of the ciphertext.
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return decryptWithGCM(e.gcm, ciphertext)
}

// EncryptJSON marshals the data to JSON and encrypts it.
func (e *Encryptor) EncryptJSON(data interface{}) ([]byte, error) {
	plaintext, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshaling data for encryption: %w", err)
	}
	return e.Encrypt(plaintext)
}

// DecryptJSON decrypts the ciphertext and unmarshals the JSON into the target.
func (e *Encryptor) DecryptJSON(ciphertext []byte, target interface{}) error {
	plaintext, err := e.Decrypt(ciphertext)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(plaintext, target); err != nil {
		return fmt.Errorf("unmarshaling decrypted data: %w", err)
	}
	return nil
}

// RotateKey re-initialises the encryptor with a new key. Existing ciphertexts
// encrypted with the old key will no longer be decryptable by this encryptor.
// Use the Keyring for multi-key decryption during key rotation windows.
func (e *Encryptor) RotateKey(newKey string) error {
	if newKey == "" {
		return errors.New("new encryption key must not be empty")
	}

	derivedKey, err := deriveKey([]byte(newKey))
	if err != nil {
		return err
	}

	gcm, err := newGCM(derivedKey)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.gcm = gcm
	e.key = derivedKey
	return nil
}

// deriveKey uses HKDF-SHA256 to derive a 256-bit key from arbitrary key material.
func deriveKey(keyMaterial []byte) ([]byte, error) {
	reader := hkdf.New(sha256.New, keyMaterial, hkdfSalt, hkdfInfo)
	derivedKey := make([]byte, 32) // 256 bits
	if _, err := io.ReadFull(reader, derivedKey); err != nil {
		return nil, fmt.Errorf("deriving key with HKDF: %w", err)
	}
	return derivedKey, nil
}

// newGCM creates an AES-256-GCM AEAD from a 256-bit key.
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}
	return gcm, nil
}

// decryptWithGCM decrypts nonce-prepended AES-GCM ciphertext.
func decryptWithGCM(gcm cipher.AEAD, ciphertext []byte) ([]byte, error) {
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize+gcm.Overhead() {
		return nil, errors.New("ciphertext too short")
	}
	nonce := ciphertext[:nonceSize]
	data := ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}
	return plaintext, nil
}
