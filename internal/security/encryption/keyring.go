package encryption

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// versionTagSize is the number of bytes used to encode the key version
// at the start of each ciphertext produced by the Keyring.
const versionTagSize = 4

// Keyring manages multiple encryption keys, supporting key rotation by
// encrypting with the current (latest) key and decrypting with any
// historical key. Each ciphertext is prefixed with a 4-byte version tag
// so that the correct key can be selected during decryption.
type Keyring struct {
	mu         sync.RWMutex
	keys       map[int]cipher.AEAD // version -> AEAD
	rawKeys    map[int][]byte      // version -> derived key bytes
	currentVer int                 // highest version number = current key
}

// NewKeyring creates an empty Keyring. Add keys with AddKey before use.
func NewKeyring() *Keyring {
	return &Keyring{
		keys:    make(map[int]cipher.AEAD),
		rawKeys: make(map[int][]byte),
	}
}

// AddKey derives an AES-256-GCM key from the provided key material using HKDF
// and registers it under the given version number. If the version is higher
// than any existing version, it becomes the current encryption key.
func (kr *Keyring) AddKey(version int, key string) error {
	if version < 1 {
		return errors.New("key version must be >= 1")
	}
	if key == "" {
		return errors.New("key material must not be empty")
	}

	derivedKey, err := deriveKey([]byte(key))
	if err != nil {
		return fmt.Errorf("deriving key for version %d: %w", version, err)
	}

	gcm, err := newGCM(derivedKey)
	if err != nil {
		return fmt.Errorf("creating cipher for version %d: %w", version, err)
	}

	kr.mu.Lock()
	defer kr.mu.Unlock()

	kr.keys[version] = gcm
	kr.rawKeys[version] = derivedKey

	if version > kr.currentVer {
		kr.currentVer = version
	}

	return nil
}

// Encrypt encrypts plaintext using the current (highest-version) key.
// The output format is: [4-byte version tag][nonce][ciphertext+tag].
func (kr *Keyring) Encrypt(plaintext []byte) ([]byte, error) {
	kr.mu.RLock()
	defer kr.mu.RUnlock()

	if kr.currentVer == 0 {
		return nil, errors.New("keyring has no keys")
	}

	gcm, exists := kr.keys[kr.currentVer]
	if !exists {
		return nil, fmt.Errorf("current key version %d not found", kr.currentVer)
	}

	// Allocate output: version tag + nonce + ciphertext.
	nonce := make([]byte, gcm.NonceSize())
	if _, err := readCryptoRand(nonce); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}

	// Build version tag.
	tag := make([]byte, versionTagSize)
	binary.BigEndian.PutUint32(tag, uint32(kr.currentVer))

	// Seal: nonce is prepended to ciphertext by using it as the dst prefix.
	sealed := gcm.Seal(nonce, nonce, plaintext, tag)

	// Final format: [version_tag][nonce + ciphertext]
	result := make([]byte, 0, versionTagSize+len(sealed))
	result = append(result, tag...)
	result = append(result, sealed...)

	return result, nil
}

// Decrypt reads the version tag from the ciphertext, selects the
// appropriate key, and decrypts the payload.
func (kr *Keyring) Decrypt(ciphertext []byte) ([]byte, error) {
	kr.mu.RLock()
	defer kr.mu.RUnlock()

	if len(ciphertext) < versionTagSize {
		return nil, errors.New("ciphertext too short to contain version tag")
	}

	versionTag := ciphertext[:versionTagSize]
	version := int(binary.BigEndian.Uint32(versionTag))

	gcm, exists := kr.keys[version]
	if !exists {
		return nil, fmt.Errorf("no key found for version %d", version)
	}

	payload := ciphertext[versionTagSize:]
	nonceSize := gcm.NonceSize()
	if len(payload) < nonceSize+gcm.Overhead() {
		return nil, errors.New("ciphertext too short")
	}

	nonce := payload[:nonceSize]
	data := payload[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, data, versionTag)
	if err != nil {
		return nil, fmt.Errorf("decryption with key version %d failed: %w", version, err)
	}

	return plaintext, nil
}

// ListKeyVersions returns all registered key version numbers in ascending order.
func (kr *Keyring) ListKeyVersions() []int {
	kr.mu.RLock()
	defer kr.mu.RUnlock()

	versions := make([]int, 0, len(kr.keys))
	for v := range kr.keys {
		versions = append(versions, v)
	}
	sort.Ints(versions)
	return versions
}

// CurrentVersion returns the version number of the current encryption key.
func (kr *Keyring) CurrentVersion() int {
	kr.mu.RLock()
	defer kr.mu.RUnlock()
	return kr.currentVer
}

// RemoveKey removes a key version from the keyring. The current key cannot
// be removed. Returns an error if the version does not exist or is current.
func (kr *Keyring) RemoveKey(version int) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if version == kr.currentVer {
		return errors.New("cannot remove the current encryption key")
	}

	if _, exists := kr.keys[version]; !exists {
		return fmt.Errorf("key version %d not found", version)
	}

	delete(kr.keys, version)
	delete(kr.rawKeys, version)
	return nil
}

// readCryptoRand fills the byte slice with cryptographically secure random bytes.
// This is a package-level function to allow the crypto/rand dependency to be
// imported once and shared.
var readCryptoRand = cryptoRandRead

func cryptoRandRead(b []byte) (int, error) {
	// Import moved to package level via init to avoid circular issues.
	// crypto/rand.Read is used directly.
	return randReader.Read(b)
}

// randReader is the cryptographic random source. Set at package init to
// crypto/rand.Reader for production use.
var randReader = cryptoRandReaderInit()

func cryptoRandReaderInit() interface{ Read([]byte) (int, error) } {
	// We return crypto/rand.Reader wrapped in a concrete interface.
	return cryptoRandWrapper{}
}

type cryptoRandWrapper struct{}

func (cryptoRandWrapper) Read(b []byte) (int, error) {
	return rand.Read(b)
}
