package transform

import (
	"math"
	"sync"
	"unsafe"
)

// Deduplicator uses a bloom filter for memory-efficient, probabilistic
// duplicate detection. It is safe for concurrent use.
type Deduplicator struct {
	mu        sync.Mutex
	bits      []uint64
	numBits   uint64
	numHashes int
	count     int64
}

// NewDeduplicator creates a Deduplicator optimised for the expected item count
// with the target false positive rate. Uses murmur3-inspired hashing internally.
func NewDeduplicator(expectedCount int, falsePositiveRate float64) *Deduplicator {
	if expectedCount <= 0 {
		expectedCount = 1000
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	// Optimal bloom filter size: m = -n*ln(p) / (ln(2))^2
	n := float64(expectedCount)
	p := falsePositiveRate
	ln2 := math.Ln2
	m := -n * math.Log(p) / (ln2 * ln2)
	numBits := uint64(math.Ceil(m))

	// Optimal number of hashes: k = (m/n) * ln(2)
	k := int(math.Ceil((float64(numBits) / n) * ln2))
	if k < 1 {
		k = 1
	}

	// Round up to the next multiple of 64 for word-aligned access.
	words := (numBits + 63) / 64
	numBits = words * 64

	return &Deduplicator{
		bits:      make([]uint64, words),
		numBits:   numBits,
		numHashes: k,
	}
}

// IsDuplicate returns true if the key has likely been seen before. If it returns
// false, the key is guaranteed to be new. The key is added to the filter either way.
func (d *Deduplicator) IsDuplicate(key string) bool {
	h1, h2 := murmur3Hash(key)

	d.mu.Lock()
	defer d.mu.Unlock()

	alreadyPresent := true
	for i := 0; i < d.numHashes; i++ {
		// Double hashing: hash_i = h1 + i*h2
		pos := (h1 + uint64(i)*h2) % d.numBits
		wordIdx := pos / 64
		bitIdx := pos % 64
		mask := uint64(1) << bitIdx

		if d.bits[wordIdx]&mask == 0 {
			alreadyPresent = false
			d.bits[wordIdx] |= mask
		}
	}
	if !alreadyPresent {
		d.count++
	}
	return alreadyPresent
}

// Contains checks if the key is probably in the set without adding it.
func (d *Deduplicator) Contains(key string) bool {
	h1, h2 := murmur3Hash(key)

	d.mu.Lock()
	defer d.mu.Unlock()

	for i := 0; i < d.numHashes; i++ {
		pos := (h1 + uint64(i)*h2) % d.numBits
		wordIdx := pos / 64
		bitIdx := pos % 64
		mask := uint64(1) << bitIdx
		if d.bits[wordIdx]&mask == 0 {
			return false
		}
	}
	return true
}

// Count returns the number of unique keys added.
func (d *Deduplicator) Count() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

// Reset clears the bloom filter.
func (d *Deduplicator) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.bits {
		d.bits[i] = 0
	}
	d.count = 0
}

// murmur3Hash produces two 64-bit hashes from a string using a murmur3-inspired
// algorithm. This is not a full MurmurHash3 implementation but provides good
// distribution for bloom filter usage with minimal allocations.
func murmur3Hash(key string) (uint64, uint64) {
	data := unsafeStringToBytes(key)
	const (
		c1 = 0x87c37b91114253d5
		c2 = 0x4cf5ad432745937f
	)
	length := len(data)
	var h1, h2 uint64
	h1 = uint64(length) * c1
	h2 = uint64(length) * c2

	nblocks := length / 16
	for i := 0; i < nblocks; i++ {
		off := i * 16
		k1 := readUint64(data[off:])
		k2 := readUint64(data[off+8:])

		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// Tail processing.
	tail := data[nblocks*16:]
	var k1, k2 uint64
	switch len(tail) {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	// Finalisation mix.
	h1 ^= uint64(length)
	h2 ^= uint64(length)
	h1 += h2
	h2 += h1
	h1 = fmix64(h1)
	h2 = fmix64(h2)
	h1 += h2
	h2 += h1

	return h1, h2
}

func rotl64(x uint64, r uint) uint64 {
	return (x << r) | (x >> (64 - r))
}

func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// readUint64 reads a little-endian uint64 from at least 8 bytes.
func readUint64(b []byte) uint64 {
	return uint64(b[0]) |
		uint64(b[1])<<8 |
		uint64(b[2])<<16 |
		uint64(b[3])<<24 |
		uint64(b[4])<<32 |
		uint64(b[5])<<40 |
		uint64(b[6])<<48 |
		uint64(b[7])<<56
}

// unsafeStringToBytes converts a string to []byte without copying by reusing
// the string's underlying memory. The returned slice MUST NOT be modified.
func unsafeStringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
