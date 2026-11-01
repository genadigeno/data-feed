// Package unique holds the pure functions behind determinism and uniqueness: keyed
// hashing, and a keyed permutation that makes unique values look random while never
// colliding. See docs/02-domain/unique-encoders.md.
package unique

import (
	"encoding/binary"
	"math/bits"
)

// Key is a 128-bit SipHash key.
type Key struct{ K0, K1 uint64 }

// keyTag separates our use of the seed from any other, as SipHash wants 128 key bits
// and a session seed has 64.
const keyTag = 0x6461746166656564 // "datafeed"

// SeedKey derives the root key of a session.
func SeedKey(seed uint64) Key { return Key{K0: seed, K1: keyTag} }

// Sum hashes msg with SipHash-2-4.
func (k Key) Sum(msg []byte) uint64 {
	s := newSipState(k)
	n := len(msg)
	for len(msg) >= 8 {
		s.block(binary.LittleEndian.Uint64(msg))
		msg = msg[8:]
	}
	last := uint64(n) << 56
	for i, b := range msg {
		last |= uint64(b) << (8 * uint(i))
	}
	s.block(last)
	return s.finish()
}

// Sum64 hashes a single 64-bit word, faster than building a byte slice.
func (k Key) Sum64(x uint64) uint64 {
	s := newSipState(k)
	s.block(x)
	s.block(8 << 56)
	return s.finish()
}

// Derive returns a sub-key for a name, so different streams and fields get unrelated
// random sequences from one session seed.
func (k Key) Derive(name string) Key {
	msg := []byte(name)
	k0 := k.Sum(msg)
	return Key{K0: k0, K1: k.Sum64(k0)}
}

type sipState struct{ v0, v1, v2, v3 uint64 }

func newSipState(k Key) sipState {
	return sipState{
		v0: k.K0 ^ 0x736f6d6570736575,
		v1: k.K1 ^ 0x646f72616e646f6d,
		v2: k.K0 ^ 0x6c7967656e657261,
		v3: k.K1 ^ 0x7465646279746573,
	}
}

func (s *sipState) round() {
	s.v0 += s.v1
	s.v1 = bits.RotateLeft64(s.v1, 13)
	s.v1 ^= s.v0
	s.v0 = bits.RotateLeft64(s.v0, 32)
	s.v2 += s.v3
	s.v3 = bits.RotateLeft64(s.v3, 16)
	s.v3 ^= s.v2
	s.v0 += s.v3
	s.v3 = bits.RotateLeft64(s.v3, 21)
	s.v3 ^= s.v0
	s.v2 += s.v1
	s.v1 = bits.RotateLeft64(s.v1, 17)
	s.v1 ^= s.v2
	s.v2 = bits.RotateLeft64(s.v2, 32)
}

func (s *sipState) block(m uint64) {
	s.v3 ^= m
	s.round()
	s.round()
	s.v0 ^= m
}

func (s *sipState) finish() uint64 {
	s.v2 ^= 0xff
	s.round()
	s.round()
	s.round()
	s.round()
	return s.v0 ^ s.v1 ^ s.v2 ^ s.v3
}

// Mix scrambles an index before it seeds a generator. Neighbouring indices would
// otherwise seed nearly identical states. It is a bijection (splitmix64's finalizer),
// so distinct indices keep distinct seeds.
func Mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

const feistelRounds = 4

// Permutation is a keyed bijection on [0, N): a balanced Feistel network, with cycle
// walking when N is not an even power of two.
type Permutation struct {
	n         uint64 // 0 means the full 64-bit space
	halfBits  uint
	halfMask  uint64
	roundKeys [feistelRounds]Key
}

// NewPermutation returns a permutation of [0, n), where n == 0 means 2^64.
func NewPermutation(key Key, n uint64) *Permutation {
	width := uint(64)
	if n != 0 {
		width = uint(bits.Len64(n - 1))
		if width%2 != 0 {
			width++
		}
		if width < 2 {
			width = 2
		}
	}
	half := width / 2
	p := &Permutation{n: n, halfBits: half, halfMask: uint64(1)<<half - 1}
	for r := range p.roundKeys {
		p.roundKeys[r] = key.Derive(string(rune('a' + r)))
	}
	return p
}

func (p *Permutation) feistel(x uint64) uint64 {
	l, r := (x>>p.halfBits)&p.halfMask, x&p.halfMask
	for i := range p.roundKeys {
		l, r = r, l^(p.roundKeys[i].Sum64(r)&p.halfMask)
	}
	return l<<p.halfBits | r
}

// Apply maps i to its image. i must be below N; the caller checks, because a value
// beyond the space means more rows were asked for than values exist. Since the domain
// is less than four times N, cycle walking averages under four steps, and it always
// ends: the cycle through i returns to i, which is below N.
func (p *Permutation) Apply(i uint64) uint64 {
	y := p.feistel(i)
	if p.n == 0 {
		return y
	}
	for y >= p.n {
		y = p.feistel(y)
	}
	return y
}

const base36Digits = "0123456789abcdefghijklmnopqrstuvwxyz"

// AppendBase36 appends x in base 36, padded with zeros to at least width digits.
func AppendBase36(dst []byte, x uint64, width int) []byte {
	var buf [13]byte // 36^13 > 2^64
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = base36Digits[x%36]
		x /= 36
	}
	for pad := width - (len(buf) - i); pad > 0; pad-- {
		dst = append(dst, '0')
	}
	return append(dst, buf[i:]...)
}
