package unique

import (
	"strconv"
	"testing"
)

// Vectors from the SipHash reference implementation: key 00..0f, messages 00, 00 01, ….
func TestSipHashReferenceVectors(t *testing.T) {
	k := Key{K0: 0x0706050403020100, K1: 0x0f0e0d0c0b0a0908}
	msg := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	vectors := map[int]uint64{
		0: 0x726fdb47dd0e0e31,
		1: 0x74f839c593dc67fd,
		2: 0x0d6c8009d9a94f5a,
		3: 0x85676696d7fb7e2d,
		8: 0x93f5f5799a932462,
	}
	for n, want := range vectors {
		if got := k.Sum(msg[:n]); got != want {
			t.Errorf("Sum(%d bytes) = %#x, want %#x", n, got, want)
		}
	}
}

func TestSum64MatchesSum(t *testing.T) {
	k := SeedKey(42)
	for _, x := range []uint64{0, 1, 0x0807060504030201, ^uint64(0)} {
		b := []byte{byte(x), byte(x >> 8), byte(x >> 16), byte(x >> 24), byte(x >> 32), byte(x >> 40), byte(x >> 48), byte(x >> 56)}
		if k.Sum(b) != k.Sum64(x) {
			t.Errorf("x=%#x: Sum and Sum64 differ", x)
		}
	}
}

func TestDeriveSeparatesNamesAndSeeds(t *testing.T) {
	a := SeedKey(1).Derive("main")
	if a == SeedKey(1).Derive("other") || a == SeedKey(2).Derive("main") {
		t.Fatal("derived keys collide")
	}
	if a != SeedKey(1).Derive("main") {
		t.Fatal("derivation is not deterministic")
	}
}

func TestMixIsInjectiveOnASample(t *testing.T) {
	seen := map[uint64]bool{}
	for i := range uint64(100000) {
		m := Mix(i)
		if seen[m] {
			t.Fatalf("collision at %d", i)
		}
		seen[m] = true
	}
}

func TestPermutationIsABijectionOnEverySmallDomain(t *testing.T) {
	key := SeedKey(7)
	for n := uint64(1); n <= 1<<12; n += 1 + n/16 {
		p := NewPermutation(key, n)
		seen := make([]bool, n)
		for i := range n {
			y := p.Apply(i)
			if y >= n {
				t.Fatalf("n=%d: %d maps outside to %d", n, i, y)
			}
			if seen[y] {
				t.Fatalf("n=%d: %d collides", n, y)
			}
			seen[y] = true
		}
	}
}

func TestPermutationIsInjectiveOnLargeDomains(t *testing.T) {
	for _, n := range []uint64{1 << 40, 1<<40 + 12345, 1 << 63, 0} {
		p := NewPermutation(SeedKey(9), n)
		seen := make(map[uint64]bool, 200000)
		for i := range uint64(200000) {
			y := p.Apply(i * 977)
			if n != 0 && y >= n {
				t.Fatalf("n=%d: out of range %d", n, y)
			}
			if seen[y] {
				t.Fatalf("n=%d: collision at %d", n, i)
			}
			seen[y] = true
		}
	}
}

func TestPermutationDependsOnKeyAndScrambles(t *testing.T) {
	const n = 1 << 20
	a, b := NewPermutation(SeedKey(1), n), NewPermutation(SeedKey(2), n)
	same, fixed, sequential := 0, 0, 0
	var prev uint64
	for i := range uint64(1000) {
		ya := a.Apply(i)
		if ya == b.Apply(i) {
			same++
		}
		if ya == i {
			fixed++
		}
		if i > 0 && ya == prev+1 {
			sequential++
		}
		prev = ya
	}
	if same > 5 || fixed > 5 || sequential > 5 {
		t.Fatalf("poor scrambling: same=%d fixed=%d sequential=%d", same, fixed, sequential)
	}
}

func TestAppendBase36(t *testing.T) {
	cases := []struct {
		x     uint64
		width int
		want  string
	}{
		{0, 0, ""},
		{0, 3, "000"},
		{35, 0, "z"},
		{36, 0, "10"},
		{36, 4, "0010"},
		{^uint64(0), 0, strconv.FormatUint(^uint64(0), 36)},
	}
	for _, c := range cases {
		if got := string(AppendBase36(nil, c.x, c.width)); got != c.want {
			t.Errorf("AppendBase36(%d, %d) = %q, want %q", c.x, c.width, got, c.want)
		}
	}
	if got := string(AppendBase36([]byte("id-"), 35, 2)); got != "id-0z" {
		t.Errorf("prefix lost: %q", got)
	}
}

func BenchmarkPermutation(b *testing.B) {
	p := NewPermutation(SeedKey(1), 1_000_003)
	for i := range b.N {
		p.Apply(uint64(i) % 1_000_003)
	}
}
