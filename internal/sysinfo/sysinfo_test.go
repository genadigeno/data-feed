package sysinfo

import "testing"

func TestParseCgroupLimit(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		limited bool
	}{
		{"max\n", 0, false},
		{"", 0, false},
		{"junk", 0, false},
		{"0", 0, false},
		{"9223372036854771712\n", 0, false}, // cgroup v1 "no limit"
		{"1073741824\n", 1 << 30, true},
	}
	for _, c := range cases {
		got, limited := parseCgroupLimit(c.in)
		if got != c.want || limited != c.limited {
			t.Errorf("parseCgroupLimit(%q) = %d, %v", c.in, got, limited)
		}
	}
}

func TestEffectiveMemory(t *testing.T) {
	cases := []struct {
		physical, limit int64
		limited         bool
		want            int64
	}{
		{100, 0, false, 100},
		{100, 40, true, 40},
		{100, 400, true, 100},
		{0, 40, true, 40},
		{0, 0, false, 0},
	}
	for _, c := range cases {
		if got := effectiveMemory(c.physical, c.limit, c.limited); got != c.want {
			t.Errorf("effectiveMemory(%d, %d, %v) = %d, want %d", c.physical, c.limit, c.limited, got, c.want)
		}
	}
}

func TestHostValuesAreSane(t *testing.T) {
	if NumCPU() < 1 {
		t.Errorf("NumCPU = %d", NumCPU())
	}
	if MemoryBytes() < 0 {
		t.Errorf("MemoryBytes = %d", MemoryBytes())
	}
}
