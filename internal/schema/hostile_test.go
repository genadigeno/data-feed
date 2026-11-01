package schema

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Each of these documents is small, and was once able to take the process down.
func TestHostileDocumentsStayCheap(t *testing.T) {
	longKey := strings.Repeat("k", 1<<20)
	var aliases strings.Builder
	aliases.WriteString("kind: hybrid\nfields:\n  a: &a\n    type: uuid\n    ? " + longKey + "\n    : 1\n")
	for i := range 3000 {
		fmt.Fprintf(&aliases, "  f%d: *a\n", i)
	}
	var numbers strings.Builder
	numbers.WriteString("kind: hybrid\nfields:\n  a: &a\n    type: string\n    max_length: 1e999999\n")
	for i := range 3000 {
		fmt.Fprintf(&numbers, "  f%d: *a\n", i)
	}
	for name, doc := range map[string]string{"aliased long keys": aliases.String(), "huge exponents": numbers.String()} {
		start := time.Now()
		_, problems := Decode([]byte(doc), FormatAuto)
		if len(problems) == 0 {
			t.Errorf("%s: accepted", name)
		}
		if len(problems) > maxProblems+1 {
			t.Errorf("%s: %d problems", name, len(problems))
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Errorf("%s took %v", name, d)
		}
	}
}

func TestNumbersWithinReasonStillParse(t *testing.T) {
	for _, ok := range []string{"1", "-1.5", "1e3", "1E+308", "2.5e-300", "12345678901234567890.123456789"} {
		if err := checkNumber(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"1e401", "1e-401", "1E999999", strings.Repeat("9", 65)} {
		if err := checkNumber(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestProblemsAreCapped(t *testing.T) {
	var ps Problems
	for range 1000 {
		ps.add("p", CodeBadValue, "x")
	}
	if len(ps) != maxProblems+1 || ps[maxProblems].Code != CodeLimit {
		t.Fatalf("%d", len(ps))
	}
}

func TestColumnsStopAtTheLimit(t *testing.T) {
	fields := []FieldDef{{Name: "a", Type: TypeArray}}
	if got := Columns(fields, nil, ".", 2000000000); len(got) > MaxColumns {
		t.Fatalf("%d columns", len(got))
	}
}
