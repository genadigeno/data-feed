package schema

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"time"
)

const (
	layoutDate = "2006-01-02"
	layoutTime = "15:04:05"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// parseInstant reads a temporal literal into something comparable.
func parseInstant(t TypeName, s string) (time.Time, error) {
	switch t {
	case TypeDate:
		return time.Parse(layoutDate, s)
	case TypeTime:
		return time.Parse(layoutTime, s)
	default: // datetime, timestamp
		return time.Parse(time.RFC3339Nano, s)
	}
}

func temporalExample(t TypeName) string {
	switch t {
	case TypeDate:
		return "2026-01-31"
	case TypeTime:
		return "23:59:59"
	}
	return "2026-01-31T23:59:59Z"
}

// checkLiteral verifies a literal is a valid value of type t.
func checkLiteral(t TypeName, l Literal) error {
	want := func(kind string) error {
		return fmt.Errorf("want %s for type %s", kind, t)
	}
	switch t.class() {
	case classNumeric:
		if l.Kind != LiteralNumber {
			return want("a number")
		}
		r, ok := new(big.Rat).SetString(l.Text)
		if !ok {
			return fmt.Errorf("%q is not a number", l.Text)
		}
		return checkNumericRange(t, r, l.Text)
	case classTemporal:
		if l.Kind != LiteralString {
			return want("a quoted " + temporalExample(t))
		}
		if _, err := parseInstant(t, l.Text); err != nil {
			return fmt.Errorf("%q: want a value like %s", l.Text, temporalExample(t))
		}
	case classBool:
		if l.Kind != LiteralBool {
			return want("true or false")
		}
	case classNull:
		if l.Kind != LiteralNull {
			return want("null")
		}
	case classUUID:
		if l.Kind != LiteralString || !uuidPattern.MatchString(l.Text) {
			return want("a UUID")
		}
	case classEnum:
		if l.Kind == LiteralNull {
			return want("a scalar")
		}
	case classText, classSemantic, classIP, classBinary:
		if l.Kind != LiteralString {
			return want("a string")
		}
	default:
		return fmt.Errorf("type %s takes no scalar values", t)
	}
	return nil
}

func checkNumericRange(t TypeName, r *big.Rat, text string) error {
	switch t {
	case TypeInteger, TypeLong:
		if !r.IsInt() {
			return fmt.Errorf("%s is not an integer", text)
		}
		bits := 64
		if t == TypeInteger {
			bits = 32
		}
		limit := new(big.Int).Lsh(big.NewInt(1), uint(bits-1))
		if r.Num().Cmp(limit) >= 0 || r.Num().Cmp(new(big.Int).Neg(limit)) < 0 {
			return fmt.Errorf("%s does not fit in %d bits", text, bits)
		}
	case TypeFloat:
		if f, _ := r.Float64(); math.IsInf(f, 0) {
			return fmt.Errorf("%s is too large for a float", text)
		}
	}
	return nil
}

// compareLiterals orders two valid literals of an ordered type.
func compareLiterals(t TypeName, a, b Literal) int {
	if t.class() == classTemporal {
		x, _ := parseInstant(t, a.Text)
		y, _ := parseInstant(t, b.Text)
		return x.Compare(y)
	}
	x, _ := new(big.Rat).SetString(a.Text)
	y, _ := new(big.Rat).SetString(b.Text)
	return x.Cmp(y)
}

// literalRat reads a number literal.
func literalRat(l Literal) (*big.Rat, bool) {
	return new(big.Rat).SetString(l.Text)
}

func literalString(l Literal) string {
	if l.Kind == LiteralNull {
		return "null"
	}
	return l.Text
}
