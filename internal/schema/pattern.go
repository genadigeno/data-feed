package schema

import (
	"fmt"
	"math/big"
	"regexp/syntax"
)

// PatternMaxRepeat caps the unbounded repeats *, + and {n,} when generating.
const PatternMaxRepeat = 16

// PatternInfo describes what a generative pattern can produce.
type PatternInfo struct {
	MinLen, MaxLen int64 // in characters
	// Capacity is the number of distinct strings when the pattern is a fixed-length
	// concatenation of literals and character classes, the form that can be encoded
	// injectively for unique fields. It is nil for any other pattern.
	Capacity *big.Int
}

// patternCapacityLimit keeps pathological patterns from building enormous numbers.
const patternMaxNodes = 4096

// AnalyzePattern parses a pattern and reports what it generates. It rejects what
// cannot be generated: Go's regexp syntax already has no backreferences or lookaround.
func AnalyzePattern(pattern string) (PatternInfo, error) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return PatternInfo{}, err
	}
	re = re.Simplify()
	a := &patternAnalyzer{}
	minLen, maxLen, capacity, err := a.walk(re)
	if err != nil {
		return PatternInfo{}, err
	}
	return PatternInfo{MinLen: minLen, MaxLen: maxLen, Capacity: capacity}, nil
}

type patternAnalyzer struct{ nodes int }

// walk returns the length range and, for injectively encodable parts, the number of
// distinct strings. Simplify has already expanded counted repeats {m,n} into
// concatenations.
func (a *patternAnalyzer) walk(re *syntax.Regexp) (minLen, maxLen int64, capacity *big.Int, err error) {
	a.nodes++
	if a.nodes > patternMaxNodes {
		return 0, 0, nil, fmt.Errorf("pattern is too complex")
	}
	switch re.Op {
	case syntax.OpNoMatch:
		return 0, 0, nil, fmt.Errorf("pattern can never match")
	case syntax.OpEmptyMatch, syntax.OpBeginLine, syntax.OpEndLine, syntax.OpBeginText, syntax.OpEndText:
		// Anchors are accepted and ignored.
		return 0, 0, big.NewInt(1), nil
	case syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		return 0, 0, nil, fmt.Errorf("word boundaries cannot be generated")
	case syntax.OpLiteral:
		n := int64(len(re.Rune))
		if re.Flags&syntax.FoldCase != 0 {
			return n, n, nil, nil
		}
		return n, n, big.NewInt(1), nil
	case syntax.OpCharClass:
		size := classSize(re.Rune)
		if size == 0 {
			return 0, 0, nil, fmt.Errorf("pattern has an empty character class")
		}
		return 1, 1, big.NewInt(size), nil
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		return 1, 1, nil, nil
	case syntax.OpCapture:
		return a.walk(re.Sub[0])
	case syntax.OpConcat:
		capacity = big.NewInt(1)
		for _, sub := range re.Sub {
			lo, hi, c, err := a.walk(sub)
			if err != nil {
				return 0, 0, nil, err
			}
			minLen += lo
			maxLen += hi
			if c == nil || capacity == nil {
				capacity = nil
			} else {
				capacity.Mul(capacity, c)
			}
		}
		return minLen, maxLen, capacity, nil
	case syntax.OpAlternate:
		minLen, maxLen = -1, 0
		for _, sub := range re.Sub {
			lo, hi, _, err := a.walk(sub)
			if err != nil {
				return 0, 0, nil, err
			}
			if minLen < 0 || lo < minLen {
				minLen = lo
			}
			maxLen = max(maxLen, hi)
		}
		return minLen, maxLen, nil, nil
	case syntax.OpQuest:
		_, hi, _, err := a.walk(re.Sub[0])
		return 0, hi, nil, err
	case syntax.OpStar:
		_, hi, _, err := a.walk(re.Sub[0])
		return 0, hi * PatternMaxRepeat, nil, err
	case syntax.OpPlus:
		lo, hi, _, err := a.walk(re.Sub[0])
		return lo, hi * PatternMaxRepeat, nil, err
	}
	return 0, 0, nil, fmt.Errorf("unsupported pattern construct %s", re.Op)
}

// Surrogate code points cannot be encoded in UTF-8, so no class really contains them.
const (
	surrogateMin = 0xD800
	surrogateMax = 0xDFFF
)

// classSize counts the encodable runes in a class given as lo-hi pairs.
func classSize(ranges []rune) int64 {
	var n int64
	for i := 0; i+1 < len(ranges); i += 2 {
		lo, hi := ranges[i], ranges[i+1]
		n += int64(hi-lo) + 1
		if overlapLo, overlapHi := max(lo, surrogateMin), min(hi, surrogateMax); overlapLo <= overlapHi {
			n -= int64(overlapHi-overlapLo) + 1
		}
	}
	return n
}
