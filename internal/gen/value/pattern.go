package value

import (
	"fmt"
	"regexp/syntax"
	"strings"

	"github.com/genadigeno/data-feed/internal/schema"
)

// patternProgram generates strings matching a regular expression, by walking its
// parsed form. schema.AnalyzePattern and this code must agree on what is generated.
type patternProgram struct {
	root *syntax.Regexp
	// parts lists the fixed pieces when the pattern is injectively encodable.
	parts     []patternPart
	encodable bool
	space     uint64 // positions for At: 0 means the full 64-bit space
	printable map[*syntax.Regexp]*runeClass
}

// patternPart is one fixed position of an encodable pattern.
type patternPart struct {
	literal string
	class   *runeClass
}

// runeClass is the encodable members of a character class.
type runeClass struct {
	ranges []rune
	counts []int64 // running count of members up to each range
	size   int64
}

const (
	surrogateMin = 0xD800
	surrogateMax = 0xDFFF
	// printableMin and printableMax are preferred when a class such as `.` or [^a]
	// offers them: generated data should stay readable.
	printableMin = 0x20
	printableMax = 0x7E
)

func newRuneClass(ranges []rune) *runeClass {
	c := &runeClass{}
	add := func(lo, hi rune) {
		if lo > hi {
			return
		}
		c.ranges = append(c.ranges, lo, hi)
		c.size += int64(hi-lo) + 1
		c.counts = append(c.counts, c.size)
	}
	for i := 0; i+1 < len(ranges); i += 2 {
		lo, hi := ranges[i], ranges[i+1]
		if hi < surrogateMin || lo > surrogateMax {
			add(lo, hi)
			continue
		}
		add(lo, surrogateMin-1)
		add(surrogateMax+1, hi)
	}
	return c
}

// at returns the member at position i.
func (c *runeClass) at(i int64) rune {
	var before int64
	for r, upto := range c.counts {
		if i < upto {
			return c.ranges[2*r] + rune(i-before)
		}
		before = upto
	}
	return c.ranges[len(c.ranges)-1]
}

// printable returns the part of the class in printable ASCII, or the class itself if
// there is none.
func (c *runeClass) printable() *runeClass {
	var ranges []rune
	for i := 0; i+1 < len(c.ranges); i += 2 {
		lo, hi := max(c.ranges[i], printableMin), min(c.ranges[i+1], printableMax)
		if lo <= hi {
			ranges = append(ranges, lo, hi)
		}
	}
	if len(ranges) == 0 {
		return c
	}
	return newRuneClass(ranges)
}

func compilePattern(pattern string) (*patternProgram, error) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, err
	}
	re = re.Simplify()
	p := &patternProgram{root: re, printable: map[*syntax.Regexp]*runeClass{}}

	info, err := schema.AnalyzePattern(pattern)
	if err != nil {
		return nil, err
	}
	if info.Capacity != nil {
		p.parts = fixedParts(re, nil)
		p.encodable = true
		if info.Capacity.IsUint64() {
			p.space = info.Capacity.Uint64()
		}
	}
	return p, nil
}

func fixedParts(re *syntax.Regexp, parts []patternPart) []patternPart {
	switch re.Op {
	case syntax.OpLiteral:
		parts = append(parts, patternPart{literal: string(re.Rune)})
	case syntax.OpCharClass:
		parts = append(parts, patternPart{class: newRuneClass(re.Rune)})
	case syntax.OpCapture:
		parts = fixedParts(re.Sub[0], parts)
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			parts = fixedParts(sub, parts)
		}
	}
	return parts
}

func (p *patternProgram) generate(c *Ctx) string {
	var b strings.Builder
	p.walk(p.root, c, &b)
	return b.String()
}

func (p *patternProgram) walk(re *syntax.Regexp, c *Ctx, b *strings.Builder) {
	switch re.Op {
	case syntax.OpLiteral:
		for _, r := range re.Rune {
			if re.Flags&syntax.FoldCase != 0 && c.Rand.Uint64()&1 == 1 {
				r = swapCase(r)
			}
			b.WriteRune(r)
		}
	case syntax.OpCharClass:
		class, ok := p.printable[re]
		if !ok {
			class = newRuneClass(re.Rune).printable()
			p.printable[re] = class
		}
		b.WriteRune(class.at(c.Rand.Int64N(class.size)))
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		b.WriteByte(byte(printableMin + c.Rand.IntN(printableMax-printableMin+1)))
	case syntax.OpCapture:
		p.walk(re.Sub[0], c, b)
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			p.walk(sub, c, b)
		}
	case syntax.OpAlternate:
		p.walk(re.Sub[c.Rand.IntN(len(re.Sub))], c, b)
	case syntax.OpQuest:
		if c.Rand.Uint64()&1 == 1 {
			p.walk(re.Sub[0], c, b)
		}
	case syntax.OpStar:
		for range c.Rand.IntN(schema.PatternMaxRepeat + 1) {
			p.walk(re.Sub[0], c, b)
		}
	case syntax.OpPlus:
		for range 1 + c.Rand.IntN(schema.PatternMaxRepeat) {
			p.walk(re.Sub[0], c, b)
		}
	}
}

func swapCase(r rune) rune {
	switch {
	case r >= 'a' && r <= 'z':
		return r - 'a' + 'A'
	case r >= 'A' && r <= 'Z':
		return r - 'A' + 'a'
	}
	return r
}

// at renders position p as mixed-radix digits, one per class. Reading the digits back
// recovers p, so distinct positions give distinct strings.
func (p *patternProgram) at(pos uint64) string {
	digits := make([]rune, len(p.parts))
	for i := len(p.parts) - 1; i >= 0; i-- {
		class := p.parts[i].class
		if class == nil {
			continue
		}
		size := uint64(class.size)
		digits[i] = class.at(int64(pos % size))
		pos /= size
	}
	var b strings.Builder
	for i := range p.parts {
		if p.parts[i].class == nil {
			b.WriteString(p.parts[i].literal)
		} else {
			b.WriteRune(digits[i])
		}
	}
	return b.String()
}

func (p *patternProgram) String() string { return fmt.Sprintf("pattern(%s)", p.root) }
