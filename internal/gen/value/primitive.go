package value

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"time"
	_ "time/tzdata" // zones must resolve in a minimal container image

	"github.com/genadigeno/data-feed/internal/gen/unique"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

const (
	defaultZipfS = 1.1
	defaultZipfV = 1.0

	// Free strings below this length have no room for a random prefix.
	uniqueSuffixWidth = 13 // base-36 digits holding any 64-bit position

	defaultStringLow  = 8
	defaultStringHigh = 24
	defaultBinaryLow  = 8
	defaultBinaryHigh = 32
	defaultFloatMax   = 1e6
	defaultMoneyMax   = 1_000_000
)

type boolGen struct{}

func (boolGen) Generate(c *Ctx, out *record.Value) error {
	out.SetBool(c.Rand.Uint64()&1 == 1)
	return nil
}

type nullGen struct{}

func (nullGen) Generate(_ *Ctx, out *record.Value) error {
	out.SetNull()
	return nil
}

// intGen produces integers across [lo, hi] according to a distribution.
type intGen struct {
	lo, hi int64
	span   uint64 // hi-lo+1; 0 means the full 64-bit range
	dist   string
	mean   float64
	stddev float64
	rate   float64
	zipf   *rand.Zipf
	start  int64
	step   int64
}

func newIntGen(f *schema.FieldDef, env *Env) (Generator, error) {
	lo, hi := int64(math.MinInt32), int64(math.MaxInt32)
	if f.Type == schema.TypeLong {
		lo, hi = math.MinInt64, math.MaxInt64
	}
	var err error
	if lo, err = intBound(f.Min, lo); err != nil {
		return nil, err
	}
	if hi, err = intBound(f.Max, hi); err != nil {
		return nil, err
	}
	g := &intGen{lo: lo, hi: hi, span: uint64(hi) - uint64(lo) + 1}
	if d := f.Distribution; d != nil {
		g.dist = d.Type
		switch d.Type {
		case schema.DistNormal:
			g.mean, g.stddev = *d.Mean, *d.StdDev
		case schema.DistExponential:
			g.rate = *d.Rate
		case schema.DistZipf:
			g.zipf = NewZipf(env.Rand, d, g.span-1)
		case schema.DistSequence:
			g.start, g.step = 0, 1
			if d.Start != nil {
				g.start = *d.Start
			}
			if d.Step != nil {
				g.step = *d.Step
			}
			return &sequenceGen{start: g.start, step: g.step}, nil
		}
	}
	return g, nil
}

// NewZipf builds a Zipf sampler over [0, imax], with the documented defaults.
func NewZipf(r *rand.Rand, d *schema.Distribution, imax uint64) *rand.Zipf {
	s, v := defaultZipfS, defaultZipfV
	if d.S != nil {
		s = *d.S
	}
	if d.V != nil {
		v = *d.V
	}
	return rand.NewZipf(r, s, v, imax)
}

func intBound(l *schema.Literal, fallback int64) (int64, error) {
	if l == nil {
		return fallback, nil
	}
	r, ok := new(big.Rat).SetString(l.Text)
	if !ok || !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("%q is not an integer", l.Text)
	}
	return r.Num().Int64(), nil
}

func (g *intGen) Generate(c *Ctx, out *record.Value) error {
	out.SetInt(g.sample(c.Rand))
	return nil
}

func (g *intGen) sample(r *rand.Rand) int64 {
	switch g.dist {
	case schema.DistNormal:
		return g.clamp(r.NormFloat64()*g.stddev + g.mean)
	case schema.DistExponential:
		return g.clamp(float64(g.lo) + r.ExpFloat64()/g.rate)
	case schema.DistZipf:
		return int64(uint64(g.lo) + g.zipf.Uint64())
	}
	if g.span == 0 {
		return int64(r.Uint64())
	}
	return int64(uint64(g.lo) + r.Uint64N(g.span))
}

func (g *intGen) clamp(x float64) int64 {
	switch {
	case x <= float64(g.lo):
		return g.lo
	case x >= float64(g.hi):
		return g.hi
	}
	return int64(math.Round(x))
}

func (g *intGen) UniqueSpace() uint64 { return g.span }

func (g *intGen) At(_ *Ctx, p uint64, out *record.Value) error {
	out.SetInt(int64(uint64(g.lo) + p))
	return nil
}

type floatGen struct {
	lo, hi float64
	dist   string
	mean   float64
	stddev float64
	rate   float64
}

func newFloatGen(f *schema.FieldDef) (Generator, error) {
	g := &floatGen{hi: defaultFloatMax}
	var err error
	if g.lo, err = floatBound(f.Min, 0); err != nil {
		return nil, err
	}
	if g.hi, err = floatBound(f.Max, max(defaultFloatMax, g.lo)); err != nil {
		return nil, err
	}
	if f.Max != nil && f.Min == nil && g.hi < 0 {
		g.lo = g.hi - defaultFloatMax
	}
	if d := f.Distribution; d != nil {
		g.dist = d.Type
		switch d.Type {
		case schema.DistNormal:
			g.mean, g.stddev = *d.Mean, *d.StdDev
		case schema.DistExponential:
			g.rate = *d.Rate
		}
	}
	return g, nil
}

func floatBound(l *schema.Literal, fallback float64) (float64, error) {
	if l == nil {
		return fallback, nil
	}
	r, ok := new(big.Rat).SetString(l.Text)
	if !ok {
		return 0, fmt.Errorf("%q is not a number", l.Text)
	}
	x, _ := r.Float64()
	return x, nil
}

func (g *floatGen) Generate(c *Ctx, out *record.Value) error {
	var x float64
	switch g.dist {
	case schema.DistNormal:
		x = c.Rand.NormFloat64()*g.stddev + g.mean
	case schema.DistExponential:
		x = g.lo + c.Rand.ExpFloat64()/g.rate
	default:
		x = g.lo + c.Rand.Float64()*(g.hi-g.lo)
	}
	out.SetFloat(min(max(x, g.lo), g.hi))
	return nil
}

func decimalScale(f *schema.FieldDef) int {
	if f.Scale != nil {
		return *f.Scale
	}
	return schema.DefaultDecimalScale
}

// decimalGen draws exact decimals: integers in units of 10^-scale, rendered as text.
type decimalGen struct {
	first, last *big.Int
	span        *big.Int
	small       bool   // the span fits in a uint64 and first fits in an int64
	lo          int64  // first, when small
	spanU       uint64 // span, when small
	scale       int
	dist        *schema.Distribution
	unit        float64
}

func newDecimalGen(f *schema.FieldDef) (Generator, error) {
	scale := decimalScale(f)
	precision := schema.DefaultDecimalPrecision
	if f.Precision != nil {
		precision = *f.Precision
	}
	limit := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision-scale)), nil))
	if f.Type == schema.TypeMoney && f.Max == nil {
		limit = new(big.Rat).SetInt64(defaultMoneyMax)
	}

	lo, hi := new(big.Rat), limit
	if f.Min != nil {
		r, ok := new(big.Rat).SetString(f.Min.Text)
		if !ok {
			return nil, fmt.Errorf("%q is not a number", f.Min.Text)
		}
		lo = r
	}
	if f.Max != nil {
		r, ok := new(big.Rat).SetString(f.Max.Text)
		if !ok {
			return nil, fmt.Errorf("%q is not a number", f.Max.Text)
		}
		hi = r
	} else if lo.Cmp(hi) > 0 {
		hi = new(big.Rat).Add(lo, big.NewRat(defaultMoneyMax, 1))
	}

	first, last := schema.DecimalGrid(lo, hi, scale)
	if f.Max == nil {
		last.Sub(last, big.NewInt(1))
	}
	if first.Cmp(last) > 0 {
		return nil, fmt.Errorf("no value with scale %d lies within the range", scale)
	}
	g := &decimalGen{first: first, last: last, scale: scale, dist: f.Distribution}
	g.span = new(big.Int).Sub(last, first)
	g.span.Add(g.span, big.NewInt(1))
	if g.span.IsUint64() && first.IsInt64() {
		g.small, g.spanU, g.lo = true, g.span.Uint64(), first.Int64()
	}
	g.unit = math.Pow10(scale)
	return g, nil
}

func (g *decimalGen) Generate(c *Ctx, out *record.Value) error {
	if g.dist != nil && g.dist.Type != schema.DistUniform {
		return g.fromFloat(c, out)
	}
	if g.small {
		g.render(c, big.NewInt(0).Add(big.NewInt(g.lo), new(big.Int).SetUint64(c.Rand.Uint64N(g.spanU))), out)
		return nil
	}
	// Rejection over enough random bits keeps wide ranges exactly uniform.
	bitLen := g.span.BitLen()
	var x big.Int
	for {
		x.SetUint64(0)
		for got := 0; got < bitLen; got += 64 {
			x.Lsh(&x, 64)
			x.Or(&x, new(big.Int).SetUint64(c.Rand.Uint64()))
		}
		x.Rsh(&x, uint((64-bitLen%64)%64))
		if x.Cmp(g.span) < 0 {
			break
		}
	}
	g.render(c, new(big.Int).Add(g.first, &x), out)
	return nil
}

func (g *decimalGen) fromFloat(c *Ctx, out *record.Value) error {
	var x float64
	switch g.dist.Type {
	case schema.DistNormal:
		x = c.Rand.NormFloat64()*(*g.dist.StdDev) + *g.dist.Mean
	case schema.DistExponential:
		first, _ := new(big.Rat).SetFrac(g.first, big.NewInt(1)).Float64()
		x = first/g.unit + c.Rand.ExpFloat64()/(*g.dist.Rate)
	}
	scaled := math.Round(x * g.unit)
	switch {
	case math.IsInf(scaled, 1):
		g.render(c, g.last, out)
		return nil
	case math.IsInf(scaled, -1):
		g.render(c, g.first, out)
		return nil
	}
	v, _ := new(big.Float).SetFloat64(scaled).Int(nil)
	if v.Cmp(g.first) < 0 {
		v = g.first
	}
	if v.Cmp(g.last) > 0 {
		v = g.last
	}
	g.render(c, v, out)
	return nil
}

func (g *decimalGen) render(c *Ctx, units *big.Int, out *record.Value) {
	if g.scale == 0 {
		out.SetDecimal(units.String())
		return
	}
	neg := units.Sign() < 0
	digits := new(big.Int).Abs(units).Append(c.Scratch[:0], 10)
	buf := make([]byte, 0, len(digits)+g.scale+3)
	if neg {
		buf = append(buf, '-')
	}
	if len(digits) <= g.scale {
		buf = append(buf, '0', '.')
		for range g.scale - len(digits) {
			buf = append(buf, '0')
		}
		buf = append(buf, digits...)
	} else {
		cut := len(digits) - g.scale
		buf = append(buf, digits[:cut]...)
		buf = append(buf, '.')
		buf = append(buf, digits[cut:]...)
	}
	c.Scratch = digits[:0]
	out.SetDecimal(string(buf))
}

func (g *decimalGen) UniqueSpace() uint64 {
	if g.span.IsUint64() {
		return g.span.Uint64()
	}
	return 0
}

func (g *decimalGen) At(c *Ctx, p uint64, out *record.Value) error {
	g.render(c, new(big.Int).Add(g.first, new(big.Int).SetUint64(p)), out)
	return nil
}

type uuidGen struct{}

func (uuidGen) Generate(c *Ctx, out *record.Value) error {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], c.Rand.Uint64())
	binary.BigEndian.PutUint64(b[8:], c.Rand.Uint64())
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 9562 variant
	out.SetString(renderUUID(&b))
	return nil
}

func (uuidGen) UniqueSpace() uint64 { return 0 }

// At keeps the position in the first 64 bits, so UUIDs never collide, and the
// remaining bits random. Version 8 is the RFC's slot for custom layouts, and setting
// its bits must not overlap the position, so they go in the second half.
func (uuidGen) At(c *Ctx, p uint64, out *record.Value) error {
	var b [16]byte
	binary.BigEndian.PutUint64(b[8:], c.Rand.Uint64())
	// Bytes 0-5 and 7, 9 hold the 64 position bits around the fixed version and variant.
	b[0], b[1], b[2], b[3], b[4], b[5] = byte(p>>56), byte(p>>48), byte(p>>40), byte(p>>32), byte(p>>24), byte(p>>16)
	b[6] = 0x80 | b[6]&0x0f
	b[7] = byte(p >> 8)
	b[8] = b[8]&0x3f | 0x80
	b[9] = byte(p)
	out.SetString(renderUUID(&b))
	return nil
}

func renderUUID(b *[16]byte) string {
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:36], b[10:16])
	return string(s[:])
}

// temporalGen draws in whole units (days or seconds) and renders per type.
type temporalGen struct {
	typ    schema.TypeName
	lo, hi int64 // in units
	unit   int64 // seconds per unit
	dist   *schema.Distribution
	loc    *time.Location
	format string
}

func newTemporalGen(f *schema.FieldDef, env *Env) (Generator, error) {
	g := &temporalGen{typ: f.Type, unit: 1, dist: f.Distribution, loc: time.UTC, format: f.Format}
	if f.Type == schema.TypeDate {
		g.unit = 86400
	}
	if f.Zone != "" {
		loc, err := time.LoadLocation(f.Zone)
		if err != nil {
			return nil, fmt.Errorf("unknown zone %q", f.Zone)
		}
		g.loc = loc
	}

	if f.Type == schema.TypeTime {
		g.hi = 86399
		if f.Min != nil && f.Max != nil {
			lo, err := secondsOfDay(f.Min.Text)
			if err != nil {
				return nil, err
			}
			hi, err := secondsOfDay(f.Max.Text)
			if err != nil {
				return nil, err
			}
			g.lo, g.hi = lo, hi
		}
		return g, nil
	}

	width := int64(schema.DefaultTemporalDays)
	if f.Type != schema.TypeDate {
		width *= 86400
	}
	lo, hasLo, err := temporalBound(f.Type, f.Min, g.unit)
	if err != nil {
		return nil, err
	}
	hi, hasHi, err := temporalBound(f.Type, f.Max, g.unit)
	if err != nil {
		return nil, err
	}
	switch {
	case hasLo && hasHi:
	case hasLo:
		hi = lo + width
	case hasHi:
		lo = hi - width
	default:
		hi = env.RefTime.Unix() / g.unit
		lo = hi - width
	}
	g.lo, g.hi = lo, hi
	return g, nil
}

func secondsOfDay(s string) (int64, error) {
	t, err := time.Parse("15:04:05", s)
	if err != nil {
		return 0, err
	}
	return int64(t.Hour())*3600 + int64(t.Minute())*60 + int64(t.Second()), nil
}

func temporalBound(t schema.TypeName, l *schema.Literal, unit int64) (int64, bool, error) {
	if l == nil {
		return 0, false, nil
	}
	layout := time.RFC3339Nano
	if t == schema.TypeDate {
		layout = "2006-01-02"
	}
	v, err := time.Parse(layout, l.Text)
	if err != nil {
		return 0, false, err
	}
	return v.Unix() / unit, true, nil
}

func (g *temporalGen) Generate(c *Ctx, out *record.Value) error {
	span := uint64(g.hi-g.lo) + 1
	var x int64
	switch {
	case g.dist != nil && g.dist.Type == schema.DistNormal:
		x = g.lo + int64(c.Rand.NormFloat64()*(*g.dist.StdDev)+*g.dist.Mean)
	case g.dist != nil && g.dist.Type == schema.DistExponential:
		x = g.lo + int64(c.Rand.ExpFloat64()/(*g.dist.Rate))
	default:
		x = g.lo + int64(c.Rand.Uint64N(span))
	}
	x = min(max(x, g.lo), g.hi)
	g.render(x, c.Rand.Int64N(1000), out)
	return nil
}

func (g *temporalGen) render(x, millis int64, out *record.Value) {
	switch g.typ {
	case schema.TypeDate:
		out.SetString(time.Unix(x*g.unit, 0).UTC().Format("2006-01-02"))
	case schema.TypeTime:
		out.SetString(time.Unix(x, 0).UTC().Format("15:04:05"))
	case schema.TypeDateTime:
		out.SetString(time.Unix(x, 0).In(g.loc).Format(time.RFC3339))
	default: // timestamp
		switch g.format {
		case "unix":
			out.SetInt(x)
		case "unix_ms":
			out.SetInt(x*1000 + millis)
		default:
			out.SetString(time.Unix(x, 0).UTC().Format(time.RFC3339))
		}
	}
}

func (g *temporalGen) UniqueSpace() uint64 { return uint64(g.hi-g.lo) + 1 }

func (g *temporalGen) At(_ *Ctx, p uint64, out *record.Value) error {
	g.render(g.lo+int64(p), 0, out)
	return nil
}

const textAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// stringGen produces free text, or text matching a pattern.
type stringGen struct {
	lo, hi  int64
	pattern *patternProgram
}

func newStringGen(f *schema.FieldDef) (Generator, error) {
	g := &stringGen{}
	if f.Pattern != "" {
		p, err := compilePattern(f.Pattern)
		if err != nil {
			return nil, err
		}
		g.pattern = p
		g.lo, g.hi = 0, math.MaxInt64
		if f.MinLength != nil {
			g.lo = *f.MinLength
		}
		if f.MaxLength != nil {
			g.hi = *f.MaxLength
		}
		return g, nil
	}
	switch {
	case f.MinLength != nil && f.MaxLength != nil:
		g.lo, g.hi = *f.MinLength, *f.MaxLength
	case f.MinLength != nil:
		g.lo = *f.MinLength
		g.hi = max(g.lo, defaultStringHigh)
	case f.MaxLength != nil:
		g.hi = *f.MaxLength
		g.lo = min(defaultStringLow, g.hi)
	default:
		g.lo, g.hi = defaultStringLow, defaultStringHigh
	}
	return g, nil
}

// patternAttempts bounds retries when length limits cut into what a pattern can match.
const patternAttempts = 256

func (g *stringGen) Generate(c *Ctx, out *record.Value) error {
	if g.pattern != nil {
		for range patternAttempts {
			s := g.pattern.generate(c)
			if n := int64(runeCount(s)); n >= g.lo && n <= g.hi {
				out.SetString(s)
				return nil
			}
		}
		return fmt.Errorf("pattern rarely fits length %d..%d: gave up after %d attempts", g.lo, g.hi, patternAttempts)
	}
	n := g.lo + c.Rand.Int64N(g.hi-g.lo+1)
	buf := c.Scratch[:0]
	for range n {
		buf = append(buf, textAlphabet[c.Rand.IntN(len(textAlphabet))])
	}
	out.SetString(string(buf))
	c.Scratch = buf[:0]
	return nil
}

func runeCount(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// UniqueSpace mirrors schema.UniqueCapacity. With room for a full suffix any position
// fits, otherwise the whole value is a padded base-36 number.
func (g *stringGen) UniqueSpace() uint64 {
	if g.pattern != nil {
		return g.pattern.space
	}
	if g.hi >= uniqueSuffixWidth {
		return 0
	}
	space := uint64(1)
	for range g.hi {
		space *= 36
	}
	return space
}

// At is injective because the suffix has a fixed width: it alone determines p.
func (g *stringGen) At(c *Ctx, p uint64, out *record.Value) error {
	if g.pattern != nil {
		if !g.pattern.encodable {
			return fmt.Errorf("pattern %s cannot produce guaranteed unique values", g.pattern)
		}
		out.SetString(g.pattern.at(p))
		return nil
	}
	if g.hi < uniqueSuffixWidth {
		out.SetString(string(unique.AppendBase36(c.Scratch[:0], p, int(g.hi))))
		return nil
	}
	lo := max(g.lo, uniqueSuffixWidth)
	n := lo + c.Rand.Int64N(g.hi-lo+1)
	buf := c.Scratch[:0]
	for range n - uniqueSuffixWidth {
		buf = append(buf, textAlphabet[c.Rand.IntN(len(textAlphabet))])
	}
	buf = unique.AppendBase36(buf, p, uniqueSuffixWidth)
	out.SetString(string(buf))
	c.Scratch = buf[:0]
	return nil
}

// binaryGen produces base64 text, the documented form in every output format.
type binaryGen struct{ lo, hi int64 }

func newBinaryGen(f *schema.FieldDef) (Generator, error) {
	g := &binaryGen{lo: defaultBinaryLow, hi: defaultBinaryHigh}
	switch {
	case f.MinLength != nil && f.MaxLength != nil:
		g.lo, g.hi = *f.MinLength, *f.MaxLength
	case f.MinLength != nil:
		g.lo = *f.MinLength
		g.hi = max(g.lo, defaultBinaryHigh)
	case f.MaxLength != nil:
		g.hi = *f.MaxLength
		g.lo = min(defaultBinaryLow, g.hi)
	}
	return g, nil
}

func (g *binaryGen) Generate(c *Ctx, out *record.Value) error {
	buf := make([]byte, g.lo+c.Rand.Int64N(g.hi-g.lo+1))
	fillRandom(c.Rand, buf)
	out.SetString(base64.StdEncoding.EncodeToString(buf))
	return nil
}

func fillRandom(r *rand.Rand, buf []byte) {
	for i := 0; i < len(buf); i += 8 {
		x := r.Uint64()
		for j := i; j < min(i+8, len(buf)); j++ {
			buf[j] = byte(x)
			x >>= 8
		}
	}
}

func (g *binaryGen) UniqueSpace() uint64 {
	if g.hi >= 8 {
		return 0
	}
	return uint64(1) << (8 * uint(g.hi))
}

func (g *binaryGen) At(c *Ctx, p uint64, out *record.Value) error {
	n := g.hi
	if n >= 8 {
		n = max(g.lo, 8) + c.Rand.Int64N(g.hi-max(g.lo, 8)+1)
	}
	buf := make([]byte, n)
	fillRandom(c.Rand, buf)
	for i := range min(n, 8) {
		buf[i] = byte(p >> (8 * uint(i)))
	}
	out.SetString(base64.StdEncoding.EncodeToString(buf))
	return nil
}
