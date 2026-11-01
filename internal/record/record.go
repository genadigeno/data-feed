// Package record is the data model that moves through the pipeline: ordered fields
// holding tagged values, in pooled batches. See ADR-0010.
package record

import (
	"math"
	"strconv"
	"sync"
)

// Kind is a value's representation.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindDecimal // exact number held as text in Str
	KindString
	KindArray
	KindObject
)

// Value is a tagged union. Dates, times and binary data arrive already rendered as
// strings, in the format their field asked for, so serializers treat them as text.
type Value struct {
	Kind Kind
	Num  uint64 // bool, int64, or float64 bits
	Str  string
	List []Value
	Obj  *Object
}

func (v *Value) SetNull()            { *v = Value{} }
func (v *Value) SetString(s string)  { *v = Value{Kind: KindString, Str: s} }
func (v *Value) SetDecimal(s string) { *v = Value{Kind: KindDecimal, Str: s} }
func (v *Value) SetInt(i int64)      { *v = Value{Kind: KindInt, Num: uint64(i)} }
func (v *Value) SetFloat(f float64)  { *v = Value{Kind: KindFloat, Num: math.Float64bits(f)} }
func (v *Value) SetArray(l []Value)  { *v = Value{Kind: KindArray, List: l} }
func (v *Value) SetObject(o *Object) { *v = Value{Kind: KindObject, Obj: o} }

func (v *Value) SetBool(b bool) {
	*v = Value{Kind: KindBool}
	if b {
		v.Num = 1
	}
}

func (v *Value) Int() int64     { return int64(v.Num) }
func (v *Value) Float() float64 { return math.Float64frombits(v.Num) }
func (v *Value) Bool() bool     { return v.Num != 0 }

// Field is a named value. Names usually point at strings owned by the schema.
type Field struct {
	Name string
	Val  Value
}

// Object is an ordered list of fields.
type Object struct {
	Fields []Field
}

// Add appends a field and returns its value to fill in. The pointer is valid until
// the next Add on this object.
func (o *Object) Add(name string) *Value {
	o.Fields = append(o.Fields, Field{Name: name})
	return &o.Fields[len(o.Fields)-1].Val
}

// Get returns the named field's value, or nil.
func (o *Object) Get(name string) *Value {
	for i := range o.Fields {
		if o.Fields[i].Name == name {
			return &o.Fields[i].Val
		}
	}
	return nil
}

// ApproxSize estimates a value's serialized size in bytes, without escaping.
func (v *Value) ApproxSize() int64 {
	switch v.Kind {
	case KindNull:
		return 4
	case KindBool:
		return 5
	case KindInt:
		return int64(len(strconv.AppendInt(nil, v.Int(), 10)))
	case KindFloat:
		return 24
	case KindDecimal:
		return int64(len(v.Str))
	case KindString:
		return int64(len(v.Str)) + 2
	case KindArray:
		n := int64(2)
		for i := range v.List {
			n += v.List[i].ApproxSize() + 1
		}
		return n
	case KindObject:
		return v.Obj.ApproxSize()
	}
	return 0
}

// ApproxSize estimates an object's serialized size in bytes.
func (o *Object) ApproxSize() int64 {
	n := int64(2)
	for i := range o.Fields {
		n += int64(len(o.Fields[i].Name)) + 4 + o.Fields[i].Val.ApproxSize()
	}
	return n
}

const arenaChunk = 1024

// Arena hands out nested values and objects that live until the batch is reset, so a
// steady workload allocates almost nothing for structure.
type Arena struct {
	values  chunks[Value]
	fields  chunks[Field]
	objects chunks[Object]
}

// Values returns a zeroed slice of n values.
func (a *Arena) Values(n int) []Value { return a.values.take(n) }

// Object returns an empty object with room for n fields.
func (a *Arena) Object(n int) *Object {
	o := &a.objects.take(1)[0]
	o.Fields = a.fields.take(n)[:0]
	return o
}

// Reset makes everything handed out reusable; it must no longer be in use.
func (a *Arena) Reset() {
	a.values.reset()
	a.fields.reset()
	a.objects.reset()
}

// chunks is a bump allocator over reusable slices.
type chunks[T any] struct {
	list [][]T
	cur  int // index of the chunk in use
	pos  int // used elements in the current chunk
}

func (c *chunks[T]) take(n int) []T {
	for c.cur < len(c.list) {
		if chunk := c.list[c.cur]; len(chunk)-c.pos >= n {
			out := chunk[c.pos : c.pos+n : c.pos+n]
			c.pos += n
			return out
		}
		c.cur++
		c.pos = 0
	}
	chunk := make([]T, max(n, arenaChunk))
	c.list = append(c.list, chunk)
	c.pos = n
	return chunk[:n:n]
}

func (c *chunks[T]) reset() {
	for i := range c.list {
		if i <= c.cur {
			clear(c.list[i])
		}
	}
	c.cur, c.pos = 0, 0
}

// Batch is a group of records moving through the pipeline. One goroutine owns it at
// a time; ownership passes over channels.
type Batch struct {
	Stream  string
	Records []Object
	// Indices holds each record's stream index, parallel to Records.
	Indices []uint64
	Arena   Arena
}

// Next returns an empty record to fill in, reusing earlier capacity. The pointer is
// valid until the next call.
func (b *Batch) Next(index uint64) *Object {
	n := len(b.Records)
	if n < cap(b.Records) {
		b.Records = b.Records[:n+1]
	} else {
		b.Records = append(b.Records, Object{})
	}
	r := &b.Records[n]
	r.Fields = r.Fields[:0]
	b.Indices = append(b.Indices, index)
	return r
}

// DropLast discards the record most recently returned by Next.
func (b *Batch) DropLast() {
	b.Records = b.Records[:len(b.Records)-1]
	b.Indices = b.Indices[:len(b.Indices)-1]
}

// Len returns the number of records.
func (b *Batch) Len() int { return len(b.Records) }

// Reset empties the batch for reuse, dropping references to generated data.
func (b *Batch) Reset() {
	for i := range b.Records {
		clear(b.Records[i].Fields)
		b.Records[i].Fields = b.Records[i].Fields[:0]
	}
	b.Records = b.Records[:0]
	b.Indices = b.Indices[:0]
	b.Stream = ""
	b.Arena.Reset()
}

// Pool reuses batches.
type Pool struct{ p sync.Pool }

// NewPool returns a pool of batches with room for size records.
func NewPool(size int) *Pool {
	return &Pool{p: sync.Pool{New: func() any {
		return &Batch{Records: make([]Object, 0, size), Indices: make([]uint64, 0, size)}
	}}}
}

// Get returns an empty batch.
func (p *Pool) Get() *Batch { return p.p.Get().(*Batch) }

// Put returns a batch; the caller must not touch it afterwards.
func (p *Pool) Put(b *Batch) {
	b.Reset()
	p.p.Put(b)
}
