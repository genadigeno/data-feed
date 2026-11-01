package record

import (
	"math"
	"testing"
)

func TestValueSetters(t *testing.T) {
	var v Value
	v.SetString("x")
	v.SetInt(-5)
	if v.Kind != KindInt || v.Int() != -5 || v.Str != "" {
		t.Fatalf("SetInt left state behind: %+v", v)
	}
	v.SetFloat(1.5)
	if v.Float() != 1.5 {
		t.Fatalf("float = %v", v.Float())
	}
	v.SetBool(true)
	if !v.Bool() {
		t.Fatal("bool lost")
	}
	v.SetBool(false)
	if v.Bool() {
		t.Fatal("bool stuck")
	}
	v.SetInt(math.MinInt64)
	if v.Int() != math.MinInt64 {
		t.Fatal("int64 extreme lost")
	}
	v.SetNull()
	if v.Kind != KindNull || v.Num != 0 {
		t.Fatalf("null = %+v", v)
	}
}

func TestObjectAddKeepsOrderAndGetFinds(t *testing.T) {
	var o Object
	o.Add("b").SetInt(1)
	o.Add("a").SetInt(2)
	if o.Fields[0].Name != "b" || o.Fields[1].Name != "a" {
		t.Fatalf("order: %+v", o.Fields)
	}
	if got := o.Get("a"); got == nil || got.Int() != 2 {
		t.Fatalf("Get = %+v", got)
	}
	if o.Get("nope") != nil {
		t.Fatal("found a missing field")
	}
}

func TestArenaSlicesDoNotOverlapAndComeBackZeroed(t *testing.T) {
	var a Arena
	first := a.Values(10)
	second := a.Values(10)
	first[9].SetInt(1)
	if second[0].Kind != KindNull {
		t.Fatal("slices overlap")
	}
	first = append(first, Value{}) // capacity is limited, so this must not reach second
	first[10].SetInt(7)
	if second[0].Kind != KindNull {
		t.Fatal("append reached the next slice")
	}

	second[0].SetString("data")
	o := a.Object(2)
	o.Add("x").SetString("data")
	a.Reset()

	again := a.Values(20)
	for i := range again {
		if again[i].Kind != KindNull || again[i].Str != "" {
			t.Fatalf("value %d not cleared: %+v", i, again[i])
		}
	}
	o = a.Object(2)
	if len(o.Fields) != 0 {
		t.Fatalf("object not empty: %+v", o)
	}
}

func TestArenaHandlesOversizeAndManyChunks(t *testing.T) {
	var a Arena
	if got := a.Values(arenaChunk * 3); len(got) != arenaChunk*3 {
		t.Fatalf("len = %d", len(got))
	}
	for range 10 {
		a.Values(arenaChunk - 1)
	}
	a.Reset()
	before := len(a.values.list)
	for range 10 {
		a.Values(arenaChunk - 1)
	}
	if len(a.values.list) > before+1 {
		t.Fatalf("reset did not reuse chunks: %d then %d", before, len(a.values.list))
	}
}

func TestBatchReuseDropsOldData(t *testing.T) {
	p := NewPool(4)
	b := p.Get()
	b.Stream = "main"
	r := b.Next(7)
	r.Add("name").SetString("secret")
	if b.Len() != 1 || b.Indices[0] != 7 {
		t.Fatalf("batch = %+v", b)
	}
	b.Reset()

	if b.Len() != 0 || len(b.Indices) != 0 || b.Stream != "" {
		t.Fatalf("not empty: %+v", b)
	}
	r = b.Next(8)
	if len(r.Fields) != 0 {
		t.Fatalf("record not empty: %+v", r)
	}
	if got := r.Fields[:1][0]; got.Name != "" || got.Val.Str != "" {
		t.Fatalf("old data still reachable: %+v", got)
	}
	p.Put(b)
}

func TestBatchDropLast(t *testing.T) {
	var b Batch
	b.Next(1)
	b.Next(2)
	b.DropLast()
	if b.Len() != 1 || b.Indices[0] != 1 {
		t.Fatalf("batch = %+v", b)
	}
}

func TestApproxSizeCoversNesting(t *testing.T) {
	var a Arena
	inner := a.Object(1)
	inner.Add("k").SetString("hello")
	list := a.Values(2)
	list[0].SetInt(12345)
	list[1].SetObject(inner)

	var o Object
	o.Add("arr").SetArray(list)
	if got := o.ApproxSize(); got < int64(len(`{"arr":[12345,{"k":"hello"}]}`))-2 {
		t.Fatalf("size = %d", got)
	}
}
