package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/genadigeno/data-feed/internal/ai"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/vocab"
)

func testConfig() *config.Config {
	c := config.Defaults(config.EnvDevelopment)
	return &c
}

func TestTargetsFor(t *testing.T) {
	cfg := testConfig()
	low, err := TargetsFor(cfg, SizeLow, 0, 0)
	if err != nil || low.ValuesPerGroup != cfg.Vocab.SizeLevels.Low.ValuesPerGroup {
		t.Fatalf("%+v %v", low, err)
	}
	if got, _ := TargetsFor(cfg, "", 0, 0); got != low {
		t.Fatal("the default level is not LOW")
	}
	if got, err := TargetsFor(cfg, SizeCustom, 5, 2); err != nil || got.ValuesPerGroup != 5 || got.MaxGroups != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	for _, bad := range []struct {
		level          string
		values, groups int
	}{{"HUGE", 0, 0}, {SizeCustom, 0, 1}, {SizeCustom, cfg.AI.MaxValuesPerGroup + 1, 1}, {SizeCustom, 1, cfg.AI.MaxGroups + 1}} {
		if _, err := TargetsFor(cfg, bad.level, bad.values, bad.groups); err == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}

func TestCacheKeyCoversWhatChangesAnAnswer(t *testing.T) {
	base := ai.ValuesRequest{Domain: "d", Group: ai.GroupPlan{Key: "g"}, Count: 10, Model: "m"}
	key := ValuesKey("p", &base)
	if ValuesKey("p", &base) != key {
		t.Fatal("unstable")
	}
	changes := []func(*ai.ValuesRequest){
		func(r *ai.ValuesRequest) { r.Domain = "x" },
		func(r *ai.ValuesRequest) { r.Description = "x" },
		func(r *ai.ValuesRequest) { r.Group.Key = "x" },
		func(r *ai.ValuesRequest) { r.Group.Description = "x" },
		func(r *ai.ValuesRequest) { r.Count = 11 },
		func(r *ai.ValuesRequest) { r.Ordinal = 1 },
		func(r *ai.ValuesRequest) { r.Facet = "x" },
		func(r *ai.ValuesRequest) { r.Exclude = []string{"x"} },
		func(r *ai.ValuesRequest) { r.Model = "x" },
		func(r *ai.ValuesRequest) { r.Effort = "x" },
	}
	for i, change := range changes {
		r := base
		change(&r)
		if ValuesKey("p", &r) == key {
			t.Errorf("change %d kept the key", i)
		}
	}
	if ValuesKey("other", &base) == key {
		t.Error("the provider is not in the key")
	}
	// Field boundaries matter: "ab"+"c" is not "a"+"bc".
	a, b := ai.ValuesRequest{Domain: "ab", Description: "c"}, ai.ValuesRequest{Domain: "a", Description: "bc"}
	if ValuesKey("p", &a) == ValuesKey("p", &b) {
		t.Error("fields run together")
	}
}

func TestExcludeSampleIsBoundedAndSpread(t *testing.T) {
	if ExcludeSample(nil) != nil {
		t.Fatal("something from nothing")
	}
	values := make([]vocab.Entry, 1000)
	for i := range values {
		values[i].Value = fmt.Sprint(i)
	}
	got := ExcludeSample(values)
	if len(got) != maxExcludeSample || got[0] != "0" || got[99] != "990" {
		t.Fatalf("%d %v", len(got), got[:3])
	}
	if got := ExcludeSample(values[:3]); len(got) != 3 {
		t.Fatal(got)
	}
}

func TestSaturationNeedsConsecutiveThinBatches(t *testing.T) {
	var s Saturation
	if s.Observe(5, 100) {
		t.Fatal("one thin batch")
	}
	if s.Observe(50, 100) {
		t.Fatal("reset by a good batch")
	}
	if s.Observe(9, 100) {
		t.Fatal("first thin batch again")
	}
	if !s.Observe(0, 100) {
		t.Fatal("two thin batches in a row")
	}
}

func TestFinalState(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		p         Progress
		cancelled bool
		fatal     error
		want      string
	}{
		{Progress{ValuesAccepted: 5}, false, nil, JobCompleted},
		{Progress{ValuesAccepted: 5}, true, nil, JobCancelled},
		{Progress{}, false, boom, JobFailed},
		{Progress{ValuesAccepted: 5}, false, boom, JobPartial},
		{Progress{ValuesAccepted: 5, Saturated: []string{"a"}}, false, nil, JobPartial},
		{Progress{ValuesAccepted: 5, Refused: []string{"a"}}, false, nil, JobPartial},
		{Progress{ValuesAccepted: 5, Errors: []string{"a"}}, false, nil, JobPartial},
		{Progress{ValuesAccepted: 5, BudgetExhausted: true}, false, nil, JobPartial},
		{Progress{}, false, nil, JobFailed},
	}
	for i, tc := range cases {
		if got := FinalState(&tc.p, tc.cancelled, tc.fatal); got != tc.want {
			t.Errorf("case %d: got %s", i, got)
		}
	}
}

// fakeProvider answers from a function, counting calls.
type fakeProvider struct {
	mu       sync.Mutex
	taxonomy []ai.GroupPlan
	values   func(r ai.ValuesRequest) (ai.ValuesResult, error)
	requests []ai.ValuesRequest
	taxCalls int
}

func (f *fakeProvider) Name() string { return "fake" }

func (f *fakeProvider) Taxonomy(context.Context, ai.TaxonomyRequest) (ai.TaxonomyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.taxCalls++
	return ai.TaxonomyResult{Groups: f.taxonomy, Usage: ai.Usage{InputTokens: 10, OutputTokens: 5}}, nil
}

func (f *fakeProvider) Values(_ context.Context, r ai.ValuesRequest) (ai.ValuesResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, r)
	f.mu.Unlock()
	return f.values(r)
}

// numbered proposes new distinct values on every call.
func numbered(r ai.ValuesRequest) (ai.ValuesResult, error) {
	out := make([]ai.Candidate, r.Count)
	for i := range out {
		out[i] = ai.Candidate{Value: fmt.Sprintf("%s value %d-%d", r.Group.Key, r.Ordinal, i), Weight: 1}
	}
	return ai.ValuesResult{Values: out, Usage: ai.Usage{InputTokens: 100, OutputTokens: 200}}, nil
}

type memoryCache struct {
	mu sync.Mutex
	m  map[string]string
}

func (c *memoryCache) GetCached(_ context.Context, key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	return v, ok
}

func (c *memoryCache) PutCached(_ context.Context, key, _, _, response string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = response
}

type collected struct {
	mu     sync.Mutex
	plans  []ai.GroupPlan
	groups map[string][]vocab.Entry
}

func newBuilder(p ai.Provider) (*Builder, *collected) {
	out := &collected{groups: map[string][]vocab.Entry{}}
	return &Builder{
		Provider: p,
		OnPlan: func(g []ai.GroupPlan) error {
			out.plans = g
			return nil
		},
		OnGroup: func(g *ai.GroupPlan, values []vocab.Entry) error {
			out.mu.Lock()
			defer out.mu.Unlock()
			out.groups[g.Key] = append(out.groups[g.Key], values...)
			return nil
		},
	}, out
}

func request(values, groups int) *BuildRequest {
	return &BuildRequest{
		Domain: "d", Targets: Targets{ValuesPerGroup: values, MaxGroups: groups}, BatchValues: 10, MaxValueLength: 200, MaxConcurrent: 3,
		ValuesModel: "values-model", TaxonomyModel: "taxonomy-model", UseCache: true,
	}
}

func plans(keys ...string) []ai.GroupPlan {
	out := make([]ai.GroupPlan, len(keys))
	for i, k := range keys {
		out[i] = ai.GroupPlan{Key: k, DataType: vocab.TypeString, Facets: []string{"f1", "f2"}}
	}
	return out
}

func TestBuildFillsGroupsInBatches(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("a", "b", "Bad Key"), values: numbered}
	b, out := newBuilder(f)
	p, err := b.Build(context.Background(), request(25, 5))
	if err != nil {
		t.Fatal(err)
	}
	if f.taxCalls != 1 || len(out.plans) != 2 || p.GroupsDropped != 1 || p.GroupsPlanned != 2 || p.GroupsDone != 2 {
		t.Fatalf("%+v", p)
	}
	if len(out.groups["a"]) != 25 || len(out.groups["b"]) != 25 || p.ValuesAccepted != 50 {
		t.Fatalf("a=%d b=%d %+v", len(out.groups["a"]), len(out.groups["b"]), p)
	}
	// 10, 10 and 5 per group, each with what came before excluded and a rotating facet.
	if len(f.requests) != 6 {
		t.Fatalf("%d calls", len(f.requests))
	}
	var last ai.ValuesRequest
	for _, r := range f.requests {
		if r.Group.Key == "a" && r.Ordinal == 2 {
			last = r
		}
	}
	if last.Count != 5 || len(last.Exclude) != 20 || last.Facet != "f1" || last.Model != "values-model" {
		t.Fatalf("%+v", last)
	}
	if p.Usage.InputTokens != 610 || p.Calls != 7 {
		t.Fatalf("%+v", p.Usage)
	}
	if FinalState(&p, false, nil) != JobCompleted {
		t.Fatal(FinalState(&p, false, nil))
	}
}

func TestBuildSkipsTaxonomyWithGivenGroups(t *testing.T) {
	f := &fakeProvider{values: numbered}
	b, out := newBuilder(f)
	r := request(5, 5)
	r.Groups = plans("mine")
	if _, err := b.Build(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if f.taxCalls != 0 || len(out.groups["mine"]) != 5 {
		t.Fatalf("%d %v", f.taxCalls, out.groups)
	}
}

func TestBuildStopsASaturatedGroup(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("a"), values: func(r ai.ValuesRequest) (ai.ValuesResult, error) {
		return ai.ValuesResult{Values: []ai.Candidate{{Value: "only one"}, {Value: "only two"}}}, nil
	}}
	b, out := newBuilder(f)
	r := request(1000, 5)
	r.BatchValues = 100
	p, err := b.Build(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 2 || len(out.groups["a"]) != 2 || len(p.Saturated) != 1 {
		t.Fatalf("%d calls, %+v", len(f.requests), p)
	}
	if p.RejectedReasons[ai.RejectDuplicate] != 2 {
		t.Fatalf("the second batch's repeats went uncounted: %+v", p)
	}
	if FinalState(&p, false, nil) != JobPartial {
		t.Fatal(FinalState(&p, false, nil))
	}
}

func TestBuildKeepsGoingAfterARefusal(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("bad", "good"), values: func(r ai.ValuesRequest) (ai.ValuesResult, error) {
		if r.Group.Key == "bad" {
			return ai.ValuesResult{}, fmt.Errorf("%w: general_harms", ai.ErrRefused)
		}
		return numbered(r)
	}}
	b, out := newBuilder(f)
	p, err := b.Build(context.Background(), request(5, 5))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Refused) != 1 || p.Refused[0] != "bad" || len(out.groups["good"]) != 5 {
		t.Fatalf("%+v", p)
	}
}

func TestBuildStopsOnPermanentFailure(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("a", "b"), values: func(ai.ValuesRequest) (ai.ValuesResult, error) {
		return ai.ValuesResult{}, fmt.Errorf("%w: status 401", ai.ErrPermanent)
	}}
	b, _ := newBuilder(f)
	if _, err := b.Build(context.Background(), request(5, 5)); !errors.Is(err, ai.ErrPermanent) {
		t.Fatalf("got %v", err)
	}
}

func TestBuildGivesUpOnARepeatedlyFailingGroup(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("a"), values: func(ai.ValuesRequest) (ai.ValuesResult, error) {
		return ai.ValuesResult{}, ai.ErrTruncated
	}}
	b, _ := newBuilder(f)
	p, err := b.Build(context.Background(), request(5, 5))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != maxEmptyRetries || len(p.Errors) != 1 || !strings.Contains(p.Errors[0], "a:") {
		t.Fatalf("%d calls, %+v", len(f.requests), p)
	}
}

func TestBuildRespectsTheTokenBudget(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("a"), values: numbered}
	b, out := newBuilder(f)
	r := request(1000, 5)
	r.MaxTotalTokens = 500 // the taxonomy and one or two batches
	p, err := b.Build(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !p.BudgetExhausted || len(out.groups["a"]) >= 1000 || p.Usage.InputTokens+p.Usage.OutputTokens > 1000 {
		t.Fatalf("%+v", p)
	}
}

func TestBuildReusesTheCache(t *testing.T) {
	cache := &memoryCache{m: map[string]string{}}
	run := func() (*fakeProvider, map[string][]vocab.Entry, Progress) {
		f := &fakeProvider{taxonomy: plans("a"), values: numbered}
		b, out := newBuilder(f)
		b.Cache = cache
		p, err := b.Build(context.Background(), request(25, 5))
		if err != nil {
			t.Fatal(err)
		}
		return f, out.groups, p
	}
	first, values, _ := run()
	if len(first.requests) != 3 || first.taxCalls != 1 {
		t.Fatalf("first run: %d %d", len(first.requests), first.taxCalls)
	}
	second, again, p := run()
	if len(second.requests) != 0 || second.taxCalls != 0 || p.CachedCalls != 4 || p.Calls != 0 {
		t.Fatalf("second run made calls: %d %d %+v", len(second.requests), second.taxCalls, p)
	}
	if fmt.Sprint(values) != fmt.Sprint(again) {
		t.Fatal("a cached run gave other values")
	}
}

func TestBuildDoesNotRepeatExistingValues(t *testing.T) {
	f := &fakeProvider{taxonomy: plans("a"), values: numbered}
	b, out := newBuilder(f)
	r := request(25, 5)
	r.Existing = map[string][]vocab.Entry{"a": make([]vocab.Entry, 20)}
	for i := range r.Existing["a"] {
		r.Existing["a"][i].Value = fmt.Sprintf("old %d", i)
	}
	if _, err := b.Build(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if len(out.groups["a"]) != 5 || len(f.requests) != 1 || len(f.requests[0].Exclude) != 20 {
		t.Fatalf("%d new, %d calls", len(out.groups["a"]), len(f.requests))
	}
}

func TestBuildStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeProvider{taxonomy: plans("a", "b", "c"), values: func(r ai.ValuesRequest) (ai.ValuesResult, error) {
		cancel()
		return numbered(r)
	}}
	b, _ := newBuilder(f)
	b.Slots = make(chan struct{}, 1)
	r := request(1000, 5)
	if _, err := b.Build(ctx, r); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
