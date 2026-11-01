// Package catalog manages vocabulary catalogs and the jobs that fill them. Decisions are
// pure functions; Service and Runner load rows, decide and store.
// See docs/02-domain/domain-vocabulary.md.
package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/genadigeno/data-feed/internal/ai"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/vocab"
)

// Size levels.
const (
	SizeLow    = "LOW"
	SizeMedium = "MEDIUM"
	SizeHigh   = "HIGH"
	SizeCustom = "CUSTOM"
)

// Job states.
const (
	JobQueued    = "QUEUED"
	JobRunning   = "RUNNING"
	JobCompleted = "COMPLETED"
	JobPartial   = "PARTIAL"
	JobFailed    = "FAILED"
	JobCancelled = "CANCELLED"
)

const (
	maxExcludeSample  = 100
	saturationShare   = 0.10 // a batch adding less than this share of what it asked for is thin
	saturationBatches = 2    // this many thin batches in a row end the group
	maxEmptyRetries   = 3
)

// Targets are how big a job makes things.
type Targets struct {
	ValuesPerGroup int
	MaxGroups      int
}

// TargetsFor resolves a size level against its configuration and limits.
func TargetsFor(cfg *config.Config, level string, customValues, customGroups int) (Targets, error) {
	levels := cfg.Vocab.SizeLevels
	var t Targets
	switch level {
	case SizeLow, "":
		t = Targets{levels.Low.ValuesPerGroup, levels.Low.MaxGroups}
	case SizeMedium:
		t = Targets{levels.Medium.ValuesPerGroup, levels.Medium.MaxGroups}
	case SizeHigh:
		t = Targets{levels.High.ValuesPerGroup, levels.High.MaxGroups}
	case SizeCustom:
		if customValues <= 0 || customGroups <= 0 {
			return t, errors.New("a CUSTOM size needs values_per_group and max_groups")
		}
		t = Targets{customValues, customGroups}
	default:
		return t, fmt.Errorf("unknown size level %q", level)
	}
	if t.ValuesPerGroup > cfg.AI.MaxValuesPerGroup {
		return t, fmt.Errorf("%d values per group exceeds ai.max_values_per_group (%d)", t.ValuesPerGroup, cfg.AI.MaxValuesPerGroup)
	}
	if t.MaxGroups > cfg.AI.MaxGroups {
		return t, fmt.Errorf("%d groups exceeds ai.max_groups (%d)", t.MaxGroups, cfg.AI.MaxGroups)
	}
	return t, nil
}

// ValuesKey is a cached batch's key. Everything that can change the answer is in it.
func ValuesKey(provider string, r *ai.ValuesRequest) string {
	return cacheKey("values", provider, r.Model, r.Effort, ai.PromptVersion, r.Domain, r.Description, r.Locale, r.Group.Key, r.Group.Name,
		r.Group.Description, r.Group.DataType, fmt.Sprint(r.Count), fmt.Sprint(r.Ordinal), r.Facet, strings.Join(r.Exclude, "\x1f"))
}

// TaxonomyKey is a cached taxonomy's key.
func TaxonomyKey(provider string, r *ai.TaxonomyRequest) string {
	return cacheKey("taxonomy", provider, r.Model, r.Effort, ai.PromptVersion, r.Domain, r.Description, r.Locale, fmt.Sprint(r.MaxGroups))
}

func cacheKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s\x00", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ExcludeSample bounds what a follow-up batch is told to avoid, spread over what exists.
func ExcludeSample(values []vocab.Entry) []string {
	if len(values) == 0 {
		return nil
	}
	n := min(len(values), maxExcludeSample)
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, values[i*len(values)/n].Value)
	}
	return out
}

// FacetFor rotates through a group's facets.
func FacetFor(g *ai.GroupPlan, ordinal int) string {
	if len(g.Facets) == 0 {
		return ""
	}
	return g.Facets[ordinal%len(g.Facets)]
}

// Saturation notices when a group has run out of new values.
type Saturation struct{ thin int }

// Observe records a batch and reports whether the group should stop.
func (s *Saturation) Observe(accepted, asked int) bool {
	if float64(accepted) < float64(asked)*saturationShare {
		s.thin++
	} else {
		s.thin = 0
	}
	return s.thin >= saturationBatches
}

// Progress is a job's state, reported as it goes.
type Progress struct {
	GroupsPlanned   int            `json:"groups_planned"`
	GroupsDone      int            `json:"groups_done"`
	ValuesAccepted  int            `json:"values_accepted"`
	ValuesRejected  int            `json:"values_rejected"`
	RejectedReasons map[string]int `json:"rejected_reasons,omitempty"`
	Saturated       []string       `json:"saturated_groups,omitempty"`
	Refused         []string       `json:"refused_groups,omitempty"`
	Errors          []string       `json:"errors,omitempty"`
	GroupsDropped   int            `json:"groups_dropped,omitempty"`
	Usage           ai.Usage       `json:"usage"`
	Calls           int            `json:"calls"`
	CachedCalls     int            `json:"cached_calls"`
	BudgetExhausted bool           `json:"budget_exhausted,omitempty"`
}

// FinalState decides how a finished job ended.
func FinalState(p *Progress, cancelled bool, fatal error) string {
	switch {
	case cancelled:
		return JobCancelled
	case fatal != nil && p.ValuesAccepted == 0:
		return JobFailed
	case fatal != nil, len(p.Refused) > 0, len(p.Errors) > 0, len(p.Saturated) > 0, p.BudgetExhausted:
		return JobPartial
	case p.ValuesAccepted == 0:
		return JobFailed
	}
	return JobCompleted
}

// Cache remembers answers.
type Cache interface {
	GetCached(ctx context.Context, key string) (string, bool)
	PutCached(ctx context.Context, key, provider, model, response string)
}

// BuildRequest describes the vocabulary to make.
type BuildRequest struct {
	Domain, Description, Locale string
	Targets                     Targets
	// Groups, if given, skip the taxonomy call.
	Groups                                     []ai.GroupPlan
	TaxonomyModel, ValuesModel                 string
	TaxonomyEffort, ValuesEffort               string
	BatchValues, MaxValueLength, MaxConcurrent int
	MaxTotalTokens                             int64
	UseCache                                   bool
	// Existing values per group, which are not produced again.
	Existing map[string][]vocab.Entry
}

// Builder runs a job's calls. Sinks are told about each finished group, and progress as it
// changes, so that the caller stores them; it holds no state of its own beyond the run.
type Builder struct {
	Provider ai.Provider
	Cache    Cache
	Limiter  *rate.Limiter
	// Slots bounds concurrent calls across jobs.
	Slots chan struct{}
	// OnPlan receives the groups once they are known.
	OnPlan func(groups []ai.GroupPlan) error
	// OnGroup receives a group's new values. It may be called more than once for a group.
	OnGroup    func(g *ai.GroupPlan, values []vocab.Entry) error
	OnProgress func(*Progress)
}

type buildRun struct {
	b    *Builder
	req  *BuildRequest
	mu   sync.Mutex
	prog Progress
}

// Build produces vocabulary. A failure in one group is recorded and the rest continue;
// only what no group can survive, such as a bad API key, ends the run and is returned.
func (b *Builder) Build(ctx context.Context, req *BuildRequest) (Progress, error) {
	run := &buildRun{b: b, req: req}
	groups := req.Groups
	if len(groups) == 0 {
		planned, err := run.taxonomy(ctx)
		if err != nil {
			return run.prog, err
		}
		groups = planned
	}
	groups, dropped := ai.CleanPlan(groups, req.Targets.MaxGroups)
	run.update(func(p *Progress) {
		p.GroupsPlanned = len(groups)
		p.GroupsDropped = dropped
	})
	if len(groups) == 0 {
		return run.prog, errors.New("no usable groups were proposed")
	}
	if b.OnPlan != nil {
		if err := b.OnPlan(groups); err != nil {
			return run.prog, err
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(req.MaxConcurrent, 1))
	for i := range groups {
		group := &groups[i]
		g.Go(func() error { return run.fill(gctx, group) })
	}
	err := g.Wait()
	return run.prog, err
}

func (r *buildRun) update(change func(*Progress)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	change(&r.prog)
	if r.b.OnProgress != nil {
		snapshot := r.prog
		r.b.OnProgress(&snapshot)
	}
}

func (r *buildRun) spent() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prog.Usage.InputTokens + r.prog.Usage.OutputTokens
}

func (r *buildRun) addUsage(u ai.Usage) {
	r.update(func(p *Progress) {
		p.Usage.InputTokens += u.InputTokens
		p.Usage.OutputTokens += u.OutputTokens
		p.Calls++
	})
}

// acquire waits for a call slot and the rate limit.
func (r *buildRun) acquire(ctx context.Context) (func(), error) {
	if r.b.Slots != nil {
		select {
		case r.b.Slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	release := func() {
		if r.b.Slots != nil {
			<-r.b.Slots
		}
	}
	if r.b.Limiter != nil {
		if err := r.b.Limiter.Wait(ctx); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

func (r *buildRun) taxonomy(ctx context.Context) ([]ai.GroupPlan, error) {
	q := ai.TaxonomyRequest{
		Domain: r.req.Domain, Description: r.req.Description, Locale: r.req.Locale, MaxGroups: r.req.Targets.MaxGroups,
		Model: r.req.TaxonomyModel, Effort: r.req.TaxonomyEffort,
	}
	key := TaxonomyKey(r.b.Provider.Name(), &q)
	if r.req.UseCache && r.b.Cache != nil {
		if text, ok := r.b.Cache.GetCached(ctx, key); ok {
			if groups, err := ai.ParseTaxonomy(text); err == nil {
				r.update(func(p *Progress) { p.CachedCalls++ })
				return groups, nil
			}
		}
	}
	release, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	res, err := r.b.Provider.Taxonomy(ctx, q)
	r.addUsage(res.Usage)
	if err != nil {
		return nil, fmt.Errorf("taxonomy: %w", err)
	}
	if r.b.Cache != nil {
		if text, err := encodeGroups(res.Groups); err == nil {
			r.b.Cache.PutCached(ctx, key, r.b.Provider.Name(), q.Model, text)
		}
	}
	return res.Groups, nil
}

// fill brings one group up to its target.
func (r *buildRun) fill(ctx context.Context, group *ai.GroupPlan) error {
	existing := r.req.Existing[group.Key]
	seen := make([]string, len(existing))
	for i := range existing {
		seen[i] = existing[i].Value
	}
	validator := ai.NewValidator(*group, r.req.MaxValueLength, seen)
	accepted := existing
	count := len(existing)
	var saturation Saturation
	failures := 0

	defer r.update(func(p *Progress) { p.GroupsDone++ })

	for ordinal := 0; count < r.req.Targets.ValuesPerGroup; ordinal++ {
		if r.req.MaxTotalTokens > 0 && r.spent() >= r.req.MaxTotalTokens {
			r.update(func(p *Progress) { p.BudgetExhausted = true })
			return nil
		}
		asked := min(r.req.Targets.ValuesPerGroup-count, r.req.BatchValues)
		candidates, err := r.batch(ctx, group, accepted, asked, ordinal)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, ai.ErrPermanent):
			return err
		case errors.Is(err, ai.ErrRefused):
			r.update(func(p *Progress) { p.Refused = append(p.Refused, group.Key) })
			return nil
		default:
			// A bad answer or a truncated one may go better next time, but not forever.
			failures++
			if failures >= maxEmptyRetries {
				r.update(func(p *Progress) { p.Errors = append(p.Errors, group.Key+": "+err.Error()) })
				return nil
			}
			continue
		}
		fresh, rejected := validator.Accept(candidates)
		if len(fresh) > 0 {
			if err := r.b.OnGroup(group, fresh); err != nil {
				return err
			}
		}
		accepted = append(accepted, fresh...)
		count += len(fresh)
		r.update(func(p *Progress) {
			p.ValuesAccepted += len(fresh)
			for reason, n := range rejected {
				if p.RejectedReasons == nil {
					p.RejectedReasons = map[string]int{}
				}
				p.RejectedReasons[reason] += n
				p.ValuesRejected += n
			}
		})
		if count < r.req.Targets.ValuesPerGroup && saturation.Observe(len(fresh), asked) {
			r.update(func(p *Progress) { p.Saturated = append(p.Saturated, group.Key) })
			return nil
		}
	}
	return nil
}

func (r *buildRun) batch(ctx context.Context, group *ai.GroupPlan, accepted []vocab.Entry, asked, ordinal int) ([]ai.Candidate, error) {
	q := ai.ValuesRequest{
		Domain: r.req.Domain, Description: r.req.Description, Locale: r.req.Locale, Group: *group, Count: asked, Ordinal: ordinal,
		Exclude: ExcludeSample(accepted), Facet: FacetFor(group, ordinal), Model: r.req.ValuesModel, Effort: r.req.ValuesEffort,
	}
	key := ValuesKey(r.b.Provider.Name(), &q)
	if r.req.UseCache && r.b.Cache != nil {
		if text, ok := r.b.Cache.GetCached(ctx, key); ok {
			if values, err := ai.ParseValues(text); err == nil {
				r.update(func(p *Progress) { p.CachedCalls++ })
				return values, nil
			}
		}
	}
	release, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	res, err := r.b.Provider.Values(ctx, q)
	r.addUsage(res.Usage)
	if err != nil {
		return nil, err
	}
	if r.b.Cache != nil {
		if text, err := encodeValues(res.Values); err == nil {
			r.b.Cache.PutCached(ctx, key, r.b.Provider.Name(), q.Model, text)
		}
	}
	return res.Values, nil
}
