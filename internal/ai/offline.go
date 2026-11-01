package ai

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"strings"

	"github.com/genadigeno/data-feed/internal/vocab"
)

// OfflineName is the offline provider's name.
const OfflineName = "offline"

// Offline makes vocabulary from word lists, with no network: for development, tests and
// air-gapped installations. Its output is plausible rather than truly domain-aware, and the
// same inputs always give the same values.
type Offline struct{}

var _ Provider = Offline{}

// Name implements Provider.
func (Offline) Name() string { return OfflineName }

var (
	offlineAdjectives = []string{
		"Prime", "Summit", "Harbor", "Golden", "Silver", "Northern", "Eastern", "Crystal", "Evergreen", "Horizon", "Atlas", "Nova",
		"Radiant", "Solid", "Swift", "Bright", "Quiet", "Bold", "Clear", "Steady", "Grand", "Modern", "Classic", "Urban",
		"Coastal", "Alpine", "Sunrise", "Riverside", "Midland", "Pioneer", "Vivid", "Noble", "Compact", "Advanced", "Essential", "Premium",
	}
	offlineNouns = []string{
		"Bridge", "Anchor", "Beacon", "Compass", "Meadow", "Forge", "Lantern", "Harvest", "Canyon", "Orchard", "Vertex", "Junction",
		"Signal", "Pillar", "Crest", "Gateway", "Vector", "Keystone", "Circuit", "Foundry", "Terrace", "Haven", "Quarry", "Willow",
		"Cascade", "Falcon", "Mosaic", "Ember", "Prairie", "Summit", "Tandem", "Monarch", "Copper", "Marble", "Nimbus", "Sable",
	}
	offlineQualifiers = []string{"Standard", "Plus", "Pro", "Lite", "Select", "Max", "One", "Series A", "Series B", "Edition", "Reserve", "Basic"}
	offlineEnumWords  = []string{
		"ACTIVE", "PENDING", "CLOSED", "OPEN", "ARCHIVED", "REVIEW", "APPROVED", "REJECTED", "DRAFT", "EXPIRED", "SUSPENDED", "VERIFIED",
		"PRIMARY", "SECONDARY", "STANDARD", "PRIORITY", "EXTERNAL", "INTERNAL", "MANUAL", "AUTOMATIC", "LOCAL", "GLOBAL", "ONLINE", "OFFLINE",
	}
	offlineGroups = []GroupPlan{
		{Key: "category", Name: "Category", Description: "Top-level categories", DataType: vocab.TypeString, Category: "classification", Facets: []string{"common", "niche"}},
		{Key: "item_name", Name: "Item name", Description: "Names of the main things in this domain", DataType: vocab.TypeString, Category: "naming", Facets: []string{"everyday", "specialist"}},
		{Key: "organisation_name", Name: "Organisation name", Description: "Fictional organisations", DataType: vocab.TypeString, Category: "naming"},
		{Key: "status", Name: "Status", Description: "Lifecycle status codes", DataType: vocab.TypeEnum, Category: "workflow"},
		{Key: "type_code", Name: "Type code", Description: "Kinds of record", DataType: vocab.TypeEnum, Category: "classification"},
		{Key: "tag", Name: "Tag", Description: "Short labels", DataType: vocab.TypeString, Category: "classification"},
		{Key: "description_phrase", Name: "Description phrase", Description: "Short descriptive phrases", DataType: vocab.TypeString, Category: "text"},
		{Key: "segment", Name: "Segment", Description: "Groupings of customers or users", DataType: vocab.TypeString, Category: "classification"},
		{Key: "channel", Name: "Channel", Description: "Ways of reaching this domain", DataType: vocab.TypeString, Category: "operations"},
		{Key: "region_label", Name: "Region label", Description: "Fictional operating regions", DataType: vocab.TypeString, Category: "operations"},
	}
)

func offlineRand(parts ...string) *rand.Rand {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	var sum [8]byte
	binary.BigEndian.PutUint64(sum[:], h.Sum64())
	seed := binary.BigEndian.Uint64(sum[:])
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

// Taxonomy implements Provider.
func (Offline) Taxonomy(_ context.Context, r TaxonomyRequest) (TaxonomyResult, error) {
	n := len(offlineGroups)
	if r.MaxGroups > 0 {
		n = min(n, r.MaxGroups)
	}
	out := make([]GroupPlan, n)
	copy(out, offlineGroups[:n])
	return TaxonomyResult{Groups: out, Usage: Usage{Model: OfflineName}}, nil
}

// Values implements Provider.
func (Offline) Values(_ context.Context, r ValuesRequest) (ValuesResult, error) {
	rng := offlineRand(r.Domain, r.Group.Key, fmt.Sprint(r.Ordinal))
	out := make([]Candidate, 0, r.Count)
	for range r.Count {
		out = append(out, Candidate{Value: offlineValue(rng, r.Group.DataType), Weight: 1 + float64(rng.IntN(4))})
	}
	return ValuesResult{Values: out, Usage: Usage{Model: OfflineName}}, nil
}

func pick(r *rand.Rand, words []string) string { return words[r.IntN(len(words))] }

func offlineValue(r *rand.Rand, dataType string) string {
	switch dataType {
	case vocab.TypeEnum:
		return pick(r, offlineEnumWords) + "_" + strings.ToUpper(pick(r, offlineNouns))
	case vocab.TypeInteger:
		return fmt.Sprint(r.IntN(100000))
	case vocab.TypeDecimal:
		return fmt.Sprintf("%d.%02d", r.IntN(10000), r.IntN(100))
	}
	return pick(r, offlineAdjectives) + " " + pick(r, offlineNouns) + " " + pick(r, offlineQualifiers)
}
