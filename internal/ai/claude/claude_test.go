package claude

import (
	"encoding/json"
	"testing"

	"github.com/genadigeno/data-feed/internal/ai"
)

func body(t *testing.T, model string) map[string]any {
	t.Helper()
	data, err := json.Marshal(Params(model, "low", "the prompt", ai.ValuesSchema, 4096))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestParamsUseStructuredOutputAndEffort(t *testing.T) {
	b := body(t, "claude-haiku-5-5")
	if b["model"] != "claude-haiku-5-5" || b["max_tokens"] != float64(4096) {
		t.Fatalf("%v", b)
	}
	config := b["output_config"].(map[string]any)
	if config["effort"] != "low" {
		t.Fatalf("%v", config)
	}
	format := config["format"].(map[string]any)
	if format["type"] != "json_schema" {
		t.Fatalf("%v", format)
	}
	if _, ok := format["schema"].(map[string]any); !ok {
		t.Fatalf("no schema: %v", format)
	}
	// Models that reject sampling parameters must never receive one.
	for _, key := range []string{"temperature", "top_p", "top_k", "thinking"} {
		if _, ok := b[key]; ok {
			t.Errorf("sent %s", key)
		}
	}
	if _, ok := b["fallbacks"]; ok {
		t.Error("haiku has no server-side fallback")
	}
}

func TestParamsEnableFallbacksOnOpus(t *testing.T) {
	if b := body(t, "claude-opus-5-5"); b["fallbacks"] != "default" {
		t.Fatalf("fallbacks: %v", b["fallbacks"])
	}
	p := Params("claude-opus-5-5", "medium", "x", ai.TaxonomySchema, 100)
	if len(p.Betas) != 1 {
		t.Fatalf("%v", p.Betas)
	}
	if p := Params("claude-haiku-5-5", "low", "x", ai.ValuesSchema, 100); len(p.Betas) != 0 {
		t.Fatalf("%v", p.Betas)
	}
}

func TestValuesMaxTokens(t *testing.T) {
	if got := ValuesMaxTokens(1, "claude-haiku-5-5"); got != minValuesTokens {
		t.Fatal(got)
	}
	if got := ValuesMaxTokens(200, "claude-haiku-5-5"); got != 18000 {
		t.Fatal(got)
	}
	if got := ValuesMaxTokens(200, "claude-opus-5-5"); got != 18000+thinkingAllowance {
		t.Fatal(got)
	}
}
