// Package claude is the Anthropic provider. It is the only place that imports the SDK.
package claude

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/genadigeno/data-feed/internal/ai"
)

// Name is the provider's name in configuration and cache keys.
const Name = "anthropic"

const (
	taxonomyMaxTokens = 16000
	tokensPerValue    = 60 // a typical value with its weight and JSON around it, plus a margin
	minValuesTokens   = 2048
	// thinkingAllowance leaves room for reasoning on models where it can't be turned off.
	thinkingAllowance = 16000
)

// Options configures the provider.
type Options struct {
	APIKey         string
	BaseURL        string
	MaxRetries     int
	RequestTimeout time.Duration
}

// Provider calls the Messages API.
type Provider struct {
	client  anthropic.Client
	timeout time.Duration
}

var _ ai.Provider = (*Provider)(nil)

// New returns a provider. The SDK does the retrying, with backoff and honouring retry-after.
func New(o Options) *Provider {
	opts := []option.RequestOption{option.WithAPIKey(o.APIKey), option.WithMaxRetries(o.MaxRetries)}
	if o.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(o.BaseURL))
	}
	return &Provider{client: anthropic.NewClient(opts...), timeout: o.RequestTimeout}
}

// Name implements ai.Provider.
func (p *Provider) Name() string { return Name }

// Taxonomy implements ai.Provider.
func (p *Provider) Taxonomy(ctx context.Context, r ai.TaxonomyRequest) (ai.TaxonomyResult, error) {
	text, usage, err := p.call(ctx, r.Model, r.Effort, ai.TaxonomyPrompt(&r), ai.TaxonomySchema, taxonomyMaxTokens)
	if err != nil {
		return ai.TaxonomyResult{Usage: usage}, err
	}
	groups, err := ai.ParseTaxonomy(text)
	return ai.TaxonomyResult{Groups: groups, Usage: usage}, err
}

// Values implements ai.Provider.
func (p *Provider) Values(ctx context.Context, r ai.ValuesRequest) (ai.ValuesResult, error) {
	text, usage, err := p.call(ctx, r.Model, r.Effort, ai.ValuesPrompt(&r), ai.ValuesSchema, ValuesMaxTokens(r.Count, r.Model))
	if err != nil {
		return ai.ValuesResult{Usage: usage}, err
	}
	values, err := ai.ParseValues(text)
	return ai.ValuesResult{Values: values, Usage: usage}, err
}

// ValuesMaxTokens sizes a batch's token limit.
func ValuesMaxTokens(count int, model string) int64 {
	tokens := max(int64(count)*tokensPerValue*3/2, minValuesTokens)
	if !SupportsFallback(model) {
		return tokens
	}
	return tokens + thinkingAllowance
}

// SupportsFallback says whether a model takes server-side refusal fallbacks. Opus models do;
// Haiku has none, and a refused batch is simply recorded.
func SupportsFallback(model string) bool { return strings.HasPrefix(model, "claude-opus-") }

// Params builds a request.
func Params(model, effort, prompt string, schema map[string]any, maxTokens int64) anthropic.BetaMessageNewParams {
	params := anthropic.BetaMessageNewParams{
		Model:     anthropic.Model(model),
		MaxTokens: maxTokens,
		System:    []anthropic.BetaTextBlockParam{{Text: ai.SystemPrompt}},
		Messages:  []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(prompt))},
		OutputConfig: anthropic.BetaOutputConfigParam{
			Effort: anthropic.BetaOutputConfigEffort(effort),
			Format: anthropic.BetaJSONOutputFormatParam{Schema: schema},
		},
	}
	if SupportsFallback(model) {
		params.Fallbacks = anthropic.BetaFallbacksParamOfDefault()
		params.Betas = []anthropic.AnthropicBeta{anthropic.AnthropicBetaServerSideFallback2026_07_01}
	}
	return params
}

// call streams one request, which keeps long answers from hitting HTTP timeouts.
func (p *Provider) call(ctx context.Context, model, effort, prompt string, schema map[string]any, maxTokens int64) (string, ai.Usage, error) {
	usage := ai.Usage{Model: model}
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	stream := p.client.Beta.Messages.NewStreaming(ctx, Params(model, effort, prompt, schema, maxTokens))
	defer stream.Close()
	var message anthropic.BetaMessage
	for stream.Next() {
		if err := message.Accumulate(stream.Current()); err != nil {
			return "", usage, fmt.Errorf("read answer: %w", err)
		}
	}
	if err := stream.Err(); err != nil {
		return "", usage, classify(err)
	}
	usage.InputTokens, usage.OutputTokens = message.Usage.InputTokens, message.Usage.OutputTokens

	switch message.StopReason {
	case anthropic.BetaStopReasonRefusal:
		return "", usage, fmt.Errorf("%w: %s", ai.ErrRefused, message.StopDetails.Category)
	case anthropic.BetaStopReasonMaxTokens:
		return "", usage, ai.ErrTruncated
	}
	var text strings.Builder
	for i := range message.Content {
		if block, ok := message.Content[i].AsAny().(anthropic.BetaTextBlock); ok {
			text.WriteString(block.Text)
		}
	}
	if text.Len() == 0 {
		return "", usage, errors.New("the answer had no text")
	}
	return text.String(), usage, nil
}

// classify separates failures that no retry will fix. The SDK has already retried the rest.
func classify(err error) error {
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			return fmt.Errorf("%w: status %d: %v", ai.ErrPermanent, apiErr.StatusCode, err)
		}
	}
	return err
}
