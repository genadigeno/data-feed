package ai

import (
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/genadigeno/data-feed/internal/vocab"
)

// Reasons a candidate is rejected. Everything rejected is counted under one of these, so
// nothing disappears silently.
const (
	RejectEmpty     = "empty"
	RejectTooLong   = "too_long"
	RejectControl   = "control_characters"
	RejectMarkup    = "markup"
	RejectType      = "wrong_type"
	RejectDuplicate = "duplicate"
	RejectEmail     = "looks_like_email"
	RejectPhone     = "looks_like_phone"
	RejectURL       = "looks_like_url"
	RejectIBAN      = "looks_like_iban"
	RejectCard      = "looks_like_card_number"
	RejectNationID  = "looks_like_national_id"
	RejectBlocked   = "blocked"
)

const (
	minWeight = 0.01
	maxWeight = 100
)

var (
	bulletPrefix = regexp.MustCompile(`^(?:[-*•·]\s+|\d{1,4}[.)]\s+)`)
	markupRe     = regexp.MustCompile(`<[a-zA-Z/!][^>]*>|\{\{|\}\}|\$\{`)
	enumRe       = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	integerRe    = regexp.MustCompile(`^[+-]?\d{1,18}$`)
	decimalRe    = regexp.MustCompile(`^[+-]?\d{1,18}(?:\.\d{1,18})?$`)
	emailRe      = regexp.MustCompile(`(?i)[a-z0-9._%+-]+@([a-z0-9-]+(?:\.[a-z0-9-]+)+)`)
	urlRe        = regexp.MustCompile(`(?i)\b(?:https?://|www\.)[^\s]+`)
	phoneRe      = regexp.MustCompile(`(?:^|[^\d])(\+?\d[\d ()./-]{7,17}\d)(?:$|[^\d])`)
	ibanRe       = regexp.MustCompile(`\b[A-Z]{2}\d{2}(?:[ ]?[A-Z0-9]{4}){2,7}(?:[ ]?[A-Z0-9]{1,3})?\b`)
	ssnRe        = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	digitsRe     = regexp.MustCompile(`\d(?:[ -]?\d){12,18}`)
)

// reservedDomains are set aside for examples, so addresses on them are nobody's.
var reservedSuffixes = []string{"example.com", "example.org", "example.net", ".example", ".test", ".invalid", ".localhost"}

// blocklist holds a few things that should never appear in test data.
var blocklist = []string{"password", "passwd", "api_key", "secret_key", "private key", "ignore previous", "ignore all previous"}

// Normalize cleans a value up: canonical Unicode, no surrounding whitespace, quotes or
// list decoration, and runs of whitespace collapsed.
func Normalize(s string) string {
	s = norm.NFC.String(s)
	s = strings.Join(strings.Fields(s), " ")
	s = bulletPrefix.ReplaceAllString(s, "")
	for len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') || (first == '`' && last == '`') {
			s = strings.TrimSpace(s[1 : len(s)-1])
			continue
		}
		break
	}
	return s
}

// foldKey is how duplicates are recognised.
func foldKey(s string) string { return strings.ToLower(norm.NFC.String(s)) }

func typeOK(v, dataType string) bool {
	switch dataType {
	case vocab.TypeEnum:
		return enumRe.MatchString(v)
	case vocab.TypeInteger:
		return integerRe.MatchString(v)
	case vocab.TypeDecimal:
		return decimalRe.MatchString(v)
	}
	return true
}

func reserved(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, r := range reservedSuffixes {
		bare := strings.TrimPrefix(r, ".")
		if host == bare || strings.HasSuffix(host, "."+bare) {
			return true
		}
	}
	return false
}

func luhn(digits string) bool {
	sum, double := 0, false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}

func onlyDigits(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
}

// semanticAllows says whether a group is explicitly about a kind of sensitive-looking value.
func semanticAllows(g *GroupPlan, word string) bool {
	return strings.Contains(strings.ToLower(g.Key), word)
}

// screen looks for what could be real personal data. A numeric group's plain numbers can't
// be told from phone or card numbers, and aren't either.
func screen(v string, g *GroupPlan) string {
	if g.DataType == vocab.TypeInteger || g.DataType == vocab.TypeDecimal {
		return ""
	}
	lower := strings.ToLower(v)
	for _, b := range blocklist {
		if strings.Contains(lower, b) {
			return RejectBlocked
		}
	}
	for _, m := range emailRe.FindAllStringSubmatch(v, -1) {
		if !reserved(m[1]) {
			return RejectEmail
		}
	}
	for _, m := range urlRe.FindAllString(v, -1) {
		raw := m
		if !strings.Contains(strings.ToLower(raw), "://") {
			raw = "https://" + raw
		}
		if u, err := url.Parse(raw); err != nil || !reserved(u.Hostname()) {
			return RejectURL
		}
	}
	if ibanRe.MatchString(v) && !semanticAllows(g, "iban") {
		return RejectIBAN
	}
	if ssnRe.MatchString(v) && !semanticAllows(g, "ssn") && !semanticAllows(g, "national_id") {
		return RejectNationID
	}
	if !semanticAllows(g, "card") {
		for _, m := range digitsRe.FindAllString(v, -1) {
			if d := onlyDigits(m); len(d) >= 13 && len(d) <= 19 && luhn(d) {
				return RejectCard
			}
		}
	}
	if !semanticAllows(g, "phone") {
		for _, m := range phoneRe.FindAllStringSubmatch(v, -1) {
			if n := len(onlyDigits(m[1])); n >= 9 && n <= 15 {
				return RejectPhone
			}
		}
	}
	return ""
}

// CheckValue normalizes one value and returns it, or why it was rejected.
func CheckValue(raw string, g *GroupPlan, maxLen int) (string, string) {
	v := Normalize(raw)
	if v == "" {
		return "", RejectEmpty
	}
	if len([]rune(v)) > maxLen {
		return "", RejectTooLong
	}
	if strings.IndexFunc(v, func(r rune) bool { return unicode.IsControl(r) || r == unicode.ReplacementChar }) >= 0 {
		return "", RejectControl
	}
	if markupRe.MatchString(v) {
		return "", RejectMarkup
	}
	if !typeOK(v, g.DataType) {
		return "", RejectType
	}
	if reason := screen(v, g); reason != "" {
		return "", reason
	}
	return v, ""
}

// Validator keeps what a group has accepted so far.
type Validator struct {
	group  GroupPlan
	maxLen int
	seen   map[string]bool
}

// NewValidator starts validating a group. existing values are not accepted again.
func NewValidator(g GroupPlan, maxLen int, existing []string) *Validator {
	v := &Validator{group: g, maxLen: maxLen, seen: make(map[string]bool, len(existing))}
	for _, e := range existing {
		v.seen[foldKey(e)] = true
	}
	return v
}

// Accept validates candidates, returning the good ones and how many of each reason failed.
func (v *Validator) Accept(candidates []Candidate) ([]vocab.Entry, map[string]int) {
	var accepted []vocab.Entry
	var rejected map[string]int
	reject := func(reason string) {
		if rejected == nil {
			rejected = map[string]int{}
		}
		rejected[reason]++
	}
	for i := range candidates {
		value, reason := CheckValue(candidates[i].Value, &v.group, v.maxLen)
		if reason != "" {
			reject(reason)
			continue
		}
		key := foldKey(value)
		if v.seen[key] {
			reject(RejectDuplicate)
			continue
		}
		v.seen[key] = true
		accepted = append(accepted, vocab.Entry{Value: value, Weight: ClampWeight(candidates[i].Weight)})
	}
	return accepted, rejected
}

// ClampWeight keeps a weight in range; anything unusable becomes the typical 1.
func ClampWeight(w float64) float64 {
	if w <= 0 || w != w {
		return 1
	}
	return min(max(w, minWeight), maxWeight)
}

// CleanPlan validates a taxonomy: bad groups are dropped and counted, and at most maxGroups kept.
func CleanPlan(groups []GroupPlan, maxGroups int) (kept []GroupPlan, dropped int) {
	seen := map[string]bool{}
	for i := range groups {
		g := groups[i]
		g.Key = strings.TrimSpace(g.Key)
		g.Name, g.Description, g.Category = Normalize(g.Name), Normalize(g.Description), Normalize(g.Category)
		if g.DataType == "" {
			g.DataType = vocab.TypeString
		}
		if !vocab.ValidKey(g.Key) || !vocab.ValidType(g.DataType) || seen[g.Key] || len(kept) >= maxGroups {
			dropped++
			continue
		}
		g.Facets = slices.DeleteFunc(slices.Clone(g.Facets), func(f string) bool { return strings.TrimSpace(f) == "" })
		seen[g.Key] = true
		kept = append(kept, g)
	}
	return kept, dropped
}

// ParseWeightText reads a weight from text, for imports.
func ParseWeightText(s string) float64 {
	w, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 1
	}
	return ClampWeight(w)
}
