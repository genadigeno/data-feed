package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/genadigeno/data-feed/internal/ai"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/hub"
	"github.com/genadigeno/data-feed/internal/ids"
	"github.com/genadigeno/data-feed/internal/store"
	"github.com/genadigeno/data-feed/internal/vocab"
)

// Error codes.
const (
	CodeNotFound  = "ERR_NOT_FOUND"
	CodeInvalid   = "ERR_CATALOG_INVALID"
	CodeImmutable = "ERR_CATALOG_IMMUTABLE"
	CodeAIOff     = "ERR_AI_DISABLED"
	CodeJobState  = "ERR_JOB_STATE"
)

const jobPollInterval = time.Second

func encodeGroups(groups []ai.GroupPlan) (string, error) {
	data, err := json.Marshal(struct {
		Groups []ai.GroupPlan `json:"groups"`
	}{groups})
	return string(data), err
}

func encodeValues(values []ai.Candidate) (string, error) {
	data, err := json.Marshal(struct {
		Values []ai.Candidate `json:"values"`
	}{values})
	return string(data), err
}

func notFound(what, id string) error {
	return apperr.New(apperr.NotFound, CodeNotFound, fmt.Sprintf("no %s %s", what, id))
}

func invalid(msg string) error { return apperr.New(apperr.InvalidArgument, CodeInvalid, msg) }

// Service manages domains, catalogs and jobs.
type Service struct {
	DB     store.DB
	Config *config.Config
	Clock  clock.Clock
	Logger *slog.Logger
	Hub    *hub.Hub
	// Provider is nil when AI is off and no offline provider is configured.
	Provider ai.Provider

	mu      sync.Mutex
	running map[string]context.CancelFunc
	wake    chan struct{}
	slots   chan struct{}
	limiter *rate.Limiter
	once    sync.Once
}

func (s *Service) init() {
	s.once.Do(func() {
		s.running = map[string]context.CancelFunc{}
		s.wake = make(chan struct{}, 1)
		s.slots = make(chan struct{}, max(s.Config.AI.MaxConcurrentRequests, 1))
		perMinute := max(s.Config.AI.RequestsPerMinute, 1)
		s.limiter = rate.NewLimiter(rate.Limit(float64(perMinute)/60), max(perMinute/10, 1))
	})
}

// CreateDomain adds a domain.
func (s *Service) CreateDomain(ctx context.Context, name, description, locale string) (*store.Domain, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, invalid("a domain needs a name")
	}
	d := &store.Domain{ID: ids.New("dom"), Name: name, Description: description, Locale: locale, CreatedAt: s.Clock.Now()}
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error { return tx.PutDomain(ctx, d) })
	return d, err
}

// UpdateDomain changes a domain's details.
func (s *Service) UpdateDomain(ctx context.Context, id, name, description, locale string) (*store.Domain, error) {
	var out *store.Domain
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		d, err := tx.GetDomain(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			return notFound("domain", id)
		}
		if err != nil {
			return err
		}
		if name != "" {
			d.Name = name
		}
		d.Description, d.Locale = description, locale
		out = d
		return tx.PutDomain(ctx, d)
	})
	return out, err
}

// GetDomain reads a domain.
func (s *Service) GetDomain(ctx context.Context, id string) (*store.Domain, error) {
	var out *store.Domain
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.GetDomain(ctx, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, notFound("domain", id)
	}
	return out, err
}

// ListDomains lists domains.
func (s *Service) ListDomains(ctx context.Context, after string, limit int) ([]store.Domain, error) {
	var out []store.Domain
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.ListDomains(ctx, after, limit)
		return err
	})
	return out, err
}

// DeleteDomain removes a domain that has no catalogs.
func (s *Service) DeleteDomain(ctx context.Context, id string) error {
	return s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := tx.GetDomain(ctx, id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return notFound("domain", id)
			}
			return err
		}
		catalogs, err := tx.ListCatalogs(ctx, id, "", 1)
		if err != nil {
			return err
		}
		if len(catalogs) > 0 {
			return apperr.New(apperr.Conflict, "ERR_DOMAIN_IN_USE", "this domain still has catalogs")
		}
		return tx.DeleteDomain(ctx, id)
	})
}

// CreateCatalog starts an empty draft.
func (s *Service) CreateCatalog(ctx context.Context, domain string) (*store.Catalog, error) {
	c := &store.Catalog{ID: ids.New("cat"), Version: 1, DomainID: domain, Status: store.CatalogDraft, CreatedAt: s.Clock.Now()}
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := tx.GetDomain(ctx, domain); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return notFound("domain", domain)
			}
			return err
		}
		return tx.PutCatalog(ctx, c)
	})
	return c, err
}

// GetCatalog reads a version; 0 means the latest.
func (s *Service) GetCatalog(ctx context.Context, id string, version int) (*store.Catalog, error) {
	var out *store.Catalog
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.GetCatalog(ctx, id, version)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, notFound("catalog", id)
	}
	return out, err
}

// Versions lists a catalog's versions.
func (s *Service) Versions(ctx context.Context, id string) ([]store.Catalog, error) {
	var out []store.Catalog
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.CatalogVersions(ctx, id)
		return err
	})
	return out, err
}

// ListCatalogs lists catalogs, optionally of one domain.
func (s *Service) ListCatalogs(ctx context.Context, domain, after string, limit int) ([]store.Catalog, error) {
	var out []store.Catalog
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.ListCatalogs(ctx, domain, after, limit)
		return err
	})
	return out, err
}

func readContent(ctx context.Context, tx store.Tx, id string, version int) (*vocab.Content, error) {
	groups, err := tx.Groups(ctx, id, version)
	if err != nil {
		return nil, err
	}
	out := &vocab.Content{Groups: make([]vocab.Group, 0, len(groups))}
	for i := range groups {
		g := &groups[i]
		rows, err := tx.Values(ctx, id, version, g.Key)
		if err != nil {
			return nil, err
		}
		group := vocab.Group{Key: g.Key, Name: g.Name, Description: g.Description, DataType: g.DataType, Category: g.Category, Locale: g.Locale, Values: make([]vocab.Entry, len(rows))}
		if g.MetadataJSON != "" {
			if err := json.Unmarshal([]byte(g.MetadataJSON), &group.Metadata); err != nil {
				return nil, fmt.Errorf("group %s metadata: %w", g.Key, err)
			}
		}
		for j := range rows {
			group.Values[j] = vocab.Entry{Value: rows[j].Value, Weight: rows[j].Weight}
			if rows[j].MetadataJSON != "" {
				if err := json.Unmarshal([]byte(rows[j].MetadataJSON), &group.Values[j].Metadata); err != nil {
					return nil, fmt.Errorf("group %s value metadata: %w", g.Key, err)
				}
			}
		}
		out.Groups = append(out.Groups, group)
	}
	return out, nil
}

// Content reads a version's groups and values.
func (s *Service) Content(ctx context.Context, id string, version int) (*vocab.Content, error) {
	var out *vocab.Content
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		c, err := tx.GetCatalog(ctx, id, version)
		if err != nil {
			return err
		}
		out, err = readContent(ctx, tx, c.ID, c.Version)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, notFound("catalog", id)
	}
	return out, err
}

// Resolve finds what a session pins: a built-in, or a published version, by default the
// latest. Drafts can't be used, because they can still change.
func (s *Service) Resolve(ctx context.Context, id string, version int) (json.RawMessage, int, error) {
	if domain, ok := strings.CutPrefix(id, vocab.BuiltinPrefix); ok {
		doc, ok := vocab.Builtin(domain)
		if !ok {
			return nil, 0, notFound("built-in catalog", domain)
		}
		content, _, err := vocab.Encode(&vocab.Content{Groups: doc.Groups})
		return content, 1, err
	}
	var content *vocab.Content
	var resolved int
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var c *store.Catalog
		var err error
		if version == 0 {
			c, err = tx.LatestPublished(ctx, id)
		} else {
			c, err = tx.GetCatalog(ctx, id, version)
		}
		if err != nil {
			return err
		}
		if c.Status != store.CatalogPublished {
			return invalid(fmt.Sprintf("version %d of catalog %s is a draft: publish it first", c.Version, id))
		}
		resolved = c.Version
		content, err = readContent(ctx, tx, c.ID, c.Version)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, 0, notFound("published catalog", id)
	}
	if err != nil {
		return nil, 0, err
	}
	encoded, _, err := vocab.Encode(content)
	return encoded, resolved, err
}

// editDraft runs a change that only a draft allows.
func (s *Service) editDraft(ctx context.Context, id string, version int, change func(ctx context.Context, tx store.Tx, c *store.Catalog) error) error {
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		c, err := tx.GetCatalog(ctx, id, version)
		if err != nil {
			return err
		}
		if c.Status != store.CatalogDraft {
			return apperr.New(apperr.Conflict, CodeImmutable, fmt.Sprintf("version %d is published: start a new version to change it", c.Version))
		}
		return change(ctx, tx, c)
	})
	if errors.Is(err, store.ErrNotFound) {
		return notFound("catalog", id)
	}
	return err
}

func groupRow(id string, version int, g *vocab.Group) (*store.CatalogGroup, error) {
	row := &store.CatalogGroup{CatalogID: id, Version: version, Key: g.Key, Name: g.Name, Description: g.Description, DataType: g.DataType, Category: g.Category, Locale: g.Locale}
	if row.DataType == "" {
		row.DataType = vocab.TypeString
	}
	if len(g.Metadata) > 0 {
		data, err := json.Marshal(g.Metadata)
		if err != nil {
			return nil, err
		}
		row.MetadataJSON = string(data)
	}
	return row, nil
}

func valueRow(id string, version int, group string, e *vocab.Entry) (*store.CatalogValue, error) {
	row := &store.CatalogValue{CatalogID: id, Version: version, GroupKey: group, Value: e.Value, Weight: e.Weight}
	if row.Weight == 0 {
		row.Weight = 1
	}
	if len(e.Metadata) > 0 {
		data, err := json.Marshal(e.Metadata)
		if err != nil {
			return nil, err
		}
		row.MetadataJSON = string(data)
	}
	return row, nil
}

// PutGroup adds or changes a draft group's definition, leaving its values alone.
func (s *Service) PutGroup(ctx context.Context, id string, version int, g *vocab.Group) error {
	if !vocab.ValidKey(g.Key) {
		return invalid(fmt.Sprintf("group key %q must be lower_snake_case, up to 64 characters", g.Key))
	}
	if g.DataType != "" && !vocab.ValidType(g.DataType) {
		return invalid(fmt.Sprintf("unknown data type %q", g.DataType))
	}
	return s.editDraft(ctx, id, version, func(ctx context.Context, tx store.Tx, c *store.Catalog) error {
		row, err := groupRow(c.ID, c.Version, g)
		if err != nil {
			return err
		}
		return tx.PutGroup(ctx, row)
	})
}

// DeleteGroup removes a draft group and its values.
func (s *Service) DeleteGroup(ctx context.Context, id string, version int, key string) error {
	return s.editDraft(ctx, id, version, func(ctx context.Context, tx store.Tx, c *store.Catalog) error {
		return tx.DeleteGroup(ctx, c.ID, c.Version, key)
	})
}

// AddValues validates values like AI output and adds them to a draft group, returning how
// many went in and why others did not.
func (s *Service) AddValues(ctx context.Context, id string, version int, group string, values []vocab.Entry) (int, map[string]int, error) {
	var added int
	var rejected map[string]int
	err := s.editDraft(ctx, id, version, func(ctx context.Context, tx store.Tx, c *store.Catalog) error {
		rows, err := tx.Groups(ctx, c.ID, c.Version)
		if err != nil {
			return err
		}
		var plan *ai.GroupPlan
		for i := range rows {
			if rows[i].Key == group {
				plan = &ai.GroupPlan{Key: rows[i].Key, DataType: rows[i].DataType}
			}
		}
		if plan == nil {
			return notFound("group", group)
		}
		existing, err := tx.Values(ctx, c.ID, c.Version, group)
		if err != nil {
			return err
		}
		seen := make([]string, len(existing))
		for i := range existing {
			seen[i] = existing[i].Value
		}
		if limit := s.Config.Vocab.MaxCatalogValues; limit > 0 && len(existing)+len(values) > limit {
			return invalid(fmt.Sprintf("the group would exceed vocab.max_catalog_values (%d)", limit))
		}
		candidates := make([]ai.Candidate, len(values))
		for i := range values {
			candidates[i] = ai.Candidate{Value: values[i].Value, Weight: values[i].Weight}
		}
		accepted, why := ai.NewValidator(*plan, s.Config.AI.MaxValueLength, seen).Accept(candidates)
		for i := range accepted {
			row, err := valueRow(c.ID, c.Version, group, &accepted[i])
			if err != nil {
				return err
			}
			if err := tx.PutValue(ctx, row); err != nil {
				return err
			}
		}
		added, rejected = len(accepted), why
		return nil
	})
	return added, rejected, err
}

// DeleteValue removes a value from a draft group.
func (s *Service) DeleteValue(ctx context.Context, id string, version int, group, value string) error {
	return s.editDraft(ctx, id, version, func(ctx context.Context, tx store.Tx, c *store.Catalog) error {
		return tx.DeleteValue(ctx, c.ID, c.Version, group, value)
	})
}

// Publish freezes a draft.
func (s *Service) Publish(ctx context.Context, id string, version int) (*store.Catalog, error) {
	var out *store.Catalog
	err := s.editDraft(ctx, id, version, func(ctx context.Context, tx store.Tx, c *store.Catalog) error {
		content, err := readContent(ctx, tx, c.ID, c.Version)
		if err != nil {
			return err
		}
		if len(content.Groups) == 0 {
			return invalid("an empty catalog cannot be published")
		}
		if err := vocab.Check(content); err != nil {
			return invalid(err.Error())
		}
		_, hash, err := vocab.Encode(content)
		if err != nil {
			return err
		}
		now := s.Clock.Now()
		c.Status, c.ContentHash, c.PublishedAt = store.CatalogPublished, hash, &now
		out = c
		return tx.PutCatalog(ctx, c)
	})
	return out, err
}

func writeContent(ctx context.Context, tx store.Tx, id string, version int, content *vocab.Content) error {
	for i := range content.Groups {
		g := &content.Groups[i]
		row, err := groupRow(id, version, g)
		if err != nil {
			return err
		}
		if err := tx.PutGroup(ctx, row); err != nil {
			return err
		}
		for j := range g.Values {
			value, err := valueRow(id, version, g.Key, &g.Values[j])
			if err != nil {
				return err
			}
			if err := tx.PutValue(ctx, value); err != nil {
				return err
			}
		}
	}
	return nil
}

// NewVersion starts a draft from a version. Only one draft exists at a time, as a second
// would make "the next version" ambiguous.
func (s *Service) NewVersion(ctx context.Context, id string, from int) (*store.Catalog, error) {
	var out *store.Catalog
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		versions, err := tx.CatalogVersions(ctx, id)
		if err != nil {
			return err
		}
		if len(versions) == 0 {
			return notFound("catalog", id)
		}
		highest := 0
		for i := range versions {
			if versions[i].Status == store.CatalogDraft {
				return apperr.New(apperr.Conflict, CodeImmutable, fmt.Sprintf("version %d is already a draft", versions[i].Version))
			}
			highest = max(highest, versions[i].Version)
		}
		if from == 0 {
			from = highest
		}
		source, err := tx.GetCatalog(ctx, id, from)
		if err != nil {
			return err
		}
		content, err := readContent(ctx, tx, id, from)
		if err != nil {
			return err
		}
		c := &store.Catalog{ID: id, Version: highest + 1, DomainID: source.DomainID, Status: store.CatalogDraft, ParentVersion: from, CreatedAt: s.Clock.Now()}
		if err := tx.PutCatalog(ctx, c); err != nil {
			return err
		}
		out = c
		return writeContent(ctx, tx, id, c.Version, content)
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, notFound("catalog", id)
	}
	return out, err
}

// Clone copies a version, or a built-in, into a new catalog's draft.
func (s *Service) Clone(ctx context.Context, id string, version int, domain string) (*store.Catalog, error) {
	if name, ok := strings.CutPrefix(id, vocab.BuiltinPrefix); ok {
		doc, ok := vocab.Builtin(name)
		if !ok {
			return nil, notFound("built-in catalog", name)
		}
		return s.insert(ctx, domain, &vocab.Content{Groups: doc.Groups})
	}
	content, err := s.Content(ctx, id, version)
	if err != nil {
		return nil, err
	}
	if domain == "" {
		source, err := s.GetCatalog(ctx, id, version)
		if err != nil {
			return nil, err
		}
		domain = source.DomainID
	}
	return s.insert(ctx, domain, content)
}

func (s *Service) insert(ctx context.Context, domain string, content *vocab.Content) (*store.Catalog, error) {
	c := &store.Catalog{ID: ids.New("cat"), Version: 1, DomainID: domain, Status: store.CatalogDraft, CreatedAt: s.Clock.Now()}
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := tx.GetDomain(ctx, domain); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return notFound("domain", domain)
			}
			return err
		}
		if err := tx.PutCatalog(ctx, c); err != nil {
			return err
		}
		return writeContent(ctx, tx, c.ID, c.Version, content)
	})
	return c, err
}

// Export renders a version in the portable format.
func (s *Service) Export(ctx context.Context, id string, version int) (*vocab.Export, error) {
	content, err := s.Content(ctx, id, version)
	if err != nil {
		return nil, err
	}
	c, err := s.GetCatalog(ctx, id, version)
	if err != nil {
		return nil, err
	}
	vocab.Canonicalize(content)
	return &vocab.Export{Format: vocab.ExportFormat, Domain: c.DomainID, Catalog: c.ID, Groups: content.Groups}, nil
}

// Import creates a draft from an exported document, validating values like AI output.
// It returns what was rejected.
func (s *Service) Import(ctx context.Context, domain string, doc *vocab.Export) (*store.Catalog, map[string]int, error) {
	if doc.Format != vocab.ExportFormat {
		return nil, nil, invalid(fmt.Sprintf("unknown format %q: expected %s", doc.Format, vocab.ExportFormat))
	}
	out := &vocab.Content{Groups: make([]vocab.Group, 0, len(doc.Groups))}
	rejected := map[string]int{}
	for i := range doc.Groups {
		g := doc.Groups[i]
		candidates := make([]ai.Candidate, len(g.Values))
		for j := range g.Values {
			candidates[j] = ai.Candidate{Value: g.Values[j].Value, Weight: g.Values[j].Weight}
		}
		accepted, why := ai.NewValidator(ai.GroupPlan{Key: g.Key, DataType: g.DataType}, s.Config.AI.MaxValueLength, nil).Accept(candidates)
		for reason, n := range why {
			rejected[reason] += n
		}
		g.Values = accepted
		if len(accepted) > 0 {
			out.Groups = append(out.Groups, g)
		}
	}
	if err := vocab.Check(out); err != nil {
		return nil, rejected, invalid(err.Error())
	}
	c, err := s.insert(ctx, domain, out)
	return c, rejected, err
}

// JobRequest asks for a catalog to be filled.
type JobRequest struct {
	DomainID string `json:"domain_id"`
	// CatalogID and Version, if set, add to an existing draft instead of starting a new one.
	CatalogID      string         `json:"catalog_id,omitempty"`
	Version        int            `json:"version,omitempty"`
	Size           string         `json:"size,omitempty"`
	ValuesPerGroup int            `json:"values_per_group,omitempty"`
	MaxGroups      int            `json:"max_groups,omitempty"`
	Groups         []ai.GroupPlan `json:"groups,omitempty"`
	TaxonomyModel  string         `json:"taxonomy_model,omitempty"`
	ValuesModel    string         `json:"values_model,omitempty"`
	NoCache        bool           `json:"no_cache,omitempty"`
}

// StartJob queues a job. Its catalog version exists at once, and fills as the job goes.
func (s *Service) StartJob(ctx context.Context, req JobRequest) (*store.VocabJob, error) {
	s.init()
	if s.Provider == nil {
		return nil, apperr.New(apperr.Conflict, CodeAIOff, "AI is disabled: set ai.enabled, or use the offline provider")
	}
	if _, err := TargetsFor(s.Config, req.Size, req.ValuesPerGroup, req.MaxGroups); err != nil {
		return nil, invalid(err.Error())
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	now := s.Clock.Now()
	job := &store.VocabJob{ID: ids.New("job"), RequestJSON: string(body), State: JobQueued, ProgressJSON: "{}", UsageJSON: "{}", CreatedAt: now}
	err = s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := tx.GetDomain(ctx, req.DomainID); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return notFound("domain", req.DomainID)
			}
			return err
		}
		if req.CatalogID != "" {
			c, err := tx.GetCatalog(ctx, req.CatalogID, req.Version)
			if errors.Is(err, store.ErrNotFound) {
				return notFound("catalog", req.CatalogID)
			}
			if err != nil {
				return err
			}
			if c.Status != store.CatalogDraft {
				return apperr.New(apperr.Conflict, CodeImmutable, "a job can only add to a draft")
			}
			job.CatalogID, job.TargetVersion = c.ID, c.Version
		} else {
			c := &store.Catalog{ID: ids.New("cat"), Version: 1, DomainID: req.DomainID, Status: store.CatalogDraft, CreatedAt: now}
			if err := tx.PutCatalog(ctx, c); err != nil {
				return err
			}
			job.CatalogID, job.TargetVersion = c.ID, c.Version
		}
		return tx.PutVocabJob(ctx, job)
	})
	if err != nil {
		return nil, err
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return job, nil
}

// GetJob reads a job.
func (s *Service) GetJob(ctx context.Context, id string) (*store.VocabJob, error) {
	var out *store.VocabJob
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.GetVocabJob(ctx, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, notFound("job", id)
	}
	return out, err
}

// CancelJob stops a job; what it has made stays in the draft.
func (s *Service) CancelJob(ctx context.Context, id string) (*store.VocabJob, error) {
	s.init()
	s.mu.Lock()
	cancel := s.running[id]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		return s.GetJob(ctx, id)
	}
	var out *store.VocabJob
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		job, err := tx.GetVocabJob(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			return notFound("job", id)
		}
		if err != nil {
			return err
		}
		if job.State != JobQueued && job.State != JobRunning {
			return apperr.New(apperr.Conflict, CodeJobState, fmt.Sprintf("the job has already ended: %s", job.State))
		}
		now := s.Clock.Now()
		job.State, job.EndedAt = JobCancelled, &now
		out = job
		return tx.PutVocabJob(ctx, job)
	})
	return out, err
}

// Run executes queued jobs until ctx is cancelled. Jobs caught running by a restart go
// again: their finished values are already in the draft and are not requested twice.
func (s *Service) Run(ctx context.Context) {
	s.init()
	if err := s.requeue(ctx); err != nil && ctx.Err() == nil {
		s.Logger.Error("requeue vocabulary jobs", "err", err)
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := s.Clock.NewTicker(jobPollInterval)
	defer ticker.Stop()
	for {
		s.startQueued(ctx, &wg)
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C():
		}
	}
}

func (s *Service) requeue(ctx context.Context) error {
	return s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		jobs, err := tx.VocabJobsInState(ctx, JobRunning)
		if err != nil {
			return err
		}
		for i := range jobs {
			jobs[i].State = JobQueued
			if err := tx.PutVocabJob(ctx, &jobs[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) startQueued(ctx context.Context, wg *sync.WaitGroup) {
	var queued []store.VocabJob
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		queued, err = tx.VocabJobsInState(ctx, JobQueued)
		return err
	})
	if err != nil {
		if ctx.Err() == nil {
			s.Logger.Error("read vocabulary jobs", "err", err)
		}
		return
	}
	for i := range queued {
		job := queued[i]
		s.mu.Lock()
		_, already := s.running[job.ID]
		if already {
			s.mu.Unlock()
			continue
		}
		jobCtx, cancel := context.WithCancel(ctx)
		s.running[job.ID] = cancel
		s.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				cancel()
				s.mu.Lock()
				delete(s.running, job.ID)
				s.mu.Unlock()
			}()
			defer func() {
				if p := recover(); p != nil {
					s.Logger.Error("vocabulary job panicked", "job_id", job.ID, "panic", p, "stack", string(debug.Stack()))
					s.finish(&job, &Progress{}, false, fmt.Errorf("panic: %v", p))
				}
			}()
			s.execute(jobCtx, ctx, &job)
		}()
	}
}

func (s *Service) execute(ctx, serverCtx context.Context, job *store.VocabJob) {
	var req JobRequest
	if err := json.Unmarshal([]byte(job.RequestJSON), &req); err != nil {
		s.finish(job, &Progress{}, false, err)
		return
	}
	targets, err := TargetsFor(s.Config, req.Size, req.ValuesPerGroup, req.MaxGroups)
	if err != nil {
		s.finish(job, &Progress{}, false, err)
		return
	}
	domain, content, err := s.jobInputs(ctx, job, req.DomainID)
	if err != nil {
		s.finish(job, &Progress{}, false, err)
		return
	}
	existing := make(map[string][]vocab.Entry, len(content.Groups))
	groups := req.Groups
	for i := range content.Groups {
		existing[content.Groups[i].Key] = content.Groups[i].Values
	}
	if err := s.setState(ctx, job, JobRunning); err != nil {
		s.finish(job, &Progress{}, false, err)
		return
	}

	cfg := &s.Config.AI
	build := &BuildRequest{
		Domain: domain.Name, Description: domain.Description, Locale: domain.Locale, Targets: targets, Groups: groups,
		TaxonomyModel: or(req.TaxonomyModel, cfg.TaxonomyModel), ValuesModel: or(req.ValuesModel, cfg.ValuesModel),
		TaxonomyEffort: cfg.TaxonomyEffort, ValuesEffort: cfg.ValuesEffort, BatchValues: cfg.BatchValues, MaxValueLength: cfg.MaxValueLength,
		MaxConcurrent: cfg.MaxConcurrentRequests, MaxTotalTokens: cfg.MaxTotalTokens, UseCache: cfg.CacheEnabled && !req.NoCache, Existing: existing,
	}
	builder := &Builder{
		Provider: s.Provider, Cache: dbCache{s}, Limiter: s.limiter, Slots: s.slots,
		OnPlan: func(plans []ai.GroupPlan) error { return s.storePlan(ctx, job, plans) },
		OnGroup: func(g *ai.GroupPlan, values []vocab.Entry) error {
			return s.storeValues(ctx, job, g.Key, values)
		},
		OnProgress: func(p *Progress) { s.publishProgress(job.ID, p) },
	}
	progress, err := builder.Build(ctx, build)
	switch {
	case serverCtx.Err() != nil:
		return // shutting down: the job stays RUNNING, and the next start requeues it
	case ctx.Err() != nil:
		s.finish(job, &progress, true, nil)
	default:
		s.finish(job, &progress, false, err)
	}
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (s *Service) jobInputs(ctx context.Context, job *store.VocabJob, domainID string) (*store.Domain, *vocab.Content, error) {
	var d *store.Domain
	var content *vocab.Content
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		if d, err = tx.GetDomain(ctx, domainID); err != nil {
			return err
		}
		content, err = readContent(ctx, tx, job.CatalogID, job.TargetVersion)
		return err
	})
	return d, content, err
}

func (s *Service) setState(ctx context.Context, job *store.VocabJob, state string) error {
	return s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		job.State = state
		return tx.PutVocabJob(ctx, job)
	})
}

func (s *Service) storePlan(ctx context.Context, job *store.VocabJob, plans []ai.GroupPlan) error {
	return s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		for i := range plans {
			p := &plans[i]
			row, err := groupRow(job.CatalogID, job.TargetVersion, &vocab.Group{Key: p.Key, Name: p.Name, Description: p.Description, DataType: p.DataType, Category: p.Category})
			if err != nil {
				return err
			}
			if err := tx.PutGroup(ctx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) storeValues(ctx context.Context, job *store.VocabJob, group string, values []vocab.Entry) error {
	return s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		for i := range values {
			row, err := valueRow(job.CatalogID, job.TargetVersion, group, &values[i])
			if err != nil {
				return err
			}
			if err := tx.PutValue(ctx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) publishProgress(id string, p *Progress) {
	if s.Hub != nil {
		s.Hub.Publish(hub.Event{Type: hub.TypeJob, Data: map[string]any{"job_id": id, "progress": p}})
	}
}

func (s *Service) finish(job *store.VocabJob, p *Progress, cancelled bool, fatal error) {
	state := FinalState(p, cancelled, fatal)
	ctx := context.Background() // the job's own context may already be cancelled
	progress, _ := json.Marshal(p)
	usage, _ := json.Marshal(p.Usage)
	now := s.Clock.Now()
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		job.State, job.ProgressJSON, job.UsageJSON, job.EndedAt = state, string(progress), string(usage), &now
		if fatal != nil {
			job.Error = fatal.Error()
		}
		return tx.PutVocabJob(ctx, job)
	})
	if err != nil {
		s.Logger.Error("store job result", "job_id", job.ID, "err", err)
	}
	s.Logger.Info("vocabulary job finished", "job_id", job.ID, "state", state, "values", p.ValuesAccepted, "rejected", p.ValuesRejected,
		"input_tokens", p.Usage.InputTokens, "output_tokens", p.Usage.OutputTokens)
	if s.Hub != nil {
		s.Hub.Publish(hub.Event{Type: hub.TypeJob, Data: map[string]any{"job_id": job.ID, "state": state, "progress": p}})
	}
}

// dbCache keeps answers in the database. A failure only costs a repeated call.
type dbCache struct{ s *Service }

func (c dbCache) GetCached(ctx context.Context, key string) (string, bool) {
	var text string
	err := c.s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		text, err = tx.GetAICache(ctx, key)
		return err
	})
	return text, err == nil
}

func (c dbCache) PutCached(ctx context.Context, key, provider, model, response string) {
	err := c.s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		return tx.PutAICache(ctx, key, provider, model, response, c.s.Clock.Now())
	})
	if err != nil && ctx.Err() == nil {
		c.s.Logger.Warn("store AI answer", "err", err)
	}
}
