package api

import (
	"context"
	_ "embed" // for the OpenAPI document
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/catalog"
	"github.com/genadigeno/data-feed/internal/coord"
	"github.com/genadigeno/data-feed/internal/ids"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
	"github.com/genadigeno/data-feed/internal/vocab"
)

//go:embed openapi.yaml
var openAPIDocument []byte

func (s *server) health(w http.ResponseWriter, _ *http.Request) error {
	s.writeJSON(w, http.StatusOK, HealthResponse{Status: "ok", Version: s.o.Version})
	return nil
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) error {
	resp := summarize(s.o.Ready(r.Context()))
	status := http.StatusOK
	if !resp.Ready {
		status = http.StatusServiceUnavailable
	}
	s.writeJSON(w, status, resp)
	return nil
}

func (s *server) openAPI(w http.ResponseWriter, _ *http.Request) error {
	w.Header().Set("Content-Type", "application/yaml")
	_, err := w.Write(openAPIDocument)
	return err
}

func (s *server) whoAmI(w http.ResponseWriter, r *http.Request) error {
	s.writeJSON(w, http.StatusOK, principalOf(r))
	return nil
}

func (s *server) dashboard(w http.ResponseWriter, r *http.Request) error {
	out := DashboardDTO{Workers: map[string]int{}}
	var recent []store.Session
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		if out.Sessions, err = tx.CountSessionsByState(ctx); err != nil {
			return err
		}
		records, bytes, err := tx.SumCommitted(ctx)
		if err != nil {
			return err
		}
		out.RecordsCommitted, out.BytesCommitted = u64(records), i64(bytes)
		workers, err := tx.ListWorkers(ctx)
		if err != nil {
			return err
		}
		for i := range workers {
			out.Workers[workers[i].State]++
		}
		recent, err = tx.ListSessions(ctx, store.SessionFilter{Limit: 10})
		return err
	})
	if err != nil {
		return err
	}
	out.Recent = make([]SessionDTO, len(recent))
	for i := range recent {
		out.Recent[i] = s.dto(&recent[i])
		out.CurrentRate += out.Recent[i].Rates.Current
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

// metrics serves the Prometheus text format. It is a few gauges and counters, which does
// not justify a client library and its dependencies.
func (s *server) metrics(w http.ResponseWriter, r *http.Request) error {
	var m MetricsSnapshot
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		if m.Sessions, err = tx.CountSessionsByState(ctx); err != nil {
			return err
		}
		if m.Records, m.Bytes, err = tx.SumCommitted(ctx); err != nil {
			return err
		}
		workers, err := tx.ListWorkers(ctx)
		m.Workers = map[string]int64{}
		for i := range workers {
			m.Workers[workers[i].State]++
		}
		return err
	})
	if err != nil {
		return err
	}
	m.Version = s.o.Version
	m.SSEClients = s.sseClients.Load()
	m.Requests = s.requests.snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, err = io.WriteString(w, RenderMetrics(&m))
	return err
}

// MetricsSnapshot is what /metrics reports.
type MetricsSnapshot struct {
	Version    string
	Sessions   map[string]int64
	Workers    map[string]int64
	Records    uint64
	Bytes      int64
	SSEClients int64
	Requests   map[int]int64 // by status class: 2 for 2xx and so on
}

func sortedKeys[K ~string | ~int, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// RenderMetrics writes the Prometheus text format, in a stable order.
func RenderMetrics(m *MetricsSnapshot) string {
	var b strings.Builder
	head := func(name, kind, help string) { fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind) }
	head("datafeed_build_info", "gauge", "Build version.")
	fmt.Fprintf(&b, "datafeed_build_info{version=%q} 1\n", m.Version)
	head("datafeed_sessions", "gauge", "Sessions by state.")
	for _, state := range sortedKeys(m.Sessions) {
		fmt.Fprintf(&b, "datafeed_sessions{state=%q} %d\n", state, m.Sessions[state])
	}
	head("datafeed_workers", "gauge", "Registered Workers by state.")
	for _, state := range sortedKeys(m.Workers) {
		fmt.Fprintf(&b, "datafeed_workers{state=%q} %d\n", state, m.Workers[state])
	}
	head("datafeed_records_committed", "gauge", "Records in stored output.")
	fmt.Fprintf(&b, "datafeed_records_committed %d\n", m.Records)
	head("datafeed_bytes_committed", "gauge", "Bytes of stored output.")
	fmt.Fprintf(&b, "datafeed_bytes_committed %d\n", m.Bytes)
	head("datafeed_sse_clients", "gauge", "Connected live update streams.")
	fmt.Fprintf(&b, "datafeed_sse_clients %d\n", m.SSEClients)
	head("datafeed_http_requests_total", "counter", "Public API requests by status class.")
	for _, class := range sortedKeys(m.Requests) {
		fmt.Fprintf(&b, "datafeed_http_requests_total{class=\"%dxx\"} %d\n", class, m.Requests[class])
	}
	return b.String()
}

// Schemas

type schemaRequest struct {
	Name     string          `json:"name"`
	Document json.RawMessage `json:"document"`
}

// parseSchemaDocument takes a schema as a JSON object, or a string holding YAML or JSON.
func (s *server) parseSchemaDocument(doc json.RawMessage) (*schema.Schema, error) {
	if len(doc) == 0 {
		return nil, badRequest("document is required")
	}
	data := []byte(doc)
	var text string
	if json.Unmarshal(doc, &text) == nil {
		data = []byte(text)
	}
	parsed, problems := schema.Decode(data, schema.FormatAuto)
	if len(problems) == 0 {
		problems = schema.Validate(parsed, schema.Options{Limits: s.schemaLimits()})
	}
	if len(problems) > 0 {
		return nil, apperr.Validation(session.CodeValidation, "invalid schema", problemsOf(problems))
	}
	return parsed, nil
}

func problemsOf(in schema.Problems) []apperr.Problem {
	out := make([]apperr.Problem, len(in))
	for i, p := range in {
		out[i] = apperr.Problem{Path: p.Path, Code: p.Code, Message: p.Message}
	}
	return out
}

func (s *server) schemaLimits() schema.Limits {
	g := &s.o.Config.Generation
	return schema.Limits{
		MaxDepth: int64(g.RandomCaps.MaxDepth), MaxFields: int64(g.RandomCaps.MaxFields), MaxArrayLength: int64(g.RandomCaps.MaxArrayLength),
		MaxStringLength: int64(g.RandomCaps.MaxStringLength), MaxRecordBytes: int64(g.MaxRecordSizeBytes),
	}
}

func (s *server) putSchemaVersion(ctx context.Context, id, name string, parsed *schema.Schema) (*store.SchemaRow, error) {
	row := &store.SchemaRow{ID: id, Name: name, Kind: string(parsed.Kind), Definition: string(schema.EncodeJSON(parsed)), CreatedAt: s.o.Clock.Now(), Version: 1}
	err := s.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		latest, err := tx.GetSchema(ctx, id, 0)
		switch {
		case err == nil:
			row.Version = latest.Version + 1
			if row.Name == "" {
				row.Name = latest.Name
			}
		case !errors.Is(err, store.ErrNotFound):
			return err
		}
		return tx.PutSchema(ctx, row)
	})
	return row, err
}

func (s *server) createSchema(w http.ResponseWriter, r *http.Request) error {
	var req schemaRequest
	if err := readJSON(r, &req); err != nil {
		return err
	}
	if strings.TrimSpace(req.Name) == "" {
		return badRequest("name is required")
	}
	parsed, err := s.parseSchemaDocument(req.Document)
	if err != nil {
		return err
	}
	row, err := s.putSchemaVersion(r.Context(), ids.New("sch"), req.Name, parsed)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, ToSchemaDTO(row, true))
	return nil
}

func (s *server) updateSchema(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	var req schemaRequest
	if err := readJSON(r, &req); err != nil {
		return err
	}
	parsed, err := s.parseSchemaDocument(req.Document)
	if err != nil {
		return err
	}
	if _, err := s.readSchema(r.Context(), id, 0); err != nil {
		return err
	}
	row, err := s.putSchemaVersion(r.Context(), id, req.Name, parsed)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToSchemaDTO(row, true))
	return nil
}

func (s *server) validateSchema(w http.ResponseWriter, r *http.Request) error {
	var req schemaRequest
	if err := readJSON(r, &req); err != nil {
		return err
	}
	parsed, err := s.parseSchemaDocument(req.Document)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"valid": true, "kind": parsed.Kind, "hash": schema.Hash(parsed)})
	return nil
}

func (s *server) readSchema(ctx context.Context, id string, version int) (*store.SchemaRow, error) {
	var row *store.SchemaRow
	err := s.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		row, err = tx.GetSchema(ctx, id, version)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, notFound("schema", id)
	}
	return row, err
}

func versionParam(r *http.Request) (int, error) {
	v := r.URL.Query().Get("version")
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, badRequest("version must be a positive number")
	}
	return n, nil
}

func pathVersion(r *http.Request) (int, error) {
	n, err := strconv.Atoi(r.PathValue("v"))
	if err != nil || n < 1 {
		return 0, badRequest("the version must be a positive number")
	}
	return n, nil
}

func (s *server) getSchema(w http.ResponseWriter, r *http.Request) error {
	version, err := versionParam(r)
	if err != nil {
		return err
	}
	row, err := s.readSchema(r.Context(), r.PathValue("id"), version)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToSchemaDTO(row, true))
	return nil
}

func (s *server) listSchemas(w http.ResponseWriter, r *http.Request) error {
	after, limit, err := s.page(r)
	if err != nil {
		return err
	}
	var rows []store.SchemaRow
	err = s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.ListSchemas(ctx, after, limit+1)
		return err
	})
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, MakePage(rows, limit, func(row *store.SchemaRow) string { return row.ID },
		func(row *store.SchemaRow) SchemaDTO { return ToSchemaDTO(row, false) }))
	return nil
}

func (s *server) deleteSchema(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if _, err := s.readSchema(r.Context(), id, 0); err != nil {
		return err
	}
	err := s.o.DB.Write(r.Context(), func(ctx context.Context, tx store.Tx) error { return tx.DeleteSchema(ctx, id) })
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) previewSchema(w http.ResponseWriter, r *http.Request) error {
	version, err := versionParam(r)
	if err != nil {
		return err
	}
	row, err := s.readSchema(r.Context(), r.PathValue("id"), version)
	if err != nil {
		return err
	}
	var req struct {
		Count   uint64  `json:"count"`
		Seed    *uint64 `json:"seed"`
		Format  string  `json:"format"`
		Table   string  `json:"table"`
		Catalog string  `json:"catalog"`
	}
	if err := readOptionalJSON(r, &req); err != nil {
		return err
	}
	if req.Count == 0 {
		req.Count = 10
	}
	if req.Count > 1000 {
		return badRequest("a preview shows at most 1000 records")
	}
	if req.Format == "" {
		req.Format = "ndjson"
	}
	spec := map[string]any{
		"name": "preview", "mode": session.ModeBatch, "schema": map[string]any{"inline": json.RawMessage(row.Definition)},
		"output": map[string]any{"format": req.Format},
	}
	if row.Kind != string(schema.KindRelational) {
		spec["limits"] = map[string]any{"count": req.Count}
	}
	if req.Seed != nil {
		spec["seed"] = *req.Seed
	}
	if req.Catalog != "" {
		spec["catalog"] = map[string]any{"ref": map[string]any{"id": req.Catalog}}
	}
	body, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return s.syncGenerate(w, r, body, req.Table)
}

// Domains and catalogs

type domainRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Locale      string `json:"locale"`
}

func (s *server) createDomain(w http.ResponseWriter, r *http.Request) error {
	var req domainRequest
	if err := readJSON(r, &req); err != nil {
		return err
	}
	d, err := s.o.Catalogs.CreateDomain(r.Context(), req.Name, req.Description, req.Locale)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, ToDomainDTO(d))
	return nil
}

func (s *server) listDomains(w http.ResponseWriter, r *http.Request) error {
	after, limit, err := s.page(r)
	if err != nil {
		return err
	}
	rows, err := s.o.Catalogs.ListDomains(r.Context(), after, limit+1)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, MakePage(rows, limit, func(d *store.Domain) string { return d.ID }, ToDomainDTO))
	return nil
}

func (s *server) getDomain(w http.ResponseWriter, r *http.Request) error {
	d, err := s.o.Catalogs.GetDomain(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToDomainDTO(d))
	return nil
}

func (s *server) updateDomain(w http.ResponseWriter, r *http.Request) error {
	var req domainRequest
	if err := readJSON(r, &req); err != nil {
		return err
	}
	d, err := s.o.Catalogs.UpdateDomain(r.Context(), r.PathValue("id"), req.Name, req.Description, req.Locale)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToDomainDTO(d))
	return nil
}

func (s *server) deleteDomain(w http.ResponseWriter, r *http.Request) error {
	if err := s.o.Catalogs.DeleteDomain(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) startVocabularyJob(w http.ResponseWriter, r *http.Request) error {
	var req catalog.JobRequest
	if err := readJSON(r, &req); err != nil {
		return err
	}
	req.DomainID = r.PathValue("id")
	job, err := s.o.Catalogs.StartJob(r.Context(), req)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusAccepted, ToJobDTO(job))
	return nil
}

func (s *server) listBuiltins(w http.ResponseWriter, _ *http.Request) error {
	type builtin struct {
		ID          string `json:"id"`
		Domain      string `json:"domain"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Groups      int    `json:"groups"`
		Values      int    `json:"values"`
	}
	names := vocab.Builtins()
	out := make([]builtin, 0, len(names))
	for _, name := range names {
		doc, ok := vocab.Builtin(name)
		if !ok {
			continue
		}
		values := 0
		for i := range doc.Groups {
			values += len(doc.Groups[i].Values)
		}
		out = append(out, builtin{vocab.BuiltinPrefix + name, name, doc.Name, doc.Description, len(doc.Groups), values})
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *server) getBuiltin(w http.ResponseWriter, r *http.Request) error {
	doc, ok := vocab.Builtin(r.PathValue("domain"))
	if !ok {
		return notFound("built-in catalog", r.PathValue("domain"))
	}
	s.writeJSON(w, http.StatusOK, doc)
	return nil
}

func (s *server) createCatalog(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		DomainID string `json:"domain_id"`
	}
	if err := readJSON(r, &req); err != nil {
		return err
	}
	c, err := s.o.Catalogs.CreateCatalog(r.Context(), req.DomainID)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, ToCatalogDTO(c))
	return nil
}

func (s *server) listCatalogs(w http.ResponseWriter, r *http.Request) error {
	after, limit, err := s.page(r)
	if err != nil {
		return err
	}
	rows, err := s.o.Catalogs.ListCatalogs(r.Context(), r.URL.Query().Get("domain"), after, limit+1)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, MakePage(rows, limit, func(c *store.Catalog) string { return c.ID }, ToCatalogDTO))
	return nil
}

func (s *server) getCatalog(w http.ResponseWriter, r *http.Request) error {
	version, err := versionParam(r)
	if err != nil {
		return err
	}
	c, err := s.o.Catalogs.GetCatalog(r.Context(), r.PathValue("id"), version)
	if err != nil {
		return err
	}
	content, err := s.o.Catalogs.Content(r.Context(), c.ID, c.Version)
	if err != nil {
		return err
	}
	vocab.Canonicalize(content)
	s.writeJSON(w, http.StatusOK, map[string]any{"catalog": ToCatalogDTO(c), "groups": content.Groups})
	return nil
}

func (s *server) cloneCatalog(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		Version  int    `json:"version"`
		DomainID string `json:"domain_id"`
	}
	if err := readOptionalJSON(r, &req); err != nil {
		return err
	}
	c, err := s.o.Catalogs.Clone(r.Context(), r.PathValue("id"), req.Version, req.DomainID)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, ToCatalogDTO(c))
	return nil
}

func (s *server) catalogVersions(w http.ResponseWriter, r *http.Request) error {
	rows, err := s.o.Catalogs.Versions(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return notFound("catalog", r.PathValue("id"))
	}
	out := make([]CatalogDTO, len(rows))
	for i := range rows {
		out[i] = ToCatalogDTO(&rows[i])
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *server) newCatalogVersion(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		From int `json:"from"`
	}
	if err := readOptionalJSON(r, &req); err != nil {
		return err
	}
	c, err := s.o.Catalogs.NewVersion(r.Context(), r.PathValue("id"), req.From)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, ToCatalogDTO(c))
	return nil
}

func (s *server) publishCatalog(w http.ResponseWriter, r *http.Request) error {
	version, err := pathVersion(r)
	if err != nil {
		return err
	}
	c, err := s.o.Catalogs.Publish(r.Context(), r.PathValue("id"), version)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToCatalogDTO(c))
	return nil
}

func (s *server) exportCatalog(w http.ResponseWriter, r *http.Request) error {
	version, err := pathVersion(r)
	if err != nil {
		return err
	}
	doc, err := s.o.Catalogs.Export(r.Context(), r.PathValue("id"), version)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-v%d.json"`, r.PathValue("id"), version))
	s.writeJSON(w, http.StatusOK, doc)
	return nil
}

func (s *server) importCatalog(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		DomainID string       `json:"domain_id"`
		Document vocab.Export `json:"document"`
	}
	if err := readJSON(r, &req); err != nil {
		return err
	}
	c, rejected, err := s.o.Catalogs.Import(r.Context(), req.DomainID, &req.Document)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, map[string]any{"catalog": ToCatalogDTO(c), "rejected": rejected})
	return nil
}

func (s *server) putGroup(w http.ResponseWriter, r *http.Request) error {
	version, err := pathVersion(r)
	if err != nil {
		return err
	}
	var g vocab.Group
	if err := readJSON(r, &g); err != nil {
		return err
	}
	g.Key = r.PathValue("key")
	if err := s.o.Catalogs.PutGroup(r.Context(), r.PathValue("id"), version, &g); err != nil {
		return err
	}
	if len(g.Values) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	added, rejected, err := s.o.Catalogs.AddValues(r.Context(), r.PathValue("id"), version, g.Key, g.Values)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"added": added, "rejected": rejected})
	return nil
}

func (s *server) deleteGroup(w http.ResponseWriter, r *http.Request) error {
	version, err := pathVersion(r)
	if err != nil {
		return err
	}
	if err := s.o.Catalogs.DeleteGroup(r.Context(), r.PathValue("id"), version, r.PathValue("key")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) addValues(w http.ResponseWriter, r *http.Request) error {
	version, err := pathVersion(r)
	if err != nil {
		return err
	}
	var req struct {
		Values []vocab.Entry `json:"values"`
	}
	if err := readJSON(r, &req); err != nil {
		return err
	}
	added, rejected, err := s.o.Catalogs.AddValues(r.Context(), r.PathValue("id"), version, r.PathValue("key"), req.Values)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"added": added, "rejected": rejected})
	return nil
}

func (s *server) deleteValue(w http.ResponseWriter, r *http.Request) error {
	version, err := pathVersion(r)
	if err != nil {
		return err
	}
	value := r.URL.Query().Get("value")
	if value == "" {
		return badRequest("the value to delete goes in ?value=")
	}
	if err := s.o.Catalogs.DeleteValue(r.Context(), r.PathValue("id"), version, r.PathValue("key"), value); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) error {
	job, err := s.o.Catalogs.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToJobDTO(job))
	return nil
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request) error {
	job, err := s.o.Catalogs.CancelJob(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusAccepted, ToJobDTO(job))
	return nil
}

// Workers and tokens

func (s *server) listWorkers(w http.ResponseWriter, r *http.Request) error {
	var rows []store.Worker
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.ListWorkers(ctx)
		return err
	})
	if err != nil {
		return err
	}
	out := make([]WorkerDTO, len(rows))
	for i := range rows {
		out[i] = ToWorkerDTO(&rows[i])
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *server) getWorker(w http.ResponseWriter, r *http.Request) error {
	var row *store.Worker
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		row, err = tx.GetWorker(ctx, r.PathValue("id"))
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return notFound("worker", r.PathValue("id"))
	}
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToWorkerDTO(row))
	return nil
}

func (s *server) deleteWorker(w http.ResponseWriter, r *http.Request) error {
	if err := s.o.Core.DeleteWorker(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) drainWorker(w http.ResponseWriter, r *http.Request) error {
	if err := s.o.Core.DrainWorker(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *server) revokeWorker(w http.ResponseWriter, r *http.Request) error {
	if err := s.o.Core.RevokeWorker(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) createJoinToken(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		MaxUses int    `json:"max_uses"`
		TTL     string `json:"ttl"`
	}
	if err := readOptionalJSON(r, &req); err != nil {
		return err
	}
	var ttl time.Duration
	if req.TTL != "" {
		var err error
		if ttl, err = time.ParseDuration(req.TTL); err != nil || ttl <= 0 {
			return badRequest("ttl must be a duration such as 24h")
		}
	}
	if req.MaxUses < 0 {
		return badRequest("max_uses cannot be negative")
	}
	id, secret, expires, err := s.o.Core.CreateJoinToken(r.Context(), req.MaxUses, ttl)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, map[string]any{"id": id, "token": secret, "max_uses": req.MaxUses, "expires_at": expires})
	return nil
}

func (s *server) createToken(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		Name string `json:"name"`
		Role string `json:"role"`
		TTL  string `json:"ttl"`
	}
	if err := readJSON(r, &req); err != nil {
		return err
	}
	if strings.TrimSpace(req.Name) == "" {
		return badRequest("name is required")
	}
	if !ValidRole(req.Role) {
		return badRequest("role must be viewer, operator or admin")
	}
	now := s.o.Clock.Now()
	token := NewToken()
	row := &store.APIToken{ID: ids.New("tok"), Name: req.Name, Hash: HashToken(token), Role: req.Role, CreatedAt: now}
	if req.TTL != "" {
		ttl, err := time.ParseDuration(req.TTL)
		if err != nil || ttl <= 0 {
			return badRequest("ttl must be a duration such as 720h")
		}
		expires := now.Add(ttl)
		row.ExpiresAt = &expires
	}
	err := s.o.DB.Write(r.Context(), func(ctx context.Context, tx store.Tx) error { return tx.InsertAPIToken(ctx, row) })
	if err != nil {
		return err
	}
	d := ToTokenDTO(row)
	d.Token = token // shown only this once
	s.writeJSON(w, http.StatusCreated, d)
	return nil
}

func (s *server) listTokens(w http.ResponseWriter, r *http.Request) error {
	var rows []store.APIToken
	err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.ListAPITokens(ctx)
		return err
	})
	if err != nil {
		return err
	}
	out := make([]TokenDTO, len(rows))
	for i := range rows {
		out[i] = ToTokenDTO(&rows[i])
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": out})
	return nil
}

func (s *server) revokeToken(w http.ResponseWriter, r *http.Request) error {
	err := s.o.DB.Write(r.Context(), func(ctx context.Context, tx store.Tx) error {
		return tx.RevokeAPIToken(ctx, r.PathValue("id"), s.o.Clock.Now())
	})
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// backup streams a consistent copy of the database. Output files are not included: they
// are immutable, so copying them separately is safe, and they can be very large.
func (s *server) backup(w http.ResponseWriter, r *http.Request) error {
	if s.o.Backup == nil {
		return apperr.New(apperr.Unavailable, "ERR_NOT_AVAILABLE", "backup is not available")
	}
	placeholder, err := s.o.Artifacts.StagingFile()
	if err != nil {
		return err
	}
	path := placeholder.Name() + ".db"
	if err := errors.Join(placeholder.Close(), os.Remove(placeholder.Name())); err != nil {
		return err
	}
	defer os.Remove(path)
	if err := s.o.Backup(r.Context(), path); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("Content-Disposition", `attachment; filename="datafeed-backup.db"`)
	noWriteDeadline(w)
	_, err = io.Copy(w, f)
	if err != nil && r.Context().Err() == nil {
		s.o.Logger.Warn("send backup", "err", err)
	}
	return nil
}

// verify reads output files and compares them with their records, for one session or
// every finished one. Only one runs at a time, as it reads everything from disk.
func (s *server) verify(w http.ResponseWriter, r *http.Request) error {
	select {
	case s.verifying <- struct{}{}:
		defer func() { <-s.verifying }()
	default:
		return apperr.New(apperr.Conflict, "ERR_VERIFY_RUNNING", "a verification is already running")
	}
	ids := []string{}
	if id := r.URL.Query().Get("session"); id != "" {
		ids = append(ids, id)
	} else {
		var rows []store.Session
		err := s.o.DB.Read(r.Context(), func(ctx context.Context, tx store.Tx) error {
			var err error
			rows, err = tx.SessionsInStates(ctx, session.Completed, session.Stopped, session.Failed)
			return err
		})
		if err != nil {
			return err
		}
		for i := range rows {
			ids = append(ids, rows[i].ID)
		}
	}
	noWriteDeadline(w)
	reports := make([]coord.VerifyReport, 0, len(ids))
	for _, id := range ids {
		report, err := s.o.Core.VerifySession(r.Context(), id)
		if err != nil {
			return err
		}
		reports = append(reports, report)
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"items": reports})
	return nil
}
