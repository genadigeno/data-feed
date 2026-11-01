package api

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/serial"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/store"
)

const (
	feedPageDefaultRecords = 1000
	// feedStreamStall is how long a stream may wait on a client that has stopped reading.
	// Without it, a stalled client would hold a stream slot for ever.
	feedStreamStall = time.Minute
	// feedCacheEntries bounds the compiled feeds kept ready, which can hold whole catalogs.
	feedCacheEntries = 16
)

// FeedDTO is a feed.
type FeedDTO struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Description   string          `json:"description,omitempty"`
	Strategy      string          `json:"strategy"`
	Format        string          `json:"format"`
	Seed          string          `json:"seed"`
	ReferenceTime time.Time       `json:"reference_time"`
	EngineVersion int             `json:"engine_version"`
	Rate          float64         `json:"rate,omitempty"`
	Streams       []FeedStreamDTO `json:"streams"`
	CreatedAt     time.Time       `json:"created_at"`
	CreatedBy     string          `json:"created_by,omitempty"`
}

// FeedStreamDTO is one stream of a feed. End is absent when the stream has no end.
type FeedStreamDTO struct {
	ID    string `json:"id"`
	End   string `json:"end,omitempty"`
	Dense bool   `json:"dense"`
}

// ToFeedDTO maps a feed.
func ToFeedDTO(f *store.Feed) FeedDTO {
	d := FeedDTO{
		ID: f.ID, Name: f.Name, Description: f.Description, Strategy: f.Strategy, Format: f.Format, Seed: u64(f.Seed),
		ReferenceTime: f.ReferenceTime, EngineVersion: f.EngineVersion, Rate: f.Rate, Streams: []FeedStreamDTO{},
		CreatedAt: f.CreatedAt, CreatedBy: f.CreatedBy,
	}
	var streams []session.FeedStream
	if json.Unmarshal([]byte(f.StreamsJSON), &streams) == nil {
		for _, st := range streams {
			out := FeedStreamDTO{ID: st.ID, Dense: st.Dense}
			if st.End > 0 {
				out.End = u64(st.End)
			}
			d.Streams = append(d.Streams, out)
		}
	}
	return d
}

// feedEntry is a compiled feed. Generators run concurrently once made; making them is what
// the lock covers.
type feedEntry struct {
	mu       sync.Mutex
	compiled *snapshot.Compiled
}

func (e *feedEntry) open(spec gen.StreamSpec) (serial.Encoder, gen.StreamGenerator, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	enc, err := e.compiled.NewEncoder(spec)
	if err != nil {
		return nil, nil, err
	}
	g, err := e.compiled.Engine.NewStreamGenerator(spec.ID)
	if err != nil {
		return nil, nil, err
	}
	return enc, g, nil
}

// feedCache keeps feeds compiled, as compiling on every page would cost more than generating
// the page. Snapshots never change, so an entry can't go stale.
type feedCache struct {
	mu      sync.Mutex
	entries map[string]*feedEntry
}

func (c *feedCache) get(hash string, compile func() (*snapshot.Compiled, error)) (*feedEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[hash]; ok {
		return e, nil
	}
	compiled, err := compile()
	if err != nil {
		return nil, err
	}
	if c.entries == nil {
		c.entries = map[string]*feedEntry{}
	}
	if len(c.entries) >= feedCacheEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	e := &feedEntry{compiled: compiled}
	c.entries[hash] = e
	return e, nil
}

// feedWindow is the range of positions one read covers.
type feedWindow struct {
	From, To uint64
	// Next is where the following read starts, if there is more.
	Next    uint64
	HasNext bool
}

// parseFeedWindow reads a window from a query. fromKey names the start, and limit is optional
// when defaultLimit is 0. A start at or past the end gives an empty window.
func parseFeedWindow(q url.Values, fromKey string, defaultLimit, maxLimit, end uint64) (feedWindow, error) {
	var w feedWindow
	if v := q.Get(fromKey); v != "" {
		from, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return w, badRequest(fromKey + " must be a whole number")
		}
		w.From = from
	}
	limit := defaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			return w, badRequest("limit must be a whole number above zero")
		}
		if maxLimit > 0 && n > maxLimit {
			return w, apperr.New(apperr.PayloadTooLarge, "ERR_LIMIT_TOO_LARGE", fmt.Sprintf("limit must be at most %d", maxLimit))
		}
		limit = n
	}
	if w.From >= end {
		w.To = w.From
		return w, nil
	}
	w.To = end
	if limit > 0 && limit < end-w.From {
		w.To = w.From + limit
	}
	w.HasNext = w.To < end
	if w.HasNext {
		w.Next = w.To
	}
	return w, nil
}

// parseFeedRate reads ?rate=, where the feed's own rate is the default and 0 means no limit.
func parseFeedRate(q url.Values, feedRate float64) (float64, error) {
	v := q.Get("rate")
	if v == "" {
		return feedRate, nil
	}
	rate, err := strconv.ParseFloat(v, 64)
	if err != nil || rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0, badRequest("rate must be a number of records per second, 0 for no limit")
	}
	return rate, nil
}

// chooseFeedStream picks the stream a read asks for.
func chooseFeedStream(streams []gen.StreamSpec, table string) (*gen.StreamSpec, error) {
	for i := range streams {
		if table == "" && len(streams) == 1 || table != "" && streams[i].ID == gen.TableStream(table) {
			return &streams[i], nil
		}
	}
	if table != "" {
		return nil, badRequest("this feed has no table " + table)
	}
	return nil, badRequest("choose a table with ?table= for a feed with several streams")
}

// canStream says whether a format can go on with no end. A JSON or XML document cut short
// isn't a document, and would have to be for a client to resume.
func canStream(format string) bool {
	return format == serial.FormatNDJSON || format == serial.FormatCSV
}

// feedMemory is what one read can hold, as the memory bound of a running unit.
func feedMemory(queueCap, batch int, maxRecord, recordCap int64) int64 {
	if maxRecord <= 0 || maxRecord > recordCap {
		maxRecord = recordCap
	}
	return int64(queueCap+2) * int64(batch) * maxRecord
}

// feedRead is what both ways of reading share.
type feedRead struct {
	row   *store.Feed
	entry *feedEntry
	spec  *gen.StreamSpec
}

func (s *server) openFeedRead(r *http.Request) (*feedRead, error) {
	row, err := s.o.Sessions.GetFeed(r.Context(), r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	entry, err := s.feeds.get(row.SnapshotHash, func() (*snapshot.Compiled, error) { return s.o.Sessions.CompileFeed(r.Context(), row) })
	if err != nil {
		return nil, err
	}
	spec, err := chooseFeedStream(entry.compiled.Engine.Streams(), r.URL.Query().Get("table"))
	if err != nil {
		return nil, err
	}
	return &feedRead{row: row, entry: entry, spec: spec}, nil
}

// reserveFeedMemory holds a read's share of the memory feeds may use. Reads run on the Coordinator, so
// they must not be able to take what it needs for everything else.
func (s *server) reserveFeedMemory(w http.ResponseWriter, need int64) (func(), error) {
	if s.feedMemory.Add(need) > int64(s.o.Config.API.FeedMaxMemoryBytes) {
		s.feedMemory.Add(-need)
		w.Header().Set("Retry-After", "1")
		return nil, apperr.New(apperr.Unavailable, "ERR_FEEDS_BUSY", "reads from feeds are using all the memory set aside for them (api.feed_max_memory_bytes)")
	}
	return func() { s.feedMemory.Add(-need) }, nil
}

// runFeed generates a window into w.
func (s *server) runFeed(rd *feedRead, r *http.Request, w io.Writer, window feedWindow, batch int, rate float64, live bool) (pipeline.Result, serial.Encoder, error) {
	enc, g, err := rd.entry.open(*rd.spec)
	if err != nil {
		return pipeline.Result{}, nil, err
	}
	snap := rd.entry.compiled.Snapshot
	unit := pipeline.Unit{
		Generators: []gen.StreamGenerator{g}, Stream: string(rd.spec.ID), From: window.From, End: window.To, BatchSize: batch, QueueCap: snap.QueueCapacity,
		Encoder: enc, Sink: &output.WriterSink{W: w}, Clock: s.o.Clock, MaxRecordBytes: rd.spec.MaxRecordBytes, RecordsPerSecond: rate, Pool: record.NewPool(batch),
		FlushBatches: live,
	}
	if live {
		unit.Finish = s.o.Done
	}
	res, err := pipeline.Run(r.Context(), unit)
	return res, enc, err
}

func (s *server) createFeed(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	row, err := s.o.Sessions.CreateFeed(r.Context(), body, principalOf(r).Name)
	if err != nil {
		return err
	}
	w.Header().Set("Location", prefix+"/feeds/"+row.ID)
	s.writeJSON(w, http.StatusCreated, ToFeedDTO(row))
	return nil
}

func (s *server) listFeeds(w http.ResponseWriter, r *http.Request) error {
	after, limit, err := s.page(r)
	if err != nil {
		return err
	}
	rows, err := s.o.Sessions.ListFeeds(r.Context(), after, limit+1)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, MakePage(rows, limit, func(row *store.Feed) string { return row.ID }, ToFeedDTO))
	return nil
}

func (s *server) getFeed(w http.ResponseWriter, r *http.Request) error {
	row, err := s.o.Sessions.GetFeed(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, ToFeedDTO(row))
	return nil
}

func (s *server) deleteFeed(w http.ResponseWriter, r *http.Request) error {
	if err := s.o.Sessions.DeleteFeed(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// feedRecords answers with one page of data. Each page is a complete document, and its
// position range is known before generating, so X-Next-Offset can go in the headers.
func (s *server) feedRecords(w http.ResponseWriter, r *http.Request) error {
	rd, err := s.openFeedRead(r)
	if err != nil {
		return err
	}
	cfg := s.o.Config
	window, err := parseFeedWindow(r.URL.Query(), "offset", feedPageDefaultRecords, uint64(cfg.API.FeedPageMaxRecords), session.StreamEnd(*rd.spec))
	if err != nil {
		return err
	}
	snap := rd.entry.compiled.Snapshot
	batch := snap.BatchSize
	if size := window.To - window.From; size > 0 && size < uint64(batch) {
		batch = int(size)
	}
	release, err := s.reserveFeedMemory(w, feedMemory(snap.QueueCapacity, batch, rd.spec.MaxRecordBytes, int64(cfg.Generation.MaxRecordSizeBytes)))
	if err != nil {
		return err
	}
	defer release()

	enc, _, err := rd.entry.open(*rd.spec)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", ContentTypeFor("x."+enc.Extension()))
	w.Header().Set("X-Seed", u64(rd.row.Seed))
	if window.HasNext {
		w.Header().Set("X-Next-Offset", u64(window.Next))
	}
	counted := &limitWriter{w: w, left: math.MaxInt64}
	res, enc, err := s.runFeed(rd, r, counted, window, batch, 0, false)
	if err != nil {
		if counted.wrote {
			// The status has gone out. Cutting the connection tells the client the body is incomplete.
			s.o.Logger.Warn("feed page stopped", "feed", rd.row.ID, "err", err)
			panic(http.ErrAbortHandler)
		}
		return err
	}
	if res.Records == 0 {
		// The window held no rows: a sparse table's gap, or past the end.
		_, err = w.Write(serial.EmptyDocument(enc))
		return err
	}
	return nil
}

// flushWriter sends what it is given straight away, and gives up on a client that doesn't
// take it in time.
type flushWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (f *flushWriter) Write(b []byte) (int, error) {
	_ = f.rc.SetWriteDeadline(time.Now().Add(feedStreamStall)) // not every writer can; then the limit stands
	n, err := f.w.Write(b)
	if err != nil {
		return n, err
	}
	return n, f.rc.Flush()
}

// feedStream sends records for as long as the client reads, at the pace it reads. A client
// resumes by counting the records it got, which is why this takes only dense streams.
func (s *server) feedStream(w http.ResponseWriter, r *http.Request) error {
	rd, err := s.openFeedRead(r)
	if err != nil {
		return err
	}
	if !canStream(rd.row.Format) {
		return badRequest("streaming needs a feed in ndjson or csv: use /records for " + rd.row.Format)
	}
	if rd.spec.PerParent != 1 {
		return badRequest("this table has gaps between positions, so a stream could not be resumed: use /records")
	}
	q := r.URL.Query()
	window, err := parseFeedWindow(q, "from", 0, 0, session.StreamEnd(*rd.spec))
	if err != nil {
		return err
	}
	rate, err := parseFeedRate(q, rd.row.Rate)
	if err != nil {
		return err
	}
	cfg := s.o.Config
	if s.feedStreams.Add(1) > int64(cfg.API.FeedMaxStreams) {
		s.feedStreams.Add(-1)
		w.Header().Set("Retry-After", "5")
		return apperr.New(apperr.Unavailable, "ERR_TOO_MANY_STREAMS", "too many feed streams are open (api.feed_max_streams)")
	}
	defer s.feedStreams.Add(-1)
	snap := rd.entry.compiled.Snapshot
	release, err := s.reserveFeedMemory(w, feedMemory(snap.QueueCapacity, snap.BatchSize, rd.spec.MaxRecordBytes, int64(cfg.Generation.MaxRecordSizeBytes)))
	if err != nil {
		return err
	}
	defer release()

	enc, _, err := rd.entry.open(*rd.spec)
	if err != nil {
		return err
	}
	rc := http.NewResponseController(w)
	noWriteDeadline(w)
	w.Header().Set("Content-Type", ContentTypeFor("x."+enc.Extension()))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Seed", u64(rd.row.Seed))
	w.Header().Set("X-Start-Offset", u64(window.From))
	w.WriteHeader(http.StatusOK)
	// A client should see the stream open even if the first record is a while away.
	if rc.Flush() != nil {
		return nil
	}
	_, _, err = s.runFeed(rd, r, &flushWriter{w: w, rc: rc}, window, snap.BatchSize, rate, true)
	if err != nil && r.Context().Err() == nil {
		s.o.Logger.Warn("feed stream stopped", "feed", rd.row.ID, "err", err)
		panic(http.ErrAbortHandler) // tells the client the stream did not end normally
	}
	return nil
}
