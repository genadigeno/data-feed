// Dashboard and sessions.
import {
  html, useState, useEffect, api, navigate, Link, useLoad, useLive, toast, showError, count, bytes, rate, when,
  Badge, Stat, Page, ErrorNotice, Loading, Action, Tabs, Table, Chart,
} from "./lib.js";

const STATES = ["CREATED", "QUEUED", "STARTING", "RUNNING", "PAUSING", "PAUSED", "STOPPING", "COMPLETED", "STOPPED", "FAILED"];
const ACTIVE = ["QUEUED", "STARTING", "RUNNING", "PAUSING", "PAUSED", "STOPPING"];

const LIMITED_BY = {
  output: "Output: generators are waiting for the encoder",
  generation: "Generation: the encoder is waiting for records",
  delivery: "Delivery: parts are waiting to reach the Coordinator",
  rate: "The configured rate",
};

function sessionRows(items) {
  return items.map((s) => [
    html`<${Link} to=${"/sessions/" + s.id}>${s.name}</${Link}><div class="sub mono">${s.id}</div>`,
    html`<${Badge} state=${s.state} />`,
    s.mode, s.format,
    count(s.progress.records), bytes(s.progress.bytes),
    when(s.created_at),
  ]);
}

const SESSION_HEAD = ["Name", "State", "Mode", "Format", "Records", "Size", "Created"];

export function Dashboard() {
  const state = useLoad(() => api("GET", "/dashboard"));
  useLive("", state.reload, { gap: 2000 });
  return html`<${Page} title="Dashboard" actions=${html`<${Link} class="button primary" to="/sessions/new">New session</${Link}>`}>
    <${Loading} state=${state}>${(d) => {
      const running = ACTIVE.reduce((n, k) => n + (d.sessions[k] || 0), 0);
      const workers = Object.values(d.workers).reduce((a, b) => a + b, 0);
      return html`
        <div class="grid">
          <${Stat} label="Active sessions" value=${running} note=${`${d.sessions.COMPLETED || 0} completed, ${d.sessions.FAILED || 0} failed`} />
          <${Stat} label="Throughput now" value=${rate(d.current_rate)} note="across recent sessions" />
          <${Stat} label="Records stored" value=${count(d.records_committed)} note=${bytes(d.bytes_committed)} />
          <${Stat} label="Workers" value=${d.workers.ACTIVE || 0} note=${`active, of ${workers} registered`} />
        </div>
        <div class="panel"><h2>Recent sessions</h2>
          <${Table} head=${SESSION_HEAD} rows=${sessionRows(d.recent)} numeric=${[4, 5]}
            empty=${html`Nothing yet. <${Link} to="/sessions/new">Create the first session</${Link}>.`} />
        </div>`;
    }}</${Loading}>
  </${Page}>`;
}

export function Sessions() {
  const [filter, setFilter] = useState("");
  const [query, setQuery] = useState("");
  const [cursors, setCursors] = useState([""]);
  const cursor = cursors[cursors.length - 1];
  const params = new URLSearchParams({ limit: "25" });
  if (filter) params.set("state", filter);
  if (query) params.set("q", query);
  if (cursor) params.set("cursor", cursor);
  const state = useLoad(() => api("GET", "/sessions?" + params), [filter, query, cursor]);
  useLive("", state.reload, { gap: 2000 });
  const reset = (f) => (v) => { f(v); setCursors([""]); };
  return html`<${Page} title="Sessions" actions=${html`<${Link} class="button primary" to="/sessions/new">New session</${Link}>`}>
    <div class="panel">
      <div class="fields">
        <label>State
          <select value=${filter} onChange=${(e) => reset(setFilter)(e.target.value)}>
            <option value="">All states</option>
            ${STATES.map((s) => html`<option value=${s}>${s}</option>`)}
          </select>
        </label>
        <label>Name contains
          <input type="search" value=${query} onChange=${(e) => reset(setQuery)(e.target.value)} />
        </label>
      </div>
      <${Loading} state=${state}>${(page) => html`
        <${Table} head=${SESSION_HEAD} rows=${sessionRows(page.items)} numeric=${[4, 5]} empty="No sessions match." />
        <div class="row">
          ${cursors.length > 1 && html`<button type="button" onClick=${() => setCursors(cursors.slice(0, -1))}>Previous</button>`}
          ${page.next_cursor && html`<button type="button" onClick=${() => setCursors([...cursors, page.next_cursor])}>Next</button>`}
        </div>`}
      </${Loading}>
    </div>
  </${Page}>`;
}

const SAMPLE_SCHEMA = `kind: hybrid
fields:
  id: {type: uuid, unique: true}
  name: {type: full_name}
  age: {type: integer, min: 18, max: 90}
`;

export function NewSession() {
  const [form, setForm] = useState({
    name: "", mode: "batch", format: "ndjson", source: "inline", schemaId: "", document: SAMPLE_SCHEMA, catalog: "",
    count: "100000", rate: "", duration: "", seed: "", compression: "none",
  });
  const [problem, setProblem] = useState(null);
  const [checked, setChecked] = useState(null);
  const options = useLoad(async () => {
    const [schemas, builtins, catalogs] = await Promise.all([api("GET", "/schemas?limit=200"), api("GET", "/builtin-catalogs"), api("GET", "/catalogs?limit=200")]);
    return { schemas: schemas.items, builtins: builtins.items, catalogs: catalogs.items };
  });
  const set = (key) => (e) => { setForm({ ...form, [key]: e.target.value }); setChecked(null); };

  const spec = () => {
    const limits = {};
    if (form.count) limits.count = Number(form.count);
    if (form.rate) limits.rate = Number(form.rate);
    if (form.duration) limits.duration = form.duration;
    const out = {
      name: form.name || "Untitled session", mode: form.mode, limits,
      schema: form.source === "saved" ? { ref: { id: form.schemaId } } : { inline: form.document },
      output: { format: form.format, compression: form.compression },
    };
    if (form.seed) out.seed = Number(form.seed);
    if (form.catalog) out.catalog = { ref: { id: form.catalog } };
    return out;
  };
  const validate = async () => {
    setProblem(null);
    try {
      setChecked(await api("POST", "/sessions/validate", spec()));
    } catch (err) { setProblem(err); setChecked(null); }
  };
  const create = (start) => async () => {
    setProblem(null);
    try {
      const s = await api("POST", "/sessions" + (start ? "?start=true" : ""), spec());
      navigate("/sessions/" + s.id);
    } catch (err) { setProblem(err); }
  };

  return html`<${Page} title="New session" sub="Describe what to generate. Nothing is stored until you create it.">
    <${ErrorNotice} error=${problem} />
    ${checked && html`<div class="notice">Valid: a ${checked.strategy} session.</div>`}
    <div class="panel">
      <div class="fields">
        <label>Name<input value=${form.name} onInput=${set("name")} placeholder="Untitled session" /></label>
        <label>Mode
          <select value=${form.mode} onChange=${set("mode")}>
            <option value="batch">Batch: a set quantity</option>
            <option value="stream">Stream: until a limit is reached</option>
          </select>
        </label>
        <label>Format
          <select value=${form.format} onChange=${set("format")}>
            ${["ndjson", "json", "csv", "xml"].map((f) => html`<option value=${f}>${f}</option>`)}
          </select>
        </label>
        <label>Compression
          <select value=${form.compression} onChange=${set("compression")}>
            <option value="none">None</option><option value="gzip">gzip (no merged download)</option>
          </select>
        </label>
      </div>
      <div class="fields">
        <label>Records<input inputmode="numeric" value=${form.count} onInput=${set("count")} />
          <span class="hint">For a relational schema, its root table's rows</span></label>
        <label>Rate per second<input inputmode="decimal" value=${form.rate} onInput=${set("rate")} placeholder="As fast as possible" /></label>
        <label>Duration<input value=${form.duration} onInput=${set("duration")} placeholder="10m" />
          <span class="hint">Stream mode</span></label>
        <label>Seed<input inputmode="numeric" value=${form.seed} onInput=${set("seed")} placeholder="Chosen for you" />
          <span class="hint">The same seed gives the same bytes</span></label>
      </div>
    </div>
    <div class="panel">
      <div class="fields">
        <label>Schema
          <select value=${form.source} onChange=${set("source")}>
            <option value="inline">Written here</option>
            <option value="saved">A saved schema</option>
          </select>
        </label>
        ${form.source === "saved" && html`<label>Saved schema
          <select value=${form.schemaId} onChange=${set("schemaId")}>
            <option value="">Choose…</option>
            ${options.data && options.data.schemas.map((s) => html`<option value=${s.id}>${s.name} (v${s.version})</option>`)}
          </select></label>`}
        <label>Vocabulary catalog
          <select value=${form.catalog} onChange=${set("catalog")}>
            <option value="">None</option>
            ${options.data && html`
              <optgroup label="Built in">${options.data.builtins.map((b) => html`<option value=${b.id}>${b.name}</option>`)}</optgroup>
              <optgroup label="Saved">${options.data.catalogs.map((c) => html`<option value=${c.id}>${c.id} (latest published)</option>`)}</optgroup>`}
          </select></label>
      </div>
      ${form.source === "inline" && html`<label>Schema document (YAML or JSON)
        <textarea spellcheck="false" value=${form.document} onInput=${set("document")} /></label>`}
      <div class="row">
        <${Action} run=${create(true)} class="primary">Create and start</${Action}>
        <${Action} run=${create(false)}>Create</${Action}>
        <${Action} run=${validate}>Check</${Action}>
      </div>
    </div>
  </${Page}>`;
}

const ACTION_LABEL = { start: "Start", pause: "Pause", resume: "Resume", stop: "Stop", clone: "Clone", retry: "Retry", delete: "Delete", configure: null };

function SessionActions({ s, reload }) {
  const act = (name) => async () => {
    if (name === "delete") {
      await api("DELETE", "/sessions/" + s.id);
      toast("Deleted. Its files stay on disk, outside managed storage.");
      navigate("/sessions");
      return;
    }
    const out = await api("POST", `/sessions/${s.id}/${name}`, name === "stop" ? { mode: "graceful" } : undefined);
    if (name === "clone" || name === "retry") {
      navigate("/sessions/" + out.id);
      return;
    }
    await reload();
  };
  const buttons = s.available_actions.filter((a) => ACTION_LABEL[a]);
  return html`<div class="row">
    ${buttons.map((a) => html`<${Action} run=${act(a)}
      class=${a === "start" || a === "resume" ? "primary" : a === "delete" ? "danger" : ""}
      confirm=${a === "delete" ? `Delete session "${s.name}"?` : a === "stop" ? "Stop this session? Output so far stays." : undefined}>${ACTION_LABEL[a]}</${Action}>`)}
    ${s.state === "STOPPING" && !s.available_actions.includes("stop") && html`<${Action}
      run=${async () => { await api("POST", `/sessions/${s.id}/stop`, { mode: "immediate" }); await reload(); }}
      class="danger" confirm="Discard what is in progress?">Stop now</${Action}>`}
  </div>`;
}

function Overview({ s }) {
  const history = useLoad(() => api("GET", `/sessions/${s.id}/metrics?history=1h`), [s.id, s.progress.records]);
  const points = (history.data ? history.data.samples : []).map((x) => Number(x.records) / 10);
  const why = s.pressure && LIMITED_BY[s.pressure.limited_by];
  return html`
    <div class="grid">
      <${Stat} label="Records" value=${count(s.progress.records)} note=${s.progress.expected_records ? `of about ${count(s.progress.expected_records)}` : s.progress.in_flight_records !== "0" ? `${count(s.progress.in_flight_records)} on their way` : ""} />
      <${Stat} label="Size" value=${bytes(s.progress.bytes)} />
      <${Stat} label="Rate now" value=${rate(s.rates.current)} note=${`average ${rate(s.rates.average)}, peak ${rate(s.rates.peak)}`} />
      <${Stat} label="Units" value=${s.units ? s.units.committed : "–"} note=${s.units ? `done; ${s.units.leased} running, ${s.units.pending} waiting` : ""} />
    </div>
    ${s.progress.fraction !== undefined && html`<div class="panel">
      <h2>Progress: ${(s.progress.fraction * 100).toFixed(1)}%</h2>
      <progress max="1" value=${s.progress.fraction} aria-label="Progress"></progress>
    </div>`}
    ${s.errors.last && html`<div class="notice bad" role="alert"><strong>${s.errors.count} error${s.errors.count === 1 ? "" : "s"}.</strong> Last: ${s.errors.last}</div>`}
    ${s.artifacts_degraded && html`<div class="notice warn">Some output files were removed or found damaged. A retry generates the same data again.</div>`}
    <div class="two">
      <div class="panel"><h2>Throughput, records per second</h2><${Chart} points=${points} label="Throughput over the last hour" />
        ${why && html`<div class="sub">Limited by: ${why}</div>`}</div>
      <div class="panel"><h2>Details</h2>
        <dl class="kv">
          <dt>Strategy</dt><dd>${s.strategy}</dd>
          <dt>Mode</dt><dd>${s.mode}</dd>
          <dt>Format</dt><dd>${s.format}</dd>
          <dt>Seed</dt><dd class="mono">${s.seed}</dd>
          <dt>Reference time</dt><dd>${when(s.reference_time)}</dd>
          ${s.catalog && html`<dt>Catalog</dt><dd class="mono">${s.catalog.id} v${s.catalog.version}</dd>`}
          ${s.limits.rate > 0 && html`<dt>Rate limit</dt><dd>${rate(s.limits.rate)}</dd>`}
          ${s.limits.duration_ms > 0 && html`<dt>Duration limit</dt><dd>${s.limits.duration_ms / 1000} s</dd>`}
          <dt>Created</dt><dd>${when(s.created_at)}</dd>
          <dt>Started</dt><dd>${when(s.started_at)}</dd>
          <dt>Ended</dt><dd>${when(s.ended_at)}</dd>
          ${s.created_by && html`<dt>Created by</dt><dd>${s.created_by}</dd>`}
          ${s.retry_of && html`<dt>Retry of</dt><dd><${Link} to=${"/sessions/" + s.retry_of}>${s.retry_of}</${Link}></dd>`}
        </dl>
      </div>
    </div>`;
}

function Output({ s }) {
  const state = useLoad(() => api("GET", `/sessions/${s.id}/artifacts`), [s.id, s.progress.records]);
  return html`<${Loading} state=${state}>${(a) => {
    if (a.items.length === 0) return html`<div class="panel empty">No output yet.</div>`;
    return a.items.map((item) => html`<div class="panel">
      <div class="head">
        <div class="grow"><h2>${item.stream}</h2>
          <div class="sub">${count(item.records)} records, ${bytes(item.bytes)}, ${item.parts.length} part${item.parts.length === 1 ? "" : "s"}. <${Badge} state=${item.status === "COMPLETE" ? "COMPLETED" : item.status} /></div></div>
        ${item.merged_url && html`<a class="button primary" href=${item.merged_url} download>Download as one file</a>`}
      </div>
      <${Table} head=${["Part", "Indices", "Records", "Size", "Checksum", ""]} numeric=${[2, 3]} empty=""
        rows=${item.parts.map((p) => [
          p.n, `${count(p.first_index)} – ${count(p.end_index)}`, count(p.records), bytes(p.bytes),
          html`<code title=${p.sha256}>${p.sha256.slice(0, 12)}…</code>`,
          html`<a href=${p.download_url} download>Download</a>`,
        ])} />
    </div>`);
  }}</${Loading}>`;
}

function Logs({ s }) {
  const [level, setLevel] = useState("info");
  const state = useLoad(() => api("GET", `/sessions/${s.id}/logs?limit=500&level=${level}`), [s.id, level, s.state, s.progress.records]);
  return html`<div class="panel">
    <label>Level
      <select value=${level} onChange=${(e) => setLevel(e.target.value)}>
        ${["debug", "info", "warn", "error"].map((l) => html`<option value=${l}>${l}</option>`)}
      </select>
    </label>
    <${Loading} state=${state}>${(d) => d.items.length === 0 ? html`<div class="empty">Nothing logged at this level.</div>` : d.items.map((e) => html`
      <div class="logline ${e.level}"><time>${new Date(e.time).toLocaleTimeString()}</time>${e.msg}
        ${e.attrs && Object.keys(e.attrs).sort().map((k) => ` ${k}=${e.attrs[k]}`).join("")}</div>`)}
    </${Loading}>
  </div>`;
}

function History({ s }) {
  const state = useLoad(() => api("GET", `/sessions/${s.id}/events`), [s.id, s.state]);
  return html`<div class="panel"><${Loading} state=${state}>${(d) => html`
    <${Table} head=${["Time", "From", "To", "Why", "By"]} empty="No changes yet."
      rows=${d.items.map((e) => [when(e.at), e.from ? html`<${Badge} state=${e.from} />` : "–", html`<${Badge} state=${e.to} />`, e.reason || e.event, e.actor || "–"])} />`}
  </${Loading}></div>`;
}

function Spec({ s }) {
  const state = useLoad(() => api("GET", `/sessions/${s.id}?include=spec`), [s.id]);
  return html`<div class="panel"><${Loading} state=${state}>${(d) => html`<pre class="output">${JSON.stringify(d.spec, null, 2)}</pre>`}</${Loading}></div>`;
}

export function SessionDetail({ id }) {
  const [tab, setTab] = useState("overview");
  const state = useLoad(() => api("GET", "/sessions/" + id), [id]);
  useLive(id, state.reload);
  const s = state.data;
  return html`<${Page} title=${s ? s.name : "Session"} sub=${id} actions=${s && html`<${Badge} state=${s.state} /><${SessionActions} s=${s} reload=${state.reload} />`}>
    <${Loading} state=${state}>${(d) => html`
      ${d.state_reason && html`<div class="notice ${d.state === "FAILED" ? "bad" : ""}">${d.state_reason}</div>`}
      <${Tabs} active=${tab} onChange=${setTab} tabs=${[["overview", "Overview"], ["output", "Output"], ["logs", "Logs"], ["history", "History"], ["spec", "Specification"]]} />
      ${tab === "overview" && html`<${Overview} s=${d} />`}
      ${tab === "output" && html`<${Output} s=${d} />`}
      ${tab === "logs" && html`<${Logs} s=${d} />`}
      ${tab === "history" && html`<${History} s=${d} />`}
      ${tab === "spec" && html`<${Spec} s=${d} />`}`}
    </${Loading}>
  </${Page}>`;
}

export { showError, useEffect };
