// Schemas, vocabulary, Workers and tokens.
import {
  html, useState, api, navigate, Link, useLoad, useLive, toast, count, when, Badge, Page, ErrorNotice, Loading, Action, Table,
} from "./lib.js";

const SAMPLE_SCHEMA = `kind: hybrid
fields:
  id: {type: uuid, unique: true}
  name: {type: full_name}
`;

export function Schemas() {
  const state = useLoad(() => api("GET", "/schemas?limit=200"));
  return html`<${Page} title="Schemas" sub="Reusable descriptions of data." actions=${html`<${Link} class="button primary" to="/schemas/new">New schema</${Link}>`}>
    <div class="panel"><${Loading} state=${state}>${(d) => html`
      <${Table} head=${["Name", "Kind", "Version", "Created"]} empty="No saved schemas."
        rows=${d.items.map((s) => [html`<${Link} to=${"/schemas/" + s.id}>${s.name}</${Link}><div class="sub mono">${s.id}</div>`, s.kind, s.version, when(s.created_at)])} />`}
    </${Loading}></div>
  </${Page}>`;
}

export function SchemaEditor({ id }) {
  const isNew = id === "new";
  const [name, setName] = useState("");
  const [doc, setDoc] = useState(isNew ? SAMPLE_SCHEMA : null);
  const [problem, setProblem] = useState(null);
  const [preview, setPreview] = useState(null);
  const [seed, setSeed] = useState("1");
  const state = useLoad(async () => {
    if (isNew) return null;
    const s = await api("GET", "/schemas/" + id);
    setName(s.name);
    setDoc(JSON.stringify(s.definition, null, 2));
    return s;
  }, [id]);

  const check = async () => {
    setProblem(null);
    try {
      const out = await api("POST", "/schemas/validate", { document: doc });
      toast(`Valid: a ${out.kind} schema.`);
    } catch (err) { setProblem(err); }
  };
  const save = async () => {
    setProblem(null);
    try {
      const saved = isNew ? await api("POST", "/schemas", { name, document: doc }) : await api("PUT", "/schemas/" + id, { name, document: doc });
      toast(`Saved version ${saved.version}.`);
      if (isNew) navigate("/schemas/" + saved.id, { replace: true });
      else await state.reload();
    } catch (err) { setProblem(err); }
  };
  const showPreview = async () => {
    setProblem(null);
    try {
      setPreview(await api("POST", `/schemas/${id}/preview`, { count: 10, seed: Number(seed) || 0 }, { raw: true }));
    } catch (err) { setProblem(err); setPreview(null); }
  };
  const remove = async () => {
    await api("DELETE", "/schemas/" + id);
    navigate("/schemas");
  };
  const s = state.data;
  return html`<${Page} title=${isNew ? "New schema" : name || "Schema"} sub=${s ? `${s.id}, version ${s.version}` : ""}
    actions=${!isNew && html`<${Action} run=${remove} class="danger" confirm="Delete this schema and its versions?">Delete</${Action}>`}>
    <${ErrorNotice} error=${problem} />
    ${doc !== null && html`<div class="panel">
      <label>Name<input value=${name} onInput=${(e) => setName(e.target.value)} /></label>
      <label>Document (YAML or JSON)
        <textarea spellcheck="false" value=${doc} onInput=${(e) => setDoc(e.target.value)} /></label>
      <div class="row">
        <${Action} run=${save} class="primary">${isNew ? "Save" : "Save new version"}</${Action}>
        <${Action} run=${check}>Check</${Action}>
      </div>
    </div>`}
    ${!isNew && html`<div class="panel"><h2>Preview</h2>
      <div class="fields"><label>Seed<input inputmode="numeric" value=${seed} onInput=${(e) => setSeed(e.target.value)} /></label></div>
      <div class="row"><${Action} run=${showPreview}>Show 10 records</${Action}></div>
      ${preview !== null && html`<pre class="output">${preview}</pre>`}
      <div class="sub">Previews the saved version. A relational schema needs a session, or the API's table parameter.</div>
    </div>`}
  </${Page}>`;
}

export function Vocabulary() {
  const domains = useLoad(() => api("GET", "/domains?limit=200"));
  const catalogs = useLoad(() => api("GET", "/catalogs?limit=200"));
  const builtins = useLoad(() => api("GET", "/builtin-catalogs"));
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [problem, setProblem] = useState(null);
  const create = async () => {
    setProblem(null);
    try {
      await api("POST", "/domains", { name, description, locale: "en" });
      setName(""); setDescription("");
      await domains.reload();
    } catch (err) { setProblem(err); }
  };
  const domainName = (id) => {
    const d = domains.data && domains.data.items.find((x) => x.id === id);
    return d ? d.name : id;
  };
  return html`<${Page} title="Vocabulary" sub="Realistic values for generation. Catalogs freeze when published, so a session always reproduces.">
    <${ErrorNotice} error=${problem} />
    <div class="panel"><h2>Domains</h2>
      <${Loading} state=${domains}>${(d) => html`
        <${Table} head=${["Name", "Description", ""]} empty="No domains yet. A domain is what a catalog is about."
          rows=${d.items.map((x) => [html`<${Link} to=${"/domains/" + x.id}>${x.name}</${Link}>`, x.description || "–", html`<${Link} to=${"/domains/" + x.id}>Generate vocabulary</${Link}>`])} />`}
      </${Loading}>
      <div class="fields">
        <label>New domain<input value=${name} onInput=${(e) => setName(e.target.value)} placeholder="Online shop" /></label>
        <label>Description<input value=${description} onInput=${(e) => setDescription(e.target.value)} placeholder="What the data is about" /></label>
      </div>
      <${Action} run=${create}>Add domain</${Action}>
    </div>
    <div class="panel"><h2>Catalogs</h2>
      <${Loading} state=${catalogs}>${(d) => html`
        <${Table} head=${["Catalog", "Latest version", "Status", "Domain", "Created"]} empty="No catalogs yet."
          rows=${d.items.map((c) => [html`<${Link} class="mono" to=${"/catalogs/" + c.id}>${c.id}</${Link}>`, c.version, html`<${Badge} state=${c.status} />`, domainName(c.domain_id), when(c.created_at)])} />`}
      </${Loading}>
    </div>
    <div class="panel"><h2>Built in</h2>
      <${Loading} state=${builtins}>${(d) => html`
        <${Table} head=${["Catalog", "Groups", "Values", "About"]} numeric=${[1, 2]} empty=""
          rows=${d.items.map((b) => [html`${b.name}<div class="sub mono">${b.id}</div>`, b.groups, count(b.values), b.description])} />`}
      </${Loading}>
    </div>
  </${Page}>`;
}

export function DomainDetail({ id }) {
  const domain = useLoad(() => api("GET", "/domains/" + id), [id]);
  const catalogs = useLoad(() => api("GET", "/catalogs?limit=200&domain=" + encodeURIComponent(id)), [id]);
  const [size, setSize] = useState("LOW");
  const [values, setValues] = useState("100");
  const [groups, setGroups] = useState("5");
  const [job, setJob] = useState(null);
  const [problem, setProblem] = useState(null);
  const watched = useLoad(() => (job ? api("GET", "/vocabulary-jobs/" + job) : Promise.resolve(null)), [job]);
  useLive("", () => { if (job) { watched.reload(); catalogs.reload(); } });

  const start = async () => {
    setProblem(null);
    const body = { size };
    if (size === "CUSTOM") { body.values_per_group = Number(values); body.max_groups = Number(groups); }
    try {
      const j = await api("POST", `/domains/${id}/vocabulary/generate`, body);
      setJob(j.id);
    } catch (err) { setProblem(err); }
  };
  const remove = async () => {
    await api("DELETE", "/domains/" + id);
    navigate("/vocabulary");
  };
  const j = watched.data;
  const p = j && j.progress;
  return html`<${Page} title=${domain.data ? domain.data.name : "Domain"} sub=${domain.data && domain.data.description}
    actions=${html`<${Action} run=${remove} class="danger" confirm="Delete this domain?">Delete</${Action}>`}>
    <${ErrorNotice} error=${problem} />
    <div class="panel"><h2>Generate vocabulary</h2>
      <div class="sub">With the Anthropic provider this makes paid API calls. Start with the low size.</div>
      <div class="fields">
        <label>Size
          <select value=${size} onChange=${(e) => setSize(e.target.value)}>
            <option value="LOW">Low</option><option value="MEDIUM">Medium</option><option value="HIGH">High</option><option value="CUSTOM">Custom</option>
          </select>
        </label>
        ${size === "CUSTOM" && html`
          <label>Values per group<input inputmode="numeric" value=${values} onInput=${(e) => setValues(e.target.value)} /></label>
          <label>Groups<input inputmode="numeric" value=${groups} onInput=${(e) => setGroups(e.target.value)} /></label>`}
      </div>
      <${Action} run=${start} class="primary">Start job</${Action}>
      ${j && html`<div class="notice" style=${{ marginTop: "12px" }}>
        <${Badge} state=${j.state} />
        ${p && p.groups_planned > 0 && html` ${p.groups_done} of ${p.groups_planned} groups, ${count(p.values_accepted)} values accepted, ${count(p.values_rejected)} rejected.`}
        ${j.error && html` ${j.error}`}
        ${" "}<${Link} to=${"/catalogs/" + j.catalog_id}>Open the draft</${Link}>
        ${(j.state === "QUEUED" || j.state === "RUNNING") && html` <${Action} run=${async () => { await api("POST", `/vocabulary-jobs/${j.id}/cancel`); await watched.reload(); }}>Cancel</${Action}>`}
      </div>`}
    </div>
    <div class="panel"><h2>Catalogs</h2>
      <${Loading} state=${catalogs}>${(d) => html`
        <${Table} head=${["Catalog", "Latest version", "Status", "Created"]} empty="None yet."
          rows=${d.items.map((c) => [html`<${Link} class="mono" to=${"/catalogs/" + c.id}>${c.id}</${Link}>`, c.version, html`<${Badge} state=${c.status} />`, when(c.created_at)])} />`}
      </${Loading}>
    </div>
  </${Page}>`;
}

function GroupEditor({ catalog, group, onChange }) {
  const [text, setText] = useState("");
  const [result, setResult] = useState(null);
  const add = async () => {
    const values = text.split("\n").map((v) => v.trim()).filter(Boolean).map((value) => ({ value }));
    const out = await api("POST", `/catalogs/${catalog.id}/versions/${catalog.version}/groups/${group.key}/values`, { values });
    setResult(out);
    setText("");
    await onChange();
  };
  const remove = (value) => async () => {
    await api("DELETE", `/catalogs/${catalog.id}/versions/${catalog.version}/groups/${group.key}/values?value=${encodeURIComponent(value)}`);
    await onChange();
  };
  const draft = catalog.status === "DRAFT";
  return html`<div class="panel">
    <h2>${group.name || group.key} <code>${group.key}</code></h2>
    <div class="sub">${group.description} ${group.values.length} values, ${group.data_type}.</div>
    <${Table} head=${["Value", "Weight", ""]} numeric=${[1]} empty="No values."
      rows=${group.values.slice(0, 100).map((v) => [v.value, v.weight || 1, draft ? html`<${Action} run=${remove(v.value)}>Remove</${Action}>` : ""])} />
    ${group.values.length > 100 && html`<div class="sub">The first 100 of ${count(group.values.length)}. Export the catalog for all of them.</div>`}
    ${draft && html`
      <label>Add values, one per line<textarea spellcheck="false" value=${text} onInput=${(e) => setText(e.target.value)} /></label>
      <${Action} run=${add}>Add</${Action}>
      ${result && html`<div class="sub">Added ${result.added}. ${result.rejected && `Rejected: ${Object.entries(result.rejected).map(([k, n]) => `${n} ${k.replaceAll("_", " ")}`).join(", ")}.`}</div>`}`}
  </div>`;
}

export function CatalogDetail({ id }) {
  const [version, setVersion] = useState(0);
  const [open, setOpen] = useState(null);
  const state = useLoad(() => api("GET", `/catalogs/${id}${version ? "?version=" + version : ""}`), [id, version]);
  const versions = useLoad(() => api("GET", `/catalogs/${id}/versions`), [id]);
  const c = state.data && state.data.catalog;
  const refresh = async () => { await Promise.all([state.reload(), versions.reload()]); };
  const publish = async () => {
    await api("POST", `/catalogs/${id}/versions/${c.version}/publish`);
    toast("Published. Sessions can now use it.");
    await refresh();
  };
  const newVersion = async () => {
    const out = await api("POST", `/catalogs/${id}/versions`, { from: c.version });
    setVersion(out.version);
    await versions.reload();
  };
  const clone = async () => {
    const out = await api("POST", `/catalogs/${id}/clone`, { version: c.version });
    navigate("/catalogs/" + out.id);
  };
  return html`<${Page} title="Catalog" sub=${id} actions=${c && html`
    <${Badge} state=${c.status} />
    <div class="row">
      ${c.status === "DRAFT" && html`<${Action} run=${publish} class="primary" confirm="Freeze this version? It can't be changed afterwards.">Publish</${Action}>`}
      ${c.status === "PUBLISHED" && html`<${Action} run=${newVersion}>New version</${Action}>`}
      <${Action} run=${clone}>Clone</${Action}>
      <a class="button" href=${`/api/v1/catalogs/${id}/versions/${c.version}/export`} download>Export</a>
    </div>`}>
    <${Loading} state=${state}>${(d) => html`
      ${versions.data && versions.data.items.length > 1 && html`<div class="panel"><div class="fields"><label>Version
        <select value=${d.catalog.version} onChange=${(e) => setVersion(Number(e.target.value))}>
          ${versions.data.items.map((v) => html`<option value=${v.version}>${v.version} (${v.status.toLowerCase()})</option>`)}
        </select></label></div></div>`}
      ${d.catalog.content_hash && html`<div class="sub mono">${d.catalog.content_hash}</div>`}
      <div class="panel">
        <${Table} head=${["Group", "Type", "Values", "Category", ""]} numeric=${[2]} empty="This draft has no groups yet."
          rows=${d.groups.map((g) => [html`<code>${g.key}</code><div class="sub">${g.description}</div>`, g.data_type, count(g.values.length), g.category || "–",
            html`<button type="button" onClick=${() => setOpen(open === g.key ? null : g.key)}>${open === g.key ? "Hide" : "Values"}</button>`])} />
      </div>
      ${open && d.groups.filter((g) => g.key === open).map((g) => html`<${GroupEditor} catalog=${d.catalog} group=${g} onChange=${refresh} />`)}`}
    </${Loading}>
  </${Page}>`;
}

export function Workers({ me }) {
  const state = useLoad(() => api("GET", "/workers"));
  const [joined, setJoined] = useState(null);
  useLive("", state.reload, { gap: 5000 });
  const admin = me.role === "admin";
  const action = (id, verb) => async () => {
    await api("POST", `/workers/${id}/${verb}`);
    await state.reload();
  };
  const createJoin = async () => setJoined(await api("POST", "/worker-tokens", { max_uses: 1, ttl: "24h" }));
  return html`<${Page} title="Workers" sub="Processes that generate data.">
    <div class="panel"><${Loading} state=${state}>${(d) => html`
      <${Table} head=${["Name", "State", "Slots", "Platform", "Last seen", ""]} numeric=${[2]} empty="No Workers have registered."
        rows=${d.items.map((w) => {
          const caps = w.capabilities || {};
          return [
            html`${w.name}<div class="sub mono">${w.id}</div>`, html`<${Badge} state=${w.state} />`, w.slots,
            caps.os ? `${caps.os}/${caps.arch}` : "–", when(w.last_heartbeat_at),
            admin && w.id !== "local" && w.state !== "REVOKED" ? html`<div class="row">
              ${w.state !== "DRAINING" && html`<${Action} run=${action(w.id, "drain")}>Drain</${Action}>`}
              <${Action} run=${action(w.id, "revoke")} class="danger" confirm=${`Revoke ${w.name}? Its units go to others, and it can't return.`}>Revoke</${Action}>
            </div>` : "",
          ];
        })} />`}
    </${Loading}></div>
    ${admin && html`<div class="panel"><h2>Add a Worker</h2>
      <div class="sub">A join token lets one Worker register, within a day.</div>
      <div class="row" style=${{ marginTop: "8px" }}><${Action} run=${createJoin}>Create a join token</${Action}></div>
      ${joined && html`<div class="notice" style=${{ marginTop: "12px" }}>Shown only now. Run this where the Worker should be:
        <code class="secret">datafeed worker run --coordinator <url> --join-token ${joined.token}</code></div>`}
    </div>`}
  </${Page}>`;
}

export function Settings({ me }) {
  const state = useLoad(() => (me.role === "admin" ? api("GET", "/tokens") : Promise.resolve({ items: [] })));
  const [name, setName] = useState("");
  const [role, setRole] = useState("viewer");
  const [ttl, setTtl] = useState("");
  const [created, setCreated] = useState(null);
  const [problem, setProblem] = useState(null);
  const create = async () => {
    setProblem(null);
    try {
      setCreated(await api("POST", "/tokens", { name, role, ttl }));
      setName("");
      await state.reload();
    } catch (err) { setProblem(err); }
  };
  const revoke = (id) => async () => {
    await api("DELETE", "/tokens/" + id);
    await state.reload();
  };
  return html`<${Page} title="Settings">
    <div class="panel"><h2>You</h2>
      <dl class="kv"><dt>Name</dt><dd>${me.name}</dd><dt>Role</dt><dd>${me.role}</dd></dl>
    </div>
    ${me.role === "admin" && html`<div class="panel"><h2>API tokens</h2>
      <${ErrorNotice} error=${problem} />
      <${Loading} state=${state}>${(d) => html`
        <${Table} head=${["Name", "Role", "Created", "Expires", "Status", ""]} empty="No tokens."
          rows=${d.items.map((t) => [t.name, t.role, when(t.created_at), t.expires_at ? when(t.expires_at) : "Never", t.revoked_at ? "Revoked" : "Active",
            t.revoked_at || t.id === me.id ? "" : html`<${Action} run=${revoke(t.id)} class="danger" confirm=${`Revoke ${t.name}?`}>Revoke</${Action}>`])} />`}
      </${Loading}>
      <div class="fields" style=${{ marginTop: "12px" }}>
        <label>Name<input value=${name} onInput=${(e) => setName(e.target.value)} placeholder="What it is for" /></label>
        <label>Role
          <select value=${role} onChange=${(e) => setRole(e.target.value)}>
            <option value="viewer">Viewer: read</option><option value="operator">Operator: run sessions</option><option value="admin">Admin: everything</option>
          </select>
        </label>
        <label>Lasts<input value=${ttl} onInput=${(e) => setTtl(e.target.value)} placeholder="Forever, or 720h" /></label>
      </div>
      <${Action} run=${create}>Create token</${Action}>
      ${created && html`<div class="notice" style=${{ marginTop: "12px" }}>Shown only now: <code class="secret">${created.token}</code></div>`}
    </div>`}
  </${Page}>`;
}
