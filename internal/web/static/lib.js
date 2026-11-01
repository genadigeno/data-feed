// Shared pieces: API calls, routing, formatting and small components.
import { html, useState, useEffect, useRef, useCallback } from "./vendor/preact-htm.js";

export { html, useState, useEffect, useRef, useCallback };

const API = "/api/v1";

export class ApiError extends Error {
  constructor(status, body) {
    const e = (body && body.error) || {};
    super(e.message || `The server answered ${status}`);
    this.status = status;
    this.code = e.code || "";
    this.problems = e.problems || [];
  }
}

// The header proves to the server that this page made the request: a cross-site form can't set it.
export async function api(method, path, body, { raw = false, contentType } = {}) {
  const headers = { "X-Datafeed-UI": "1" };
  const init = { method, headers, credentials: "same-origin" };
  if (body !== undefined) {
    headers["Content-Type"] = contentType || "application/json";
    init.body = typeof body === "string" ? body : JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch(API + path, init);
  } catch {
    throw new ApiError(0, { error: { code: "ERR_OFFLINE", message: "The server cannot be reached." } });
  }
  if (!res.ok) {
    let parsed = null;
    try { parsed = await res.json(); } catch { /* not JSON: the status has to do */ }
    const err = new ApiError(res.status, parsed);
    if (res.status === 401) authListeners.forEach((f) => f());
    throw err;
  }
  if (raw) return res.text();
  if (res.status === 204) return null;
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

const authListeners = new Set();
export function onAuthLost(f) {
  authListeners.add(f);
  return () => authListeners.delete(f);
}

// Routing: real paths, with the server answering index.html for all of them.
const routeListeners = new Set();
export function navigate(to, { replace = false } = {}) {
  history[replace ? "replaceState" : "pushState"](null, "", to);
  routeListeners.forEach((f) => f());
  window.scrollTo(0, 0);
}
addEventListener("popstate", () => routeListeners.forEach((f) => f()));

export function usePath() {
  const [path, setPath] = useState(location.pathname);
  useEffect(() => {
    const update = () => setPath(location.pathname + location.search);
    routeListeners.add(update);
    return () => routeListeners.delete(update);
  }, []);
  return path;
}

export function Link({ to, children, ...rest }) {
  const click = (e) => {
    if (e.ctrlKey || e.metaKey || e.shiftKey || e.button !== 0) return;
    e.preventDefault();
    navigate(to);
  };
  return html`<a href=${to} onClick=${click} ...${rest}>${children}</a>`;
}

// Loads something, and reloads on demand. A later answer always wins over an earlier one.
export function useLoad(fn, deps = []) {
  const [state, setState] = useState({ loading: true, data: null, error: null });
  const latest = useRef(0);
  const reload = useCallback(async () => {
    const n = ++latest.current;
    try {
      const data = await fn();
      if (n === latest.current) setState({ loading: false, data, error: null });
    } catch (error) {
      if (n === latest.current) setState((s) => ({ loading: false, data: s.data, error }));
    }
  }, deps);
  useEffect(() => {
    setState({ loading: true, data: null, error: null });
    reload();
    return () => { latest.current++; };
  }, deps);
  return { ...state, reload };
}

// Live updates. The stream only says something changed; what is shown is read from the API
// afterwards, at most about once a second, so it can't drift from the server's state.
export function useLive(session, onChange, { gap = 1000 } = {}) {
  const callback = useRef(onChange);
  callback.current = onChange;
  useEffect(() => {
    const source = new EventSource(API + "/events" + (session ? "?session=" + encodeURIComponent(session) : ""));
    let timer = null;
    let last = 0;
    const fire = () => {
      if (timer) return;
      const wait = Math.max(0, gap - (Date.now() - last));
      timer = setTimeout(() => {
        timer = null;
        last = Date.now();
        callback.current();
      }, wait);
    };
    ["state", "progress", "artifact", "job", "resync"].forEach((t) => source.addEventListener(t, fire));
    return () => {
      clearTimeout(timer);
      source.close();
    };
  }, [session, gap]);
}

// Toasts
let pushToast = () => {};
export function toast(message, kind = "") { pushToast(message, kind); }
export function showError(err) { toast(sentence(err.message || String(err)), "bad"); }

export function Toasts() {
  const [items, setItems] = useState([]);
  const next = useRef(0);
  useEffect(() => {
    pushToast = (message, kind) => {
      const id = ++next.current;
      setItems((list) => [...list, { id, message, kind }]);
      setTimeout(() => setItems((list) => list.filter((t) => t.id !== id)), 6000);
    };
  }, []);
  return html`<div class="toasts" role="status" aria-live="polite">
    ${items.map((t) => html`<div class="toast ${t.kind}" key=${t.id}>${t.message}</div>`)}
  </div>`;
}

// Formatting. Counts arrive as strings, because they can exceed what a JavaScript number holds.
export function count(v) {
  if (v === undefined || v === null || v === "") return "–";
  return String(v).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function bytes(v) {
  if (v === undefined || v === null || v === "") return "–";
  let n = Number(v);
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < units.length - 1);
  return `${n.toFixed(1)} ${units[i]}`;
}

export function rate(v) {
  if (!v) return "0/s";
  return `${count(Math.round(v))}/s`;
}

export function when(v) {
  if (!v) return "–";
  const d = new Date(v);
  return isNaN(d) ? "–" : d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "medium" });
}

const STATE_KIND = {
  COMPLETED: "ok", ACTIVE: "ok", PUBLISHED: "ok",
  RUNNING: "run", STARTING: "run", QUEUED: "run", ONLINE: "run",
  PAUSING: "warn", PAUSED: "warn", STOPPING: "warn", DRAINING: "warn", PARTIAL: "warn", DRAFT: "warn",
  FAILED: "bad", REVOKED: "bad",
};

// The word is always there: colour is never the only sign of a state.
export function Badge({ state }) {
  return html`<span class="badge ${STATE_KIND[state] || "idle"}">${state}</span>`;
}

export function Stat({ label, value, note }) {
  return html`<div class="panel stat">
    <div class="label">${label}</div>
    <div class="value">${value}</div>
    ${note && html`<div class="note">${note}</div>`}
  </div>`;
}

export function Page({ title, sub, actions, children }) {
  useEffect(() => { document.title = title ? `${title} · datafeed` : "datafeed"; }, [title]);
  return html`<div>
    <div class="head">
      <div class="grow"><h1>${title}</h1>${sub && html`<div class="sub">${sub}</div>`}</div>
      ${actions}
    </div>
    ${children}
  </div>`;
}

// The server's messages start in lower case, which suits a command line better than a page.
function sentence(text) {
  return text ? text[0].toUpperCase() + text.slice(1) : text;
}

export function ErrorNotice({ error }) {
  if (!error) return null;
  return html`<div class="notice bad" role="alert">
    ${sentence(error.message)}
    ${error.problems && error.problems.length > 0 && html`<ul>
      ${error.problems.map((p) => html`<li><code>${p.path || "(document)"}</code>: ${p.message}</li>`)}
    </ul>`}
  </div>`;
}

export function Loading({ state, children }) {
  if (state.loading && !state.data) return html`<div class="empty">Loading…</div>`;
  if (state.error && !state.data) return html`<${ErrorNotice} error=${state.error} />`;
  return html`${state.error && html`<${ErrorNotice} error=${state.error} />`}${children(state.data)}`;
}

// A button that runs something, staying disabled until it ends.
export function Action({ run, children, confirm: question, ...rest }) {
  const [busy, setBusy] = useState(false);
  const click = async () => {
    if (question && !window.confirm(question)) return;
    setBusy(true);
    try {
      await run();
    } catch (err) {
      showError(err);
    } finally {
      setBusy(false);
    }
  };
  return html`<button type="button" disabled=${busy} onClick=${click} ...${rest}>${children}</button>`;
}

export function Tabs({ tabs, active, onChange }) {
  return html`<div class="tabs" role="tablist">
    ${tabs.map(([id, label]) => html`<button type="button" role="tab" aria-selected=${active === id} onClick=${() => onChange(id)}>${label}</button>`)}
  </div>`;
}

export function Table({ head, rows, empty, numeric = [] }) {
  if (rows.length === 0) return html`<div class="empty">${empty}</div>`;
  return html`<div class="scroll"><table>
    <thead><tr>${head.map((h, i) => html`<th class=${numeric.includes(i) ? "num" : ""} scope="col">${h}</th>`)}</tr></thead>
    <tbody>${rows.map((row) => html`<tr>${row.map((c, i) => html`<td class=${numeric.includes(i) ? "num" : ""}>${c}</td>`)}</tr>`)}</tbody>
  </table></div>`;
}

export function Chart({ points, label }) {
  if (points.length < 2) return html`<div class="empty">Not enough to draw: history builds up in ten-second steps.</div>`;
  const peak = Math.max(...points, 1);
  const line = points.map((v, i) => `${(i / (points.length - 1)) * 100},${40 - (v / peak) * 38}`).join(" ");
  return html`<svg class="chart" viewBox="0 0 100 40" preserveAspectRatio="none" role="img" aria-label=${label}>
    <line class="floor" x1="0" y1="40" x2="100" y2="40" />
    <polyline points=${line} />
  </svg>`;
}
