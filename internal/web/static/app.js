// The application: login, navigation and routes.
import { render } from "./vendor/preact-htm.js";
import { html, useState, useEffect, api, navigate, usePath, Link, onAuthLost, Toasts, ErrorNotice, Page } from "./lib.js";
import { Dashboard, Sessions, NewSession, SessionDetail } from "./sessions.js";
import { Schemas, SchemaEditor, Vocabulary, DomainDetail, CatalogDetail, Workers, Settings } from "./manage.js";

function Login({ onDone }) {
  const [token, setToken] = useState("");
  const [problem, setProblem] = useState(null);
  useEffect(() => { document.title = "Sign in · datafeed"; }, []);
  const submit = async (e) => {
    e.preventDefault();
    setProblem(null);
    try {
      await api("POST", "/auth/login", { token });
      onDone();
    } catch (err) { setProblem(err); }
  };
  return html`<main class="login">
    <div class="brand">datafeed</div>
    <div class="panel">
      <${ErrorNotice} error=${problem} />
      <form onSubmit=${submit}>
        <label>API token
          <input type="password" autocomplete="current-password" value=${token} onInput=${(e) => setToken(e.target.value)} required />
          <span class="hint">Create one with <code>datafeed token create</code>.</span>
        </label>
        <button class="primary" type="submit">Sign in</button>
      </form>
    </div>
  </main>`;
}

const NAV = [["/", "Dashboard"], ["/sessions", "Sessions"], ["/schemas", "Schemas"], ["/vocabulary", "Vocabulary"], ["/workers", "Workers"], ["/settings", "Settings"]];

function route(path, me) {
  const [clean] = path.split("?");
  const parts = clean.split("/").filter(Boolean);
  switch (parts[0]) {
    case undefined: return html`<${Dashboard} />`;
    case "sessions":
      if (parts[1] === "new") return html`<${NewSession} />`;
      return parts[1] ? html`<${SessionDetail} id=${parts[1]} key=${parts[1]} />` : html`<${Sessions} />`;
    case "schemas": return parts[1] ? html`<${SchemaEditor} id=${parts[1]} key=${parts[1]} />` : html`<${Schemas} />`;
    case "vocabulary": return html`<${Vocabulary} />`;
    case "domains": return parts[1] ? html`<${DomainDetail} id=${parts[1]} key=${parts[1]} />` : html`<${Vocabulary} />`;
    case "catalogs": return parts[1] ? html`<${CatalogDetail} id=${parts[1]} key=${parts[1]} />` : html`<${Vocabulary} />`;
    case "workers": return html`<${Workers} me=${me} />`;
    case "settings": return html`<${Settings} me=${me} />`;
  }
  return html`<${Page} title="Not found"><div class="panel">Nothing is here. <${Link} to="/">Back to the dashboard</${Link}>.</div></${Page}>`;
}

function isCurrent(path, to) {
  const [clean] = path.split("?");
  if (to === "/") return clean === "/";
  if (to === "/vocabulary") return ["/vocabulary", "/domains", "/catalogs"].some((p) => clean.startsWith(p));
  return clean.startsWith(to);
}

function App() {
  const [me, setMe] = useState(undefined); // undefined: not known yet; null: signed out
  const [problem, setProblem] = useState(null);
  const path = usePath();
  const load = async () => {
    try {
      setMe(await api("GET", "/whoami"));
      setProblem(null);
    } catch (err) {
      setMe(null);
      if (err.status !== 401) setProblem(err);
    }
  };
  useEffect(() => {
    load();
    return onAuthLost(() => setMe(null));
  }, []);
  const signOut = async () => {
    await api("POST", "/auth/logout").catch(() => {});
    setMe(null);
    navigate("/", { replace: true });
  };

  if (me === undefined) return null;
  if (me === null) {
    return html`${problem && html`<${ErrorNotice} error=${problem} />`}<${Login} onDone=${load} /><${Toasts} />`;
  }
  return html`<div class="shell">
    <nav class="nav" aria-label="Main">
      <div class="brand">datafeed</div>
      ${NAV.map(([to, label]) => html`<${Link} to=${to} aria-current=${isCurrent(path, to) ? "page" : undefined}>${label}</${Link}>`)}
      <div class="spacer"></div>
      <div class="who">${me.name} (${me.role})</div>
      ${me.id !== "local" && html`<a href="/" onClick=${(e) => { e.preventDefault(); signOut(); }}>Sign out</a>`}
    </nav>
    <main class="main">${route(path, me)}</main>
  </div><${Toasts} />`;
}

render(html`<${App} />`, document.getElementById("root"));
