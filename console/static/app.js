// Weave console. Plain ES modules, no build step: the page is served as-is by
// console/app.py, and everything it shows comes from that API.

const $ = (sel, root = document) => root.querySelector(sel);
const el = (tag, attrs = {}, ...kids) => {
  const node = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === 'class') node.className = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null && v !== false) node.setAttribute(k, v);
  });
  kids.flat().forEach((kid) => {
    if (kid !== null && kid !== undefined) node.append(kid.nodeType ? kid : String(kid));
  });
  return node;
};

const api = async (path, opts = {}) => {
  const res = await fetch(path, {
    ...opts,
    headers: { 'Content-Type': 'application/json', 'X-Weave-Console': '1', ...(opts.headers || {}) },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.detail || `${res.status} ${res.statusText}`);
  return body;
};
const post = (path, body) => api(path, { method: 'POST', body: JSON.stringify(body) });

const fmt = {
  n: (v, d = 0) => (v === null || v === undefined ? '—' : Number(v).toLocaleString(undefined, { maximumFractionDigits: d })),
  ts: (v) => (v ? new Date(v).toLocaleString() : '—'),
  time: (v) => (v ? new Date(v).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '—'),
  age: (v) => {
    if (!v) return '—';
    const s = (Date.now() - new Date(v).getTime()) / 1000;
    if (s < 90) return `${Math.round(s)} s ago`;
    if (s < 5400) return `${Math.round(s / 60)} min ago`;
    if (s < 172800) return `${Math.round(s / 3600)} h ago`;
    return `${Math.round(s / 86400)} d ago`;
  },
};
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

const state = {
  config: null,
  session: { mode: 'admin', role: 'admin', demo_enabled: false },
  actions: [],
  status: null,
  selectedDma: null,
  selectedJob: null,
  jobOffset: 0,
  tab: 'map',
};

// ------------------------------------------------------------------- tabs
document.querySelectorAll('[role="tab"]').forEach((btn) => {
  btn.addEventListener('click', () => showTab(btn.dataset.tab));
});

function showTab(name) {
  state.tab = name;
  document.querySelectorAll('[role="tab"]').forEach((b) => b.setAttribute('aria-selected', b.dataset.tab === name));
  document.querySelectorAll('.tab').forEach((t) => { t.hidden = t.id !== `tab-${name}`; });
  if (name === 'map') map.invalidateSize();
  if (name === 'telemetry') loadTelemetry();
  if (name === 'anomalies') loadAnomalies();
  if (name === 'jobs') loadJobs();
  if (name === 'gis') loadGisLayers();
}

// ----------------------------------------------------------------- header
// Auto follows the OS; Light and Dark pin it for this browser.
const THEMES = ['auto', 'light', 'dark'];
const darkQuery = window.matchMedia('(prefers-color-scheme: dark)');
const readTheme = () => { try { return localStorage.getItem('weave-theme') || 'auto'; } catch { return 'auto'; } };
const isDark = () => document.documentElement.dataset.theme === 'dark'
  || (!document.documentElement.dataset.theme && darkQuery.matches);

function applyTheme(theme) {
  if (theme === 'auto') delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
  try { localStorage.setItem('weave-theme', theme); } catch { /* storage blocked: still applies */ }
  $('#theme-btn').textContent = `Theme: ${theme[0].toUpperCase()}${theme.slice(1)}`;
  $('#theme-btn').dataset.theme = theme;
}

$('#theme-btn').addEventListener('click', () => {
  const next = THEMES[(THEMES.indexOf($('#theme-btn').dataset.theme) + 1) % THEMES.length];
  applyTheme(next);
  onThemeChange();
});
darkQuery.addEventListener('change', () => onThemeChange());
applyTheme(readTheme());

function renderHeader() {
  const { config, session } = state;
  $('#region').textContent = config.region_config || 'GIS_REGION_CONFIG not set';
  // The public console has no admin tools to link to; the server leaves them out.
  const links = [['Kibana', config.kibana_url], ['Flink', config.flink_url],
    ['Control Center', config.control_center_url]].filter(([, href]) => href);
  $('#links').replaceChildren(...links.map(([label, href]) => el('a', { href, target: '_blank', rel: 'noopener' }, label)));
  const auth = $('#auth');
  if (session.mode !== 'public') {
    auth.replaceChildren();
    return;
  }
  const names = { demo: 'Demo user', admin: 'Admin', public: 'Public view' };
  const badge = el('span', { class: 'pill role' }, names[session.role] || names.public);
  const loginBtn = (user, label) => el('button', { type: 'button', class: 'btn', onclick: () => openLogin(user) }, label);
  const buttons = session.role !== 'public'
    ? [el('button', { type: 'button', class: 'btn', onclick: logout }, 'Log out')]
    : [session.demo_enabled && loginBtn('demo', 'Demo login'),
      session.admin_enabled && loginBtn('admin', 'Admin login')].filter(Boolean);
  auth.replaceChildren(badge, ...buttons);
}

// One dialog for both logins. The demo password is not a secret, so it is
// shown and filled in; the admin one is masked and never pre-filled.
function openLogin(user) {
  const { session } = state;
  const demo = user === 'demo';
  const form = $('#login-form');
  form.dataset.user = user;
  $('#login-title').textContent = demo ? 'Demo login' : 'Admin login';
  $('#login-demo-text').hidden = !demo;
  $('#login-admin-text').hidden = demo;
  $('#login-hint').textContent = demo ? session.demo_password : '';
  const pw = $('#login-password');
  pw.type = demo ? 'text' : 'password';
  pw.autocomplete = demo ? 'off' : 'current-password';
  pw.value = demo ? session.demo_password : '';
  const code = $('#login-code');
  const needCode = !demo && session.admin_code;
  $('#login-code-row').hidden = !needCode;
  code.required = needCode;
  code.value = '';
  $('#login-err').hidden = true;
  $('#login-dialog').showModal();
}

async function reloadSession() {
  [state.session, state.actions] = await Promise.all([api('/api/session'), api('/api/actions')]);
  state.selectedJob = null;
  state.jobOffset = 0;
  $('#job-log').textContent = '';
  renderHeader();
  if (state.status) renderSteps(state.status);
  if (state.tab === 'jobs') await loadJobs();
}

async function logout() {
  await post('/api/logout', {}).catch(() => {});
  await reloadSession();
}

$('#login-form').addEventListener('submit', async (ev) => {
  if (ev.submitter?.value !== 'login') return;
  ev.preventDefault();
  try {
    const user = $('#login-form').dataset.user || 'demo';
    await post('/api/login', { user, password: $('#login-password').value, code: $('#login-code').value });
    $('#login-password').value = '';
    $('#login-code').value = '';
    $('#login-dialog').close();
    await reloadSession();
  } catch (err) {
    $('#login-err').textContent = err.message;
    $('#login-err').hidden = false;
  }
});

function renderHealth(st) {
  const pill = (label, probe, okText) => el(
    'span',
    { class: 'pill', title: probe.ok ? okText : probe.error },
    el('i', { class: `dot ${probe.ok ? 'ok' : 'bad'}` }),
    label,
  );
  $('#health').replaceChildren(
    pill('Postgres', st.db, 'reachable'),
    pill('Kafka', st.topics, `${(st.topics.value || []).length} topics`),
    pill('Connect', st.connectors, `${(st.connectors.value || []).length} connectors`),
    pill('Flink', st.flink, `${(st.flink.value || []).length} jobs`),
    st.containers.hidden ? null : pill('Docker', st.containers, 'socket reachable'),
  );
}

// ------------------------------------------------------------------ steps
// Each step's state is derived from what the step leaves behind, not from
// whether its button was pressed, so steps run from the CLI show up too.
function stepStates(st) {
  const db = st.db.value || {};
  const topics = new Set(st.topics.value || []);
  const haveTopics = st.expected_topics.filter((t) => topics.has(t)).length;
  const datagen = (st.connectors.value || []).filter((c) => c.name.startsWith('datagen-'));
  const running = datagen.filter((c) => c.state === 'RUNNING' && c.tasks.every((t) => t === 'RUNNING'));
  const flinkRunning = (st.flink.value || []).filter((j) => j.state === 'RUNNING');
  const schemas = st.schemas.value || [];

  const s = (ok, partial, text, probe) => ({
    cls: probe && !probe.ok ? 'error' : ok ? 'done' : partial ? 'partial' : '',
    text: probe && !probe.ok ? probe.error : text,
  });
  return {
    create_topics: s(haveTopics === st.expected_topics.length, haveTopics > 0,
      `${haveTopics} of ${st.expected_topics.length} topics`, st.topics),
    load_gis: s(db.pipes > 0 && db.pipes_in_graph > 0, db.pipes > 0,
      `${fmt.n(db.pipes)} pipes, ${fmt.n(db.facilities)} facilities · graph: ${fmt.n(db.nodes)} nodes`
        + (db.pipes > 0 && !db.pipes_in_graph ? ' (topology not built)' : ''), st.db),
    seed: s(db.dmas > 0 && schemas.length > 0, db.dmas > 0,
      `${fmt.n(db.dmas)} DMAs, ${fmt.n(db.water_meters)} meters, ${fmt.n(db.pressure_loggers)} loggers · ${schemas.length} schemas`, st.db),
    register_connectors: s(datagen.length > 0 && running.length === datagen.length, running.length > 0,
      datagen.length ? datagen.map((c) => `${c.name.replace('datagen-', '')}: ${c.state}`).join(', ') : 'none registered',
      st.connectors),
    build_model: s(st.model.value?.built, false,
      st.model.value?.built ? `network.inp ${fmt.age(st.model.value.mtime)}` : 'not built', st.model),
    start_flink: s(flinkRunning.length > 0, false,
      flinkRunning.length ? `${flinkRunning.length} running job(s)` : 'no running job', st.flink),
    run_sim_once: s(!!db.last_residual_ts, false,
      db.last_residual_ts ? `last residuals ${fmt.age(db.last_residual_ts)}` : 'no residuals yet', st.db),
  };
}

// What each role sees on an action button. Public viewers can look but not
// run; a demo user's simulated actions say so before anything is clicked.
function runButton(a, busy, job) {
  const label = { real: 'Run', simulated: 'Try (simulated)', none: 'Run' }[a.run] || 'Run';
  const title = a.run === 'none' ? 'Log in as the demo user to try actions'
    : busy && a.run === 'real' ? `${job.label} is running` : a.description;
  return el('button', {
    class: 'btn',
    disabled: a.run === 'none' || (busy && a.run === 'real'),
    title,
    onclick: () => openRun(a),
  }, label);
}

function renderSteps(st) {
  const states = stepStates(st);
  const busy = !!st.job;
  const steps = state.actions.filter((a) => a.step).sort((a, b) => a.step - b.step);
  $('#steps').replaceChildren(...steps.map((a) => {
    const s = states[a.id] || { cls: '', text: '' };
    return el('li', { class: `step ${s.cls}` },
      el('span', { class: 'n' }, s.cls === 'done' ? '✓' : a.step),
      el('div', {}, el('div', { class: 't' }, a.label), el('div', { class: 's' }, s.text)),
      runButton(a, busy, st.job));
  }));
  $('#tools').replaceChildren(el('div', { class: 'tools' }, ...state.actions.filter((a) => !a.step).map((a) => el(
    'div', { class: 'step' },
    el('span', { class: 'n' }, '·'),
    el('div', {}, el('div', { class: 't' }, a.label), el('div', { class: 's' }, a.description)),
    runButton(a, busy, st.job),
  ))));
}

function renderContainers(st) {
  $('#containers-section').hidden = !!st.containers.hidden;
  if (st.containers.hidden) return;
  const rows = st.containers.value || [];
  if (!st.containers.ok) {
    $('#containers').replaceChildren(el('li', { class: 'muted' }, st.containers.error));
    return;
  }
  $('#containers').replaceChildren(...rows.map((c) => el(
    'li', { title: c.status },
    // One-shot services (elk-setup) exit 0 by design; only a failure is red.
    el('i', {
      class: `dot ${c.state === 'running' ? 'ok'
        : c.state === 'exited' ? (c.status.startsWith('Exited (0)') ? '' : 'bad') : 'warn'}`,
    }),
    c.name,
  )));
}

async function refreshStatus() {
  try {
    const st = await api('/api/status');
    state.status = st;
    renderHealth(st);
    renderSteps(st);
    renderContainers(st);
    const badge = $('#job-badge');
    badge.hidden = !st.job;
    badge.textContent = st.job ? 'running' : '';
  } catch (err) {
    $('#health').replaceChildren(el('span', { class: 'pill' }, el('i', { class: 'dot bad' }), `console API: ${err.message}`));
  }
}

// ------------------------------------------------------------- run dialog
const dlg = $('#run-dialog');
let pendingAction = null;

function paramInput(p) {
  const id = `p-${p.name}`;
  if (p.kind === 'bool') {
    return el('label', { class: 'cb' }, el('input', { type: 'checkbox', id, name: p.name, checked: p.default }), p.label);
  }
  if (p.kind === 'choice') {
    const options = ['(all)', ...(state.config.layers || [])];
    return el('label', {}, p.label, el('select', { id, name: p.name },
      ...options.map((o) => el('option', { value: o, selected: o === p.default }, o))));
  }
  return el('label', {}, p.label, el('input', {
    id, name: p.name, type: p.kind === 'int' ? 'number' : 'text',
    value: p.default ?? '', min: p.min, max: p.max,
  }));
}

function readParams(action) {
  const out = {};
  action.params.forEach((p) => {
    const input = $(`#p-${p.name}`);
    out[p.name] = p.kind === 'bool' ? input.checked : p.kind === 'int' ? Number(input.value) : input.value;
  });
  return out;
}

async function updatePreview() {
  if (!pendingAction) return;
  $('#run-err').hidden = true;
  try {
    const { command } = await post(`/api/actions/${pendingAction.id}/preview`, { params: readParams(pendingAction) });
    $('#run-cmd').textContent = command.map((a) => (/[\s'"&|;<>]/.test(a) ? `'${a.replace(/'/g, "'\\''")}'` : a)).join(' ');
    $('#run-go').disabled = false;
  } catch (err) {
    $('#run-cmd').textContent = '';
    $('#run-err').textContent = err.message;
    $('#run-err').hidden = false;
    $('#run-go').disabled = true;
  }
}

function openRun(action) {
  pendingAction = action;
  $('#run-title').textContent = action.label;
  $('#run-desc').textContent = action.description;
  $('#run-params').replaceChildren(...action.params.map(paramInput));
  $('#run-params').oninput = updatePreview;
  const simulated = action.run === 'simulated';
  const demoLive = state.session.role === 'demo' && action.run === 'real';
  const note = simulated ? 'Simulated: shows the exact command and sample output. Nothing runs on the live stack.'
    : demoLive ? 'Runs on the live stack. Live demo runs are limited to one a minute across all demo users.'
      : action.destructive;
  $('#run-warn').hidden = !note;
  $('#run-warn').textContent = note || '';
  const danger = !simulated && action.destructive;
  $('#run-go').className = `btn ${danger ? 'danger' : 'primary'}`;
  $('#run-go').textContent = simulated ? 'Simulate' : danger ? 'Run (destructive)' : 'Run';
  $('#run-err').hidden = true;
  updatePreview();
  dlg.showModal();
}

$('#run-form').addEventListener('submit', async (ev) => {
  if (ev.submitter?.value !== 'run') return;
  ev.preventDefault();
  try {
    const { id } = await post(`/api/actions/${pendingAction.id}`, { params: readParams(pendingAction) });
    dlg.close();
    state.selectedJob = id;
    state.jobOffset = 0;
    $('#job-log').textContent = '';
    showTab('jobs');
    refreshStatus();
  } catch (err) {
    $('#run-err').textContent = err.message;
    $('#run-err').hidden = false;
  }
});

// ------------------------------------------------------------------- jobs
async function loadJobs() {
  await renderJobList();
  if (state.selectedJob) await pollJob();
}

async function renderJobList() {
  const jobs = await api('/api/jobs').catch(() => []);
  if (!state.selectedJob && jobs.length) state.selectedJob = jobs[0].id;
  const icon = { running: 'warn', succeeded: 'ok', failed: 'bad' };
  $('#job-list').replaceChildren(...(jobs.length ? jobs.map((j) => el(
    'li', {
      class: j.id === state.selectedJob ? 'sel' : '',
      onclick: () => { state.selectedJob = j.id; state.jobOffset = 0; $('#job-log').textContent = ''; loadJobs(); },
    },
    el('div', { class: 'r' }, el('i', { class: `dot ${icon[j.state]}` }), j.label),
    el('div', { class: 'small muted' }, `${j.state} · ${fmt.ts(j.started)}`),
  )) : [el('li', { class: 'muted' }, 'No jobs yet.')]));
}

async function pollJob() {
  const id = state.selectedJob;
  let job;
  try {
    job = await api(`/api/jobs/${id}?since=${state.jobOffset}`);
  } catch {
    return;
  }
  if (id !== state.selectedJob) return;
  const logEl = $('#job-log');
  const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 8;
  if (job.lines.length) logEl.append(`${job.lines.join('\n')}\n`);
  state.jobOffset = job.offset;
  if (atBottom) logEl.scrollTop = logEl.scrollHeight;
  $('#job-head').replaceChildren(
    el('strong', {}, job.label), ' ',
    job.simulated ? el('span', { class: 'pill' }, 'simulated') : null, ' ',
    el('span', { class: 'muted small' },
      `${job.state}${job.exit_code !== null ? ` (exit ${job.exit_code})` : ''} · started ${fmt.ts(job.started)}`),
    el('pre', { class: 'cmd' }, job.command.join(' ')),
  );
  if (job.state === 'running') {
    setTimeout(pollJob, 1000);
  } else {
    // Finished: the list's state and the pipeline steps are now stale.
    renderJobList();
    refreshStatus();
  }
}

// -------------------------------------------------------------------- map
const map = L.map('map', { zoomControl: true, preferCanvas: true, attributionControl: true });
map.setView([0, 0], 2);
const layers = {
  pipes: L.layerGroup().addTo(map),
  dmas: L.layerGroup().addTo(map),
  facilities: L.layerGroup().addTo(map),
  gauges: L.layerGroup().addTo(map),
  meters: L.layerGroup().addTo(map),
};
// Map colours resolve against #map, which carries its own tokens when the
// basemap is dark so pipes and markers stay readable on it.
const mcss = (name) => getComputedStyle($('#map')).getPropertyValue(name).trim();
const statusColour = (s) => mcss({ OK: '--ok', WATCH: '--watch', SUSPECTED_LEAK: '--leak' }[s] || '--unknown');

document.querySelectorAll('[data-layer]').forEach((cb) => cb.addEventListener('change', () => {
  const layer = layers[cb.dataset.layer];
  if (cb.checked) layer.addTo(map); else map.removeLayer(layer);
}));

function popupHtml(title, rows) {
  const box = el('div', {}, el('strong', {}, title),
    el('dl', { class: 'kv' }, ...rows.filter(([, v]) => v !== null && v !== undefined && v !== '')
      .flatMap(([k, v]) => [el('dt', {}, k), el('dd', {}, v)])));
  return box;
}

let fitted = false;
async function loadMap() {
  let dbError = null;
  const [dmas, pipes, facilities, gauges, meters] = await Promise.all([
    api('/api/geo/dmas').catch((e) => { dbError = e.message; return null; }),
    api('/api/geo/pipes?limit=8000').catch(() => null),
    api('/api/geo/facilities').catch(() => null),
    api('/api/geo/gauges').catch(() => null),
    api(`/api/geo/meters?limit=${METER_CAP}`).catch(() => null),
  ]);
  drawMeters(meters);

  layers.pipes.clearLayers();
  if (pipes) {
    L.geoJSON(pipes, {
      style: (f) => ({
        color: mcss(f.properties.in_graph ? '--pipe' : '--pipe-off'),
        weight: f.properties.diameter_mm >= 600 ? 2.5 : f.properties.diameter_mm >= 300 ? 1.6 : 1,
        opacity: 0.85,
      }),
      onEachFeature: (f, lyr) => lyr.bindPopup(() => popupHtml(f.properties.pipe_id, [
        ['Layer', f.properties.layer], ['Diameter', f.properties.diameter_mm && `${f.properties.diameter_mm} mm`],
        ['Material', f.properties.material], ['In graph', f.properties.in_graph ? 'yes' : 'no'],
      ])),
    }).addTo(layers.pipes);
  }
  $('#map-note').textContent = [
    pipes && pipes.features.length >= 8000 ? 'Pipes capped at 8,000, largest diameters first.' : '',
    meters && meters.features.length >= METER_CAP ? `Meters capped at ${fmt.n(METER_CAP)}.` : '',
  ].filter(Boolean).join(' ');

  layers.dmas.clearLayers();
  if (dmas) {
    L.geoJSON(dmas, {
      style: (f) => ({
        color: statusColour(f.properties.status), weight: 1.2, fillOpacity: 0.18,
        fillColor: statusColour(f.properties.status),
      }),
      onEachFeature: (f, lyr) => {
        lyr.bindTooltip(`${f.properties.dma_id} · ${f.properties.status}`, { sticky: true });
        lyr.on('click', () => openDma(f.properties.dma_id));
      },
    }).addTo(layers.dmas);
  }

  layers.facilities.clearLayers();
  if (facilities) {
    L.geoJSON(facilities, {
      pointToLayer: (f, ll) => L.circleMarker(ll, {
        radius: 6, color: mcss('--facility'), fillColor: mcss('--facility'), fillOpacity: 0.85, weight: 1,
      }),
      onEachFeature: (f, lyr) => lyr.bindPopup(() => popupHtml(f.properties.name || f.properties.facility_id, [
        ['Type', f.properties.facility_type], ['Capacity', f.properties.capacity_ml && `${fmt.n(f.properties.capacity_ml)} ML`],
      ])),
    }).addTo(layers.facilities);
  }

  layers.gauges.clearLayers();
  if (gauges) {
    L.geoJSON(gauges, {
      pointToLayer: (f, ll) => L.circleMarker(ll, {
        radius: 5, color: '#fff', fillColor: mcss('--gauge'), fillOpacity: 1, weight: 1.5,
      }),
      onEachFeature: (f, lyr) => lyr.bindPopup(() => popupHtml(f.properties.site_name || f.properties.site_id, [
        ['Site', f.properties.site_id], ['Variable', f.properties.variable], ['Provider', f.properties.provider],
      ])),
    }).addTo(layers.gauges);
  }

  const all = L.featureGroup([layers.dmas, layers.pipes, layers.facilities, layers.gauges].flatMap((g) => g.getLayers()));
  const empty = !all.getLayers().length;
  $('#map-empty').hidden = !empty;
  $('#map-empty').textContent = dmas === null
    ? `Postgres is not answering (${dbError}).`
    : 'Nothing loaded yet. Run steps 1 to 3 in the pipeline on the left.';
  if (!empty && !fitted) {
    map.invalidateSize();
    map.fitBounds(all.getBounds(), { padding: [30, 30] });
    fitted = true;
  }
}

// ------------------------------------------------------------- DMA drawer
async function openDma(dmaId) {
  state.selectedDma = dmaId;
  const drawer = $('#drawer');
  drawer.hidden = false;
  drawer.replaceChildren(el('div', { class: 'muted' }, `Loading ${dmaId} ...`));
  let d;
  try {
    d = await api(`/api/dma/${encodeURIComponent(dmaId)}`);
  } catch (err) {
    drawer.replaceChildren(el('div', { class: 'err' }, err.message));
    return;
  }
  if (state.selectedDma !== dmaId) return;
  const i = d.info;
  drawer.replaceChildren(
    el('header', {},
      el('div', {}, el('h3', {}, i.name || i.dma_id, el('span', { class: 'tag syn' }, 'synthetic')),
        el('span', { class: `status ${i.status || ''}` }, i.status || 'NO STATUS')),
      el('button', { class: 'btn ghost', onclick: closeDma, 'aria-label': 'Close' }, '×')),
    el('dl', { class: 'kv' },
      el('dt', {}, 'DMA id'), el('dd', {}, i.dma_id),
      el('dt', {}, 'Pipes'), el('dd', {}, `${fmt.n(i.pipe_count)} (${fmt.n(i.total_length_m / 1000, 1)} km)`),
      el('dt', {}, 'Meters / loggers'), el('dd', {}, `${fmt.n(i.meters)} / ${fmt.n(i.loggers)}`),
      el('dt', {}, 'Observed pressure'), el('dd', {}, i.min_pressure_m === null ? '—' : `${fmt.n(i.min_pressure_m, 1)} m`),
      el('dt', {}, 'Status as of'), el('dd', {}, fmt.ts(i.ts))),
    chart('Consumption per 15 min (L)', [{ name: 'consumption', colour: css('--syn'), points: d.balance.map((r) => [r.ts, r.consumption_l]) }]),
    chart('Min pressure (m)', [{ name: 'min pressure', colour: css('--derived'), points: d.balance.map((r) => [r.ts, r.min_pressure_m]) }]),
    chart('Pressure: modelled vs observed (m)', [
      { name: 'modelled', colour: css('--derived'), points: d.residuals.map((r) => [r.ts, r.modelled]) },
      { name: 'observed', colour: css('--syn'), points: d.residuals.map((r) => [r.ts, r.observed]) },
    ]),
    el('p', { class: 'hint' }, 'Geometry and pipes are real; meters, readings and the DMA boundary are synthetic. Flagged when |modelled − observed| > 5 m; watch above 2 m.'),
  );
}

function closeDma() {
  state.selectedDma = null;
  $('#drawer').hidden = true;
}

const METER_CAP = 20000;
function drawMeters(fc) {
  layers.meters.clearLayers();
  if (!fc) return;
  L.geoJSON(fc, {
    pointToLayer: (f, ll) => L.circleMarker(ll, {
      radius: f.properties.kind === 'logger' ? 5 : 3,
      color: mcss('--syn'), fillColor: f.properties.kind === 'logger' ? '#fff' : mcss('--syn'),
      fillOpacity: 0.9, weight: 1.2,
    }),
    onEachFeature: (f, lyr) => lyr.bindTooltip(`${f.properties.id} · ${f.properties.kind}${f.properties.meter_type ? ` (${f.properties.meter_type})` : ''} · ${f.properties.dma_id}`),
  }).addTo(layers.meters);
}

// ----------------------------------------------------------------- charts
// A small SVG line chart: enough for a 24 h trend, and no dependency.
function chart(title, series, { height = 130 } = {}) {
  const W = 360;
  const H = height;
  const pad = { l: 40, r: 8, t: 18, b: 18 };
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  const add = (tag, attrs, text) => {
    const n = document.createElementNS(ns, tag);
    Object.entries(attrs).forEach(([k, v]) => n.setAttribute(k, v));
    if (text !== undefined) n.textContent = text;
    svg.append(n);
    return n;
  };
  add('text', { x: 0, y: 11, class: 'title' }, title);

  const pts = series.flatMap((s) => s.points.filter(([, v]) => v !== null && v !== undefined)
    .map(([t, v]) => [new Date(t).getTime(), Number(v)]));
  if (!pts.length) {
    add('text', { x: W / 2, y: H / 2 + 4, 'text-anchor': 'middle', class: 'nodata' }, 'No data in window');
    return el('div', { class: 'chart' }, svg);
  }
  let [x0, x1] = [Math.min(...pts.map((p) => p[0])), Math.max(...pts.map((p) => p[0]))];
  let [y0, y1] = [Math.min(...pts.map((p) => p[1])), Math.max(...pts.map((p) => p[1]))];
  if (x0 === x1) { x0 -= 60000; x1 += 60000; }
  if (y0 === y1) { y0 -= 1; y1 += 1; }
  const sx = (t) => pad.l + ((t - x0) / (x1 - x0)) * (W - pad.l - pad.r);
  const sy = (v) => H - pad.b - ((v - y0) / (y1 - y0)) * (H - pad.t - pad.b);

  add('line', { x1: pad.l, x2: W - pad.r, y1: H - pad.b, y2: H - pad.b, class: 'axis' });
  [y0, (y0 + y1) / 2, y1].forEach((v) => {
    add('line', { x1: pad.l, x2: W - pad.r, y1: sy(v), y2: sy(v), class: 'axis', 'stroke-dasharray': '2 3' });
    add('text', { x: pad.l - 4, y: sy(v) + 3, 'text-anchor': 'end', class: 'lbl' }, fmt.n(v, Math.abs(y1 - y0) < 10 ? 1 : 0));
  });
  add('text', { x: pad.l, y: H - 4, class: 'lbl' }, fmt.time(x0));
  add('text', { x: W - pad.r, y: H - 4, 'text-anchor': 'end', class: 'lbl' }, fmt.time(x1));

  series.forEach((s, idx) => {
    const p = s.points.filter(([, v]) => v !== null && v !== undefined)
      .map(([t, v]) => [sx(new Date(t).getTime()), sy(Number(v))]);
    if (!p.length) return;
    if (p.length === 1) add('circle', { cx: p[0][0], cy: p[0][1], r: 2.5, fill: s.colour });
    else add('polyline', { points: p.map((q) => q.join(',')).join(' '), fill: 'none', stroke: s.colour, 'stroke-width': 1.6 });
    if (series.length > 1) {
      add('text', { x: W - pad.r - idx * 70, y: 11, 'text-anchor': 'end', class: 'lbl', fill: s.colour }, s.name);
    }
  });
  return el('div', { class: 'chart' }, svg);
}

// -------------------------------------------------------------- telemetry
async function loadTelemetry() {
  const [fresh, feeds] = await Promise.all([
    api('/api/freshness').catch((e) => ({ error: e.message })),
    api('/api/feeds').catch((e) => ({ error: e.message })),
  ]);

  const tbl = $('#freshness');
  if (fresh.error) {
    tbl.replaceChildren(el('tr', {}, el('td', { class: 'err' }, fresh.error)));
  } else {
    tbl.replaceChildren(
      el('tr', {}, el('th', {}, 'Table'), el('th', {}, 'Origin'), el('th', {}, 'Latest row'), el('th', { class: 'num' }, 'Rows, last hour')),
      ...fresh.map((r) => {
        const stale = r.latest && Date.now() - new Date(r.latest).getTime() > 3600e3;
        return el('tr', {},
          el('td', {}, r.table),
          el('td', {}, el('span', { class: `tag ${r.origin}` }, r.origin)),
          el('td', { class: !r.latest ? 'none' : stale ? 'stale' : '', title: fmt.ts(r.latest) }, r.latest ? fmt.age(r.latest) : 'none'),
          el('td', { class: 'num' }, fmt.n(r.last_hour)));
      }),
    );
  }

  if (feeds.error) {
    $('#market-price').replaceChildren(el('div', { class: 'err' }, feeds.error));
    $('#market-demand').replaceChildren();
    $('#gauges').replaceChildren();
    return;
  }
  const regions = [...new Set(feeds.market.map((r) => r.region_id))];
  const palette = [css('--real'), css('--derived'), css('--watch'), css('--syn')];
  const bySeries = (key) => regions.map((reg, i) => ({
    name: reg, colour: palette[i % palette.length],
    points: feeds.market.filter((r) => r.region_id === reg).map((r) => [r.ts, r[key]]),
  }));
  $('#market-price').replaceChildren(chart('Price ($/MWh)', bySeries('price_mwh')));
  $('#market-demand').replaceChildren(chart('Total demand (MW)', bySeries('total_demand_mw')));

  const gauges = feeds.hydrometry_latest;
  $('#gauges').replaceChildren(...(gauges.length ? gauges.map((g) => el('div', { class: 'gauge' },
    el('div', { class: 'h' }, el('strong', {}, g.site_name || g.site_id), el('span', { class: 'muted small' }, g.site_id)),
    el('div', { class: 'h' },
      el('span', { class: 'val' }, fmt.n(g.value, 3)),
      el('span', { class: 'muted small' }, `${g.variable} · ${fmt.age(g.ts)}${g.quality_code ? ` · q${g.quality_code}` : ''}`)),
    chart('', [{
      name: g.variable, colour: css('--real'),
      points: feeds.hydrometry_series.filter((s) => s.site_id === g.site_id && s.variable === g.variable).map((s) => [s.ts, s.value]),
    }], { height: 80 }),
  )) : [el('div', { class: 'muted' }, 'No gauge readings in the last 24 h. Check the ingest-hydrometry container and the Flink job.')]));
}

// -------------------------------------------------------------- anomalies
async function loadAnomalies() {
  const counts = state.status?.db.value?.dma_status || {};
  const tile = (label, n, cls) => el('div', { class: 'card tile' },
    el('div', { class: 'v' }, fmt.n(n)), el('div', { class: 'l' }, el('i', { class: `sw ${cls}` }), label));
  $('#status-tiles').replaceChildren(
    tile('Suspected leak', counts.SUSPECTED_LEAK || 0, 'leak'),
    tile('Watch', counts.WATCH || 0, 'watch'),
    tile('OK', counts.OK || 0, 'ok'),
    tile('DMAs without status', Math.max(0, (state.status?.db.value?.dmas || 0)
      - (counts.SUSPECTED_LEAK || 0) - (counts.WATCH || 0) - (counts.OK || 0)), 'unknown'),
  );

  const tbl = $('#anomalies');
  let rows;
  try {
    rows = await api('/api/anomalies');
  } catch (err) {
    tbl.replaceChildren(el('tr', {}, el('td', { class: 'err' }, err.message)));
    return;
  }
  tbl.replaceChildren(
    el('tr', {}, ...['Time', 'DMA', 'Current status', 'Modelled (m)', 'Observed (m)', 'Residual (m)', 'z'].map((h, i) => el('th', { class: i > 2 ? 'num' : '' }, h))),
    ...(rows.length ? rows.map((r) => el('tr', {
      class: 'click',
      onclick: () => { showTab('map'); openDma(r.dma_id); },
    },
    el('td', {}, fmt.ts(r.ts)),
    el('td', {}, r.name || r.dma_id),
    el('td', {}, el('span', { class: `status ${r.status || ''}` }, r.status || '—')),
    el('td', { class: 'num' }, fmt.n(r.modelled, 2)),
    el('td', { class: 'num' }, fmt.n(r.observed, 2)),
    el('td', { class: 'num' }, fmt.n(r.residual, 2)),
    el('td', { class: 'num' }, fmt.n(r.z_score, 2)))) : [el('tr', {}, el('td', { class: 'muted', colspan: 7 }, 'Nothing over threshold in the last 24 h.'))]),
  );
}

// ---------------------------------------------------------------- basemaps
// The list comes from the server (Google only appears when .env has a key).
// The choice is per browser. In dark mode a light basemap switches to its
// dark variant where one exists, or is inverted in CSS, which sends the area
// in view to no extra provider. Imagery and Google are never altered.
let baseLayer = null;
let googleTimer = null;
const readBasemap = () => { try { return localStorage.getItem('weave-basemap'); } catch { return null; } };

function currentBasemap() {
  const all = state.config.basemaps || [];
  const chosen = $('#basemap').value || state.config.basemap_default;
  return all.find((b) => b.id === chosen) || all[0];
}

function applyBasemap() {
  const all = state.config.basemaps || [];
  let b = currentBasemap();
  const style = state.config.tile_style || 'auto';
  const wantDark = style === 'dark' || (style === 'auto' && isDark());
  if (wantDark && b.dark_variant) b = all.find((x) => x.id === b.dark_variant) || b;
  const invert = wantDark && b.kind === 'light';
  $('#map').classList.toggle('map-invert', invert);
  // Overlay colours follow what the map looks like, not the page theme.
  $('#map').classList.toggle('map-dark', invert || b.kind === 'dark' || b.kind === 'imagery');

  if (baseLayer) map.removeLayer(baseLayer);
  baseLayer = null;
  clearTimeout(googleTimer);
  map.off('moveend', googleAttribution);
  if (b.url) {
    baseLayer = L.tileLayer(b.url, {
      attribution: b.attribution, maxZoom: b.max_zoom || 19, subdomains: b.subdomains || 'abc',
    }).addTo(map);
    baseLayer.bringToBack();
    if (b.google) {
      map.on('moveend', googleAttribution);
      googleAttribution();
    }
  }
}

// Google's terms require the copyright for the area in view.
async function googleAttribution() {
  const b = currentBasemap();
  if (!b.google || !baseLayer) return;
  clearTimeout(googleTimer);
  googleTimer = setTimeout(async () => {
    const bounds = map.getBounds();
    const q = new URLSearchParams({
      map_id: b.id, zoom: map.getZoom(), north: bounds.getNorth(), south: bounds.getSouth(),
      east: bounds.getEast(), west: bounds.getWest(),
    });
    const res = await api(`/api/basemaps/google-attribution?${q}`).catch(() => null);
    if (res && baseLayer) {
      map.attributionControl.removeAttribution(baseLayer.getAttribution());
      baseLayer.options.attribution = `Google ${res.copyright}`.trim();
      map.attributionControl.addAttribution(baseLayer.options.attribution);
    }
  }, 300);
}

function renderBasemapPicker() {
  const all = state.config.basemaps || [];
  const saved = readBasemap();
  const value = all.some((b) => b.id === saved) ? saved : state.config.basemap_default;
  $('#basemap').replaceChildren(...all.map((b) => el('option', { value: b.id, selected: b.id === value }, b.label)));
  $('#basemap').addEventListener('change', () => {
    try { localStorage.setItem('weave-basemap', $('#basemap').value); } catch { /* per-visit only */ }
    applyBasemap();
    loadMap();
  });
}

function onThemeChange() {
  if (!state.config) return;
  applyBasemap();
  loadMap();
}

// -------------------------------------------------------------- GIS clients
function copyable(value, secret = false) {
  const code = el('code', { class: 'val' }, secret ? '••••••••' : value);
  const btn = el('button', {
    type: 'button', class: 'btn ghost',
    onclick: async () => {
      try {
        await navigator.clipboard.writeText(value);
        btn.textContent = 'Copied';
      } catch {
        code.textContent = value;
        btn.textContent = 'Select and copy';
      }
      setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
    },
  }, 'Copy');
  return el('span', { class: 'copy' }, code, btn);
}

function steps(...items) {
  return el('ol', { class: 'steps-list' }, ...items.map((t) => el('li', {}, t)));
}

function renderGis() {
  const g = state.config.gis;
  // Not offered on the public console: it hands out database connection details.
  $('[data-tab="gis"]').hidden = !g;
  if (!g) return;
  const pgUri = `postgresql://${g.db_user}@${g.db_host}:${g.db_port}/${g.db_name}`;
  $('#gis-cards').replaceChildren(
    el('div', { class: 'card gis' },
      el('h3', {}, 'PostGIS (direct)', el('span', { class: 'tag real' }, 'full SQL')),
      el('p', { class: 'muted small' }, 'Fastest for large layers, and the only route for SQL queries or the raw GIS attributes.'),
      el('dl', { class: 'kv' },
        el('dt', {}, 'Host'), el('dd', {}, copyable(g.db_host)),
        el('dt', {}, 'Port'), el('dd', {}, copyable(g.db_port)),
        el('dt', {}, 'Database'), el('dd', {}, copyable(g.db_name)),
        el('dt', {}, 'User'), el('dd', {}, copyable(g.db_user)),
        el('dt', {}, 'Password'), el('dd', {}, copyable(g.db_password, true))),
      el('a', { class: 'btn primary', href: '/api/gis/qgis/postgis-connection.xml', download: 'weave-postgis.xml' }, 'Download QGIS connection'),
      el('h4', {}, 'QGIS'),
      steps('In the Browser panel, right-click PostgreSQL and choose Load Connections.',
        'Pick the downloaded weave-postgis.xml and import it.',
        'Expand "Weave twin (read-only)": asset, ref and twin hold the layers. asset.dma_status has DMA outlines with their current status.'),
      el('h4', {}, 'ArcGIS Pro'),
      steps('Insert > Connections > Database > New Database Connection.',
        'Platform PostgreSQL, instance localhost,5432, database authentication with the user and password above.'),
      el('h4', {}, 'GDAL / ogr2ogr'),
      el('pre', { class: 'cmd' }, `ogrinfo "PG:host=${g.db_host} port=${g.db_port} dbname=${g.db_name} user=${g.db_user}" asset.dma_status`),
      el('p', { class: 'muted small' }, `Connection URI: ${pgUri}`)),
    el('div', { class: 'card gis' },
      el('h3', {}, 'OGC API – Features', el('span', { class: 'tag real' }, 'standard web API')),
      el('p', { class: 'muted small' }, 'An open standard most GIS clients and web maps read without a database driver. Read-only, paged GeoJSON.'),
      el('dl', { class: 'kv' },
        el('dt', {}, 'URL'), el('dd', {}, copyable(g.ogcapi_url))),
      el('div', { class: 'row-end', style: 'justify-content:flex-start;margin-top:0' },
        el('a', { class: 'btn primary', href: '/api/gis/qgis/ogcapi-connection.xml', download: 'weave-ogcapi.xml' }, 'Download QGIS connection'),
        el('a', { class: 'btn', href: g.ogcapi_url, target: '_blank', rel: 'noopener' }, 'Browse the API')),
      el('h4', {}, 'QGIS'),
      steps('In the Browser panel, right-click WFS / OGC API - Features and choose Load Connections.',
        'Pick the downloaded weave-ogcapi.xml, or add a new connection with the URL above and version "OGC API - Features".',
        'Expand "Weave twin (OGC API)" and drag layers onto the map.'),
      el('h4', {}, 'ArcGIS Pro'),
      steps('Insert > Connections > New OGC API Server, and enter the URL above.'),
      el('h4', {}, 'GDAL / ogr2ogr'),
      el('pre', { class: 'cmd' }, `ogrinfo OAPIF:${g.ogcapi_url}`)),
  );
}

async function loadGisLayers() {
  const tbl = $('#gis-layers');
  let rows;
  try {
    rows = await api('/api/gis/collections');
  } catch (err) {
    tbl.replaceChildren(el('tr', {}, el('td', { class: 'err' }, `${err.message}. Is the featureserv container running?`)));
    return;
  }
  const origin = (id) => (id.startsWith('ref.') && id !== 'ref.hydrometry_site') || id === 'asset.dma' || id === 'asset.dma_status'
    ? 'synthetic' : 'real';
  tbl.replaceChildren(
    el('tr', {}, el('th', {}, 'Layer'), el('th', {}, 'Origin'), el('th', {}, '')),
    ...rows.map((c) => el('tr', {},
      el('td', {}, el('code', {}, c.id)),
      el('td', {}, el('span', { class: `tag ${origin(c.id)}` }, origin(c.id))),
      el('td', {}, el('a', { href: `${state.config.gis.ogcapi_url}/collections/${encodeURIComponent(c.id)}/items.html`, target: '_blank', rel: 'noopener' }, 'Preview')))),
  );
}

// ------------------------------------------------------------------- boot
async function boot() {
  try {
    [state.config, state.session, state.actions] = await Promise.all(
      [api('/api/config'), api('/api/session'), api('/api/actions')]);
  } catch (err) {
    $('#health').replaceChildren(el('span', { class: 'pill' }, el('i', { class: 'dot bad' }), `console API: ${err.message}`));
    return;
  }
  renderHeader();
  renderBasemapPicker();
  applyBasemap();
  renderGis();
  await refreshStatus();
  loadMap();
  setInterval(refreshStatus, 10000);
  setInterval(() => {
    loadMap();
    if (state.selectedDma && state.tab === 'map') openDma(state.selectedDma);
    if (state.tab === 'telemetry') loadTelemetry();
    if (state.tab === 'anomalies') loadAnomalies();
  }, 60000);
}

boot();
