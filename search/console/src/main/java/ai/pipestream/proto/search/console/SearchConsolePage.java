package ai.pipestream.proto.search.console;

/**
 * The page: no build step, no framework. Four tabs over the same two bridges.
 *
 * <p>Search and Operations drive {@code /subjects}, {@code /search} and the named actions the
 * page has always driven. Metrics and Catalog drive the actions proxy alone, so they need no
 * route the console did not already serve.
 *
 * <p>The two newer tabs sit at opposite ends of one idea. Metrics is purpose-built because
 * {@code describe-mapping} returns a subject's members with their roles, which is enough to
 * offer exactly the measures and dimensions that exist and refuse the rest before a query is
 * sent. Catalog is generic because every action publishes a JSON Schema for its input, so one
 * renderer reaches every verb the caller holds a scope for, including the ones contributed at
 * wire time that never reach the typed HTTP surface.
 *
 * <p>What the page offers is what the server serves: subjects and lanes come from
 * {@code /subjects}, actions and their fields come from {@code /actions}, and refusals render
 * verbatim because the services write them for humans.
 */
final class SearchConsolePage {

    private SearchConsolePage() {
    }

    static final String PAGE = """
            <!doctype html>
            <html lang="en">
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Search Console</title>
            <style>
              :root {
                --ground: #F7F8F8;
                --surface: #FFFFFF;
                --sunken: #F0F2F3;
                --ink: #16191C;
                --muted: #5F6E77;
                --faint: #8B99A2;
                --line: #E1E5E7;
                --line-strong: #CBD2D6;
                --accent: #0B6A6A;
                --accent-ink: #FFFFFF;
                --accent-soft: #E2EFEE;
                --danger: #A3342A;
                --danger-soft: #F8E9E7;
                --radius: 8px;
                --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
                --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
              }
              @media (prefers-color-scheme: dark) {
                :root:not([data-theme="light"]) {
                  --ground: #0E1215;
                  --surface: #161B1F;
                  --sunken: #12171A;
                  --ink: #E3E9EB;
                  --muted: #93A1A9;
                  --faint: #6B7A82;
                  --line: #242C32;
                  --line-strong: #344047;
                  --accent: #4CB5AC;
                  --accent-ink: #08201F;
                  --accent-soft: #16302E;
                  --danger: #E08376;
                  --danger-soft: #2B1815;
                }
              }
              :root[data-theme="dark"] {
                --ground: #0E1215;
                --surface: #161B1F;
                --sunken: #12171A;
                --ink: #E3E9EB;
                --muted: #93A1A9;
                --faint: #6B7A82;
                --line: #242C32;
                --line-strong: #344047;
                --accent: #4CB5AC;
                --accent-ink: #08201F;
                --accent-soft: #16302E;
                --danger: #E08376;
                --danger-soft: #2B1815;
              }

              * { box-sizing: border-box; }

              body {
                margin: 0;
                background: var(--ground);
                color: var(--ink);
                font-family: var(--sans);
                font-size: 14px;
                line-height: 1.55;
                -webkit-font-smoothing: antialiased;
              }

              .shell { max-width: 68rem; margin: 0 auto; padding: 0 1.5rem 5rem; }

              /* ---------------------------------------------------------- masthead */
              header.top {
                display: flex; align-items: center; gap: 1rem;
                padding: 1.25rem 0 1rem;
              }
              header.top .mark {
                width: 1.6rem; height: 1.6rem; border-radius: 5px;
                background: var(--accent); flex: none;
                display: grid; place-items: center;
                color: var(--accent-ink); font-family: var(--mono);
                font-size: .8rem; font-weight: 700;
              }
              header.top h1 {
                font-size: .98rem; font-weight: 600; letter-spacing: -.01em;
                margin: 0; margin-right: auto;
              }
              header.top .who {
                font-family: var(--mono); font-size: .75rem; color: var(--muted);
              }

              /* ---------------------------------------------------------- controls */
              button, input, select, textarea { font: inherit; color: inherit; }

              .btn {
                background: var(--surface); color: var(--ink);
                border: 1px solid var(--line-strong); border-radius: 6px;
                padding: .38rem .8rem; cursor: pointer;
                transition: background .12s ease, border-color .12s ease;
              }
              .btn:hover:not(:disabled) { background: var(--sunken); }
              .btn.primary {
                background: var(--accent); color: var(--accent-ink);
                border-color: var(--accent); font-weight: 550;
              }
              .btn.primary:hover:not(:disabled) { filter: brightness(1.08); }
              .btn:disabled { opacity: .5; cursor: not-allowed; }
              .btn.quiet { border-color: transparent; background: none; color: var(--muted); }
              .btn.quiet:hover:not(:disabled) { background: var(--sunken); color: var(--ink); }

              input[type=text], input[type=password], input[type=number],
              input[type=search], select, textarea {
                background: var(--surface);
                border: 1px solid var(--line-strong);
                border-radius: 6px; padding: .38rem .55rem;
              }
              textarea {
                width: 100%; min-height: 5.5rem; resize: vertical;
                font-family: var(--mono); font-size: .82rem; line-height: 1.5;
              }
              :focus-visible {
                outline: 2px solid var(--accent); outline-offset: 1px;
              }
              input::placeholder, textarea::placeholder { color: var(--faint); }

              /* ---------------------------------------------------------- tabs */
              nav.tabs {
                display: flex; gap: .15rem; flex-wrap: wrap;
                border-bottom: 1px solid var(--line);
                margin-bottom: 1.5rem;
              }
              nav.tabs button {
                background: none; border: none; cursor: pointer;
                padding: .55rem .9rem; margin-bottom: -1px;
                border-bottom: 2px solid transparent;
                color: var(--muted); font-size: .875rem;
                transition: color .12s ease;
              }
              nav.tabs button:hover:not(:disabled) { color: var(--ink); }
              nav.tabs button[aria-selected="true"] {
                color: var(--ink); font-weight: 600;
                border-bottom-color: var(--accent);
              }
              nav.tabs button:disabled { opacity: .38; cursor: not-allowed; }

              .panel[hidden] { display: none; }

              /* ---------------------------------------------------------- surfaces */
              .card {
                background: var(--surface); border: 1px solid var(--line);
                border-radius: var(--radius); padding: 1rem 1.15rem;
              }
              .card + .card { margin-top: 1rem; }

              .bar { display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; }
              .bar > label {
                font-size: .75rem; color: var(--muted); text-transform: uppercase;
                letter-spacing: .06em; margin-right: -.2rem;
              }
              .grow { flex: 1 1 14rem; min-width: 0; }

              h2.sec {
                font-size: .72rem; text-transform: uppercase; letter-spacing: .09em;
                color: var(--muted); font-weight: 600; margin: 1.4rem 0 .5rem;
              }

              /* ---------------------------------------------------------- status */
              .status { font-size: .84rem; color: var(--muted); min-height: 1.4em;
                        margin: .8rem 0; display: flex; align-items: center; gap: .45rem; }
              .status::before {
                content: ""; width: .4rem; height: .4rem; border-radius: 50%;
                background: currentColor; opacity: .55; flex: none;
              }
              .status:empty { display: none; }
              .status.error { color: var(--danger); }
              .status.working::before { animation: pulse 1s ease-in-out infinite; }
              @keyframes pulse { 0%, 100% { opacity: .25; } 50% { opacity: 1; } }

              .empty {
                color: var(--faint); font-size: .875rem; text-align: center;
                padding: 2.5rem 1rem; border: 1px dashed var(--line-strong);
                border-radius: var(--radius);
              }

              /* ---------------------------------------------------------- hits */
              .hit {
                background: var(--surface); border: 1px solid var(--line);
                border-radius: var(--radius); padding: .8rem 1rem; margin-bottom: .6rem;
                animation: arrive .3s ease-out;
              }
              @keyframes arrive { from { opacity: 0; transform: translateY(3px); } }
              .hit .head { display: flex; align-items: baseline; gap: .75rem; }
              .hit .id {
                font-family: var(--mono); font-size: .78rem; color: var(--muted);
                overflow-wrap: anywhere; margin-right: auto;
              }
              .hit .score {
                font-family: var(--mono); font-size: .78rem; color: var(--accent);
                font-variant-numeric: tabular-nums; flex: none;
              }
              .hit .text { margin: .5rem 0 0; }
              .hit dl {
                margin: .55rem 0 0; display: grid; gap: .1rem .8rem;
                grid-template-columns: auto 1fr; font-size: .82rem;
              }
              .hit dt { color: var(--muted); font-family: var(--mono); font-size: .76rem; }
              .hit dd { margin: 0; overflow-wrap: anywhere; }

              /* ---------------------------------------------------------- tables */
              .tablewrap { overflow-x: auto; border: 1px solid var(--line);
                           border-radius: var(--radius); background: var(--surface); }
              table { border-collapse: collapse; width: 100%; font-size: .85rem; }
              th {
                text-align: left; padding: .5rem .75rem;
                font-size: .68rem; text-transform: uppercase; letter-spacing: .07em;
                color: var(--muted); font-weight: 600;
                border-bottom: 1px solid var(--line); white-space: nowrap;
              }
              td { padding: .45rem .75rem; border-bottom: 1px solid var(--line);
                   overflow-wrap: anywhere; }
              tbody tr:last-child td { border-bottom: none; }
              tbody tr:hover { background: var(--sunken); }
              td.num, th.num { text-align: right; font-variant-numeric: tabular-nums;
                               font-family: var(--mono); }

              /* ---------------------------------------------------------- members */
              .members { display: flex; flex-wrap: wrap; gap: .4rem; }
              .members label {
                display: inline-flex; align-items: center; gap: .4rem;
                border: 1px solid var(--line-strong); border-radius: 999px;
                padding: .25rem .7rem .25rem .55rem; font-size: .82rem;
                cursor: pointer; background: var(--surface);
                transition: border-color .12s ease, background .12s ease;
              }
              .members label:hover { border-color: var(--accent); }
              .members label:has(input:checked) {
                background: var(--accent-soft); border-color: var(--accent);
              }
              .members .agg { color: var(--muted); font-size: .74rem;
                              font-family: var(--mono); }
              .members select {
                padding: .05rem .2rem; font-size: .74rem; border-radius: 4px;
                margin-left: .15rem;
              }
              .members .none { color: var(--faint); font-size: .84rem; }

              /* ---------------------------------------------------------- catalog */
              .split { display: flex; gap: 1.25rem; align-items: flex-start; flex-wrap: wrap; }
              .split .side { flex: 0 0 16rem; }
              .split .main { flex: 1 1 22rem; min-width: 0; }
              .picker {
                margin-top: .5rem; max-height: 30rem; overflow-y: auto;
                border: 1px solid var(--line); border-radius: var(--radius);
                background: var(--surface);
              }
              .picker button {
                display: block; width: 100%; text-align: left; background: none;
                border: none; border-bottom: 1px solid var(--line);
                padding: .4rem .7rem; cursor: pointer;
                font-family: var(--mono); font-size: .79rem; color: var(--muted);
              }
              .picker button:last-child { border-bottom: none; }
              .picker button:hover { background: var(--sunken); color: var(--ink); }
              .picker button[aria-current="true"] {
                background: var(--accent-soft); color: var(--ink); font-weight: 600;
                box-shadow: inset 2px 0 0 var(--accent);
              }
              .picker .none { padding: .8rem .7rem; color: var(--faint); font-size: .82rem; }

              .verb { font-family: var(--mono); font-size: 1rem; font-weight: 600;
                      margin: 0 0 .3rem; }
              .about { color: var(--muted); margin: 0 0 1rem; font-size: .875rem; }
              .field { margin-bottom: .9rem; }
              .field > label {
                display: block; font-size: .78rem; font-weight: 600;
                font-family: var(--mono); margin-bottom: .15rem;
              }
              .field .req { color: var(--danger); }
              .field .hint { color: var(--muted); font-size: .79rem; margin: 0 0 .3rem; }
              .field input[type=text], .field input[type=number], .field select {
                width: 100%;
              }
              .field.check > label { display: inline-flex; align-items: center; gap: .45rem; }

              .out {
                font-family: var(--mono); font-size: .8rem; line-height: 1.5;
                white-space: pre-wrap; overflow-wrap: anywhere;
                background: var(--sunken); border: 1px solid var(--line);
                border-radius: var(--radius); padding: .75rem .85rem; margin-top: 1rem;
                max-height: 26rem; overflow-y: auto;
              }
              .out:empty { display: none; }
              .out.error { color: var(--danger); background: var(--danger-soft);
                           border-color: var(--danger); }

              .plan {
                margin-top: .8rem; font-family: var(--mono); font-size: .76rem;
                color: var(--muted); white-space: pre-wrap; overflow-wrap: anywhere;
                border-left: 2px solid var(--line-strong); padding-left: .7rem;
              }

              /* ---------------------------------------------------------- sign in */
              .signin { max-width: 22rem; margin: 5rem auto; }
              .signin h2 { font-size: 1.05rem; margin: 0 0 .3rem; }
              .signin p { color: var(--muted); margin: 0 0 1rem; font-size: .875rem; }
              .signin input { width: 100%; margin-bottom: .6rem; }
              .signin .btn { width: 100%; }
              #loginStatus { color: var(--danger); font-size: .84rem; min-height: 1.4em;
                             margin-top: .5rem; }
            </style>

            <div class="shell">

            <section class="signin" id="loginPanel" hidden>
              <h2>Search Console</h2>
              <p>This console is guarded. Sign in with a credential the access policy names.</p>
              <form id="loginForm">
                <input id="credential" type="password" placeholder="Credential"
                       autocomplete="current-password">
                <button class="btn primary">Sign in</button>
              </form>
              <div id="loginStatus"></div>
            </section>

            <div id="app" hidden>

            <header class="top">
              <div class="mark">pm</div>
              <h1>Search Console</h1>
              <span class="who" id="who"></span>
              <button class="btn quiet" id="signout" hidden>Sign out</button>
            </header>

            <nav class="tabs" id="tabs">
              <button data-tab="search" aria-selected="true">Search</button>
              <button data-tab="metrics" aria-selected="false">Metrics</button>
              <button data-tab="catalog" aria-selected="false">Catalog</button>
              <button data-tab="operations" aria-selected="false">Operations</button>
            </nav>

            <section class="panel" id="panel-search">
              <form class="card bar" id="searchForm">
                <label for="subject">Subject</label>
                <select id="subject"></select>
                <label for="lane">Lane</label>
                <select id="lane">
                  <option value="SEARCH_LANE_LEXICAL">lexical</option>
                  <option value="SEARCH_LANE_VECTOR">vector</option>
                  <option value="SEARCH_LANE_HYBRID">hybrid</option>
                </select>
                <label for="k">Hits</label>
                <input id="k" type="number" min="1" value="10" style="width: 4.5rem">
                <input id="query" class="grow" type="search" placeholder="Query" autofocus>
                <button class="btn primary">Search</button>
              </form>
              <div class="status" id="status"></div>
              <div id="hits"></div>
            </section>

            <section class="panel" id="panel-metrics" hidden>
              <form class="card bar" id="describeForm">
                <label for="metricSubject">Subject</label>
                <input id="metricSubject" class="grow" type="text"
                       placeholder="Mapping subject">
                <button class="btn">Describe</button>
              </form>
              <div class="status" id="metricStatus"></div>

              <div id="metricMapping" hidden>
                <div class="card">
                  <h2 class="sec" style="margin-top:0">Measures</h2>
                  <div class="members" id="measures"></div>
                  <h2 class="sec">Dimensions</h2>
                  <div class="members" id="dimensions"></div>
                  <h2 class="sec">Run</h2>
                  <form class="bar" id="queryForm">
                    <label for="metricBackend">Backend</label>
                    <select id="metricBackend"></select>
                    <label for="metricLimit">Limit</label>
                    <input id="metricLimit" type="number" min="1" value="50"
                           style="width: 5rem">
                    <button class="btn primary">Run query</button>
                    <button class="btn quiet" type="button" id="rebuildToggle">
                      Rebuild rollup
                    </button>
                  </form>
                  <form class="bar" id="rebuildForm" hidden style="margin-top:.6rem">
                    <label for="rollupTable">Table</label>
                    <input id="rollupTable" class="grow" type="text"
                           placeholder="Rollup table name">
                    <button class="btn">Rebuild</button>
                  </form>
                </div>
              </div>
              <div id="metricOut"></div>
            </section>

            <section class="panel" id="panel-catalog" hidden>
              <div class="split">
                <div class="side">
                  <input id="actionFilter" type="search" placeholder="Filter actions"
                         style="width:100%">
                  <div class="picker" id="actionList"></div>
                </div>
                <div class="main">
                  <div class="card" id="actionDetail"></div>
                </div>
              </div>
            </section>

            <section class="panel" id="panel-operations" hidden>
              <section id="ops">
                <div class="card">
                  <h2 class="sec" style="margin-top:0">Replay a drive</h2>
                  <form class="bar" id="replayForm">
                    <label for="replayWorkflow">Workflow</label>
                    <input id="replayWorkflow" type="text" value="parse-and-index">
                    <label for="replayDrive">Drive</label>
                    <input id="replayDrive" type="text" value="intake">
                    <label for="replayAccount">Account</label>
                    <input id="replayAccount" type="text" placeholder="optional">
                    <button class="btn primary">Replay into subject</button>
                  </form>
                </div>
                <div class="card">
                  <h2 class="sec" style="margin-top:0">Jobs</h2>
                  <form class="bar" id="jobsForm">
                    <button class="btn">Refresh jobs</button>
                  </form>
                  <div class="status" id="opsOut"></div>
                  <div class="tablewrap" id="jobsWrap" hidden>
                    <table id="jobs">
                      <thead><tr><th>Job</th><th>Workflow</th><th>Status</th>
                        <th class="num">Attempt</th></tr></thead>
                      <tbody></tbody>
                    </table>
                  </div>
                </div>
              </section>
            </section>

            </div>
            </div>

            <script>
            const el = id => document.getElementById(id);
            const hits = el('hits');
            let subjects = [];
            let actionsReachable = false;

            function setStatus(node, message, state) {
              node.textContent = message || '';
              node.className = 'status' + (state ? ' ' + state : '');
            }
            const say = (message, state) => setStatus(el('status'), message, state);
            function text(node, value) { node.textContent = value == null ? '' : String(value); }
            function empty(message) {
              const box = document.createElement('div');
              box.className = 'empty';
              box.textContent = message;
              return box;
            }
            /** Keeps a submit button honest while its request is in flight. */
            async function busy(form, run) {
              const button = form.querySelector('button:not([type=button])');
              if (button) button.disabled = true;
              try { return await run(); } finally { if (button) button.disabled = false; }
            }

            // ---------------------------------------------------------------- tabs
            const TABS = ['search', 'metrics', 'catalog', 'operations'];
            // Metrics, Catalog and Operations reach the server only through the actions proxy,
            // so a console mounted without one disables them by name rather than failing on
            // the first click.
            const NEEDS_ACTIONS = { metrics: true, catalog: true, operations: true };
            function showTab(name) {
              for (const tab of TABS) el('panel-' + tab).hidden = tab !== name;
              for (const button of el('tabs').querySelectorAll('button')) {
                button.setAttribute('aria-selected', String(button.dataset.tab === name));
              }
              if (name === 'catalog' && actionsReachable) loadActions();
            }
            el('tabs').onclick = e => {
              const button = e.target.closest('button');
              if (button && !button.disabled) showTab(button.dataset.tab);
            };

            // ---------------------------------------------------------------- session
            async function load() {
              let session = {};
              try { session = await (await fetch('/session')).json(); } catch {}
              if (session.loginRequired && !session.authenticated) {
                el('loginPanel').hidden = false;
                el('app').hidden = true;
                return;
              }
              el('loginPanel').hidden = true;
              el('app').hidden = false;
              el('signout').hidden = !session.loginRequired;
              el('who').textContent = session.loginRequired ? 'signed in' : 'open console';

              const resp = await fetch('/subjects');
              if (!resp.ok) {
                say((await resp.json()).error || 'subjects unavailable', 'error');
              } else {
                subjects = (await resp.json()).subjects || [];
                el('subject').innerHTML = subjects.map(s =>
                    `<option value="${s.subject}">${s.subject}</option>`).join('');
                laneOptions();
                if (subjects.length && !el('metricSubject').value) {
                  el('metricSubject').value = subjects[0].subject;
                }
                if (!subjects.length) {
                  hits.appendChild(empty('This node serves no mapping subjects yet.'));
                }
              }
              const ops = await fetch('/actions');
              actionsReachable = ops.ok;
              if (ops.ok) el('ops').style.display = 'block';
              for (const button of el('tabs').querySelectorAll('button')) {
                if (NEEDS_ACTIONS[button.dataset.tab] && !actionsReachable) {
                  button.disabled = true;
                  button.title = 'this console has no actions route configured';
                }
              }
            }
            function current() {
              return subjects.find(s => s.subject === el('subject').value);
            }
            function laneOptions() {
              const vector = !!(current() && current().hasVectorLane);
              for (const option of el('lane').options) {
                if (option.value !== 'SEARCH_LANE_LEXICAL') {
                  option.disabled = !vector;
                  option.textContent = option.value.split('_').pop().toLowerCase()
                      + (vector ? '' : ' (no chunk lane)');
                }
              }
              if (!vector) el('lane').value = 'SEARCH_LANE_LEXICAL';
            }
            el('loginForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                const resp = await fetch('/session',
                    { method: 'POST', body: el('credential').value });
                if (!resp.ok) {
                  el('loginStatus').textContent =
                      (await resp.json()).error || 'sign-in failed';
                  return;
                }
                el('credential').value = '';
                el('loginStatus').textContent = '';
                load();
              });
            };
            el('signout').onclick = async () => {
              await fetch('/session', { method: 'DELETE' });
              location.reload();
            };

            // ---------------------------------------------------------------- search
            el('subject').onchange = laneOptions;
            el('searchForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                hits.innerHTML = '';
                say('Searching', 'working');
                const resp = await fetch('/search', { method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ mappingSubject: el('subject').value,
                        query: el('query').value, k: Number(el('k').value),
                        lane: el('lane').value }) });
                const body = await resp.json();
                if (!resp.ok) { say(body.error || resp.statusText, 'error'); return; }
                const found = body.hits || [];
                say(found.length
                    ? `${found.length} hit${found.length > 1 ? 's' : ''}` : 'No hits');
                if (!found.length) {
                  hits.appendChild(empty('Nothing matched this query on this lane.'));
                  return;
                }
                for (const hit of found) hits.appendChild(render(hit));
              });
            };
            function render(hit) {
              const card = document.createElement('div');
              card.className = 'hit';
              const head = document.createElement('div');
              head.className = 'head';
              const id = document.createElement('span');
              id.className = 'id';
              id.textContent = hit.chunkId || hit.docId || '';
              const score = document.createElement('span');
              score.className = 'score';
              score.textContent = (hit.score ?? 0).toFixed(4);
              head.append(id, score);
              card.appendChild(head);
              // Stored values are typed cells (search.v1 StoredValue); the proto3 JSON form
              // carries exactly one populated arm.
              const stored = hit.stored || {};
              if (stored.chunk_text) {
                const body = document.createElement('p');
                body.className = 'text';
                body.textContent = cell(stored.chunk_text);
                card.appendChild(body);
              }
              const fields = document.createElement('dl');
              for (const [key, value] of Object.entries(stored)) {
                if (key === 'chunk_text') continue;
                const dt = document.createElement('dt'); dt.textContent = key;
                const dd = document.createElement('dd'); dd.textContent = cell(value);
                fields.append(dt, dd);
              }
              if (fields.childNodes.length) card.appendChild(fields);
              return card;
            }
            function cell(v) {
              if (v == null || typeof v !== 'object') return String(v);
              if ('stringValue' in v) return v.stringValue;
              if ('int64Value' in v) return String(v.int64Value);
              if ('doubleValue' in v) return String(v.doubleValue);
              if ('boolValue' in v) return String(v.boolValue);
              if ('timestampValue' in v) return v.timestampValue;
              if ('bytesValue' in v) return '(bytes)';
              return JSON.stringify(v);
            }

            // ---------------------------------------------------------------- actions
            async function action(name, input) {
              const resp = await fetch('/actions/' + name, { method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(input) });
              const body = await resp.json();
              if (!resp.ok) throw new Error(body.error || body.message || resp.statusText);
              return body;
            }

            // ---------------------------------------------------------------- metrics
            // describe-mapping answers a subject's members with their roles, so the query
            // builder offers exactly the measures and dimensions the mapping declares. A
            // member the subject does not carry is never offerable, so it is never sent.
            const GRAINS = ['TIME_GRAIN_UNSPECIFIED', 'TIME_GRAIN_DAY', 'TIME_GRAIN_WEEK',
                            'TIME_GRAIN_MONTH', 'TIME_GRAIN_QUARTER', 'TIME_GRAIN_YEAR'];
            let mapping = null;
            const metricSay = (message, state) =>
                setStatus(el('metricStatus'), message, state);

            el('describeForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                el('metricMapping').hidden = true;
                el('metricOut').innerHTML = '';
                metricSay('Describing', 'working');
                try {
                  mapping = await action('describe-mapping',
                      { mappingSubject: el('metricSubject').value });
                } catch (err) { metricSay(err.message, 'error'); return; }
                const count = (mapping.members || []).length;
                metricSay(`${mapping.messageType || mapping.mappingSubject}, `
                    + `${count} member${count === 1 ? '' : 's'}`);
                renderMembers();
                el('metricMapping').hidden = false;
              });
            };
            function renderMembers() {
              const members = mapping.members || [];
              fillMembers(el('measures'),
                  members.filter(m => m.role === 'MEMBER_ROLE_MEASURE'), 'measure');
              fillMembers(el('dimensions'),
                  members.filter(m => m.role === 'MEMBER_ROLE_DIMENSION'), 'dimension');
              const backends = mapping.backends || [];
              el('metricBackend').innerHTML = backends.length
                  ? backends.map(b =>
                      `<option value="${b}">${shortEnum(b)}</option>`).join('')
                  : '<option value="">mount default</option>';
            }
            function fillMembers(host, members, kind) {
              host.innerHTML = '';
              if (!members.length) {
                const none = document.createElement('span');
                none.className = 'none';
                none.textContent = 'This mapping declares no ' + kind + 's.';
                host.appendChild(none);
                return;
              }
              for (const m of members) {
                const label = document.createElement('label');
                const box = document.createElement('input');
                box.type = 'checkbox'; box.value = m.name; box.className = kind;
                label.append(box, document.createTextNode(m.name));
                if (kind === 'measure' && m.aggregate) {
                  const agg = document.createElement('span');
                  agg.className = 'agg';
                  agg.textContent = shortEnum(m.aggregate);
                  label.appendChild(agg);
                }
                // A grain only means something on a member that declares one.
                if (kind === 'dimension' && m.defaultGrain) {
                  const grain = document.createElement('select');
                  grain.className = 'grain';
                  grain.dataset.member = m.name;
                  grain.onclick = e => e.preventDefault();
                  for (const g of GRAINS) {
                    grain.appendChild(new Option(
                        g === 'TIME_GRAIN_UNSPECIFIED'
                            ? shortEnum(m.defaultGrain) + ' (default)' : shortEnum(g), g));
                  }
                  label.appendChild(grain);
                }
                if (m.description) label.title = m.description;
                host.appendChild(label);
              }
            }
            function shortEnum(value) {
              if (!value) return '';
              const parts = String(value).split('_');
              return parts[parts.length - 1].toLowerCase();
            }
            function checkedValues(className) {
              return Array.from(document.querySelectorAll('input.' + className + ':checked'))
                  .map(box => box.value);
            }
            function chosenDimensions() {
              return checkedValues('dimension').map(name => {
                const grain = document.querySelector('select.grain[data-member="' + name + '"]');
                const ref = { name };
                if (grain && grain.value && grain.value !== 'TIME_GRAIN_UNSPECIFIED') {
                  ref.grain = grain.value;
                }
                return ref;
              });
            }
            function metricRequest() {
              const request = {
                mappingSubject: el('metricSubject').value,
                measures: checkedValues('measure'),
                dimensions: chosenDimensions(),
              };
              const backend = el('metricBackend').value;
              if (backend) request.backend = backend;
              return request;
            }
            el('queryForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                const request = metricRequest();
                if (!request.measures.length) {
                  metricSay('Pick at least one measure.', 'error'); return;
                }
                request.limit = Number(el('metricLimit').value);
                metricSay('Querying', 'working');
                el('metricOut').innerHTML = '';
                let answer;
                try { answer = await action('query-metrics', { request }); }
                catch (err) { metricSay(err.message, 'error'); return; }
                const rows = answer.rowCount || 0;
                metricSay(`${rows} row${rows === 1 ? '' : 's'} via `
                    + `${shortEnum(answer.backend) || 'mount default'}`);
                renderRows(answer, request);
              });
            };
            function renderRows(answer, request) {
              const out = el('metricOut');
              const rows = answer.rows || [];
              if (!rows.length) {
                out.appendChild(empty('No rows for those members.'));
              } else {
                const names = request.dimensions.map(d => d.name);
                const wrap = document.createElement('div');
                wrap.className = 'tablewrap';
                const table = document.createElement('table');
                const head = document.createElement('tr');
                for (const name of names) {
                  const th = document.createElement('th');
                  th.textContent = name;
                  head.appendChild(th);
                }
                for (const name of request.measures) {
                  const th = document.createElement('th');
                  th.className = 'num';
                  th.textContent = name;
                  head.appendChild(th);
                }
                table.createTHead().appendChild(head);
                const body = table.createTBody();
                for (const row of rows) {
                  const tr = document.createElement('tr');
                  for (const name of names) {
                    const td = document.createElement('td');
                    text(td, (row.dimensions || {})[name]);
                    tr.appendChild(td);
                  }
                  for (const name of request.measures) {
                    const td = document.createElement('td');
                    td.className = 'num';
                    const value = (row.measures || {})[name];
                    text(td, value == null ? '' : value);
                    tr.appendChild(td);
                  }
                  body.appendChild(tr);
                }
                wrap.appendChild(table);
                out.appendChild(wrap);
              }
              showPlan(out, answer.physicalPlan);
            }
            /** The executor's plan is evidence for a human, never input to a later query. */
            function showPlan(host, plan) {
              if (!plan) return;
              const node = document.createElement('div');
              node.className = 'plan';
              node.textContent = plan;
              host.appendChild(node);
            }
            el('rebuildToggle').onclick = () => {
              el('rebuildForm').hidden = !el('rebuildForm').hidden;
            };
            el('rebuildForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                const request = metricRequest();
                request.table = el('rollupTable').value;
                if (!request.table) { metricSay('Name the rollup table.', 'error'); return; }
                if (!request.measures.length) {
                  metricSay('Pick at least one measure.', 'error'); return;
                }
                metricSay('Rebuilding', 'working');
                try {
                  const answer = await action('rebuild-rollup', { request });
                  metricSay(`Rebuilt ${answer.table} via `
                      + `${shortEnum(answer.backend) || 'mount default'}`);
                  el('metricOut').innerHTML = '';
                  showPlan(el('metricOut'), answer.physicalPlan);
                } catch (err) { metricSay(err.message, 'error'); }
              });
            };

            // ---------------------------------------------------------------- catalog
            // Every action publishes a JSON Schema for its input, and the manifest the server
            // returns is already filtered to the scopes this principal holds. So one renderer
            // reaches every verb the caller may run, including the ones contributed at wire
            // time that never appear on the typed HTTP surface.
            let catalog = null;
            let chosen = null;
            async function loadActions() {
              if (catalog) return;
              try {
                const resp = await fetch('/actions');
                if (!resp.ok) throw new Error((await resp.json()).error || resp.statusText);
                catalog = await resp.json();
              } catch (err) {
                const detail = el('actionDetail');
                detail.innerHTML = '';
                const problem = document.createElement('div');
                problem.className = 'out error';
                problem.textContent = err.message;
                detail.appendChild(problem);
                return;
              }
              renderActionList();
              el('actionDetail').appendChild(
                  empty(`${catalog.length} action${catalog.length === 1 ? '' : 's'} `
                      + 'you hold a scope for. Pick one.'));
            }
            function renderActionList() {
              const filter = el('actionFilter').value.trim().toLowerCase();
              const list = el('actionList');
              list.innerHTML = '';
              for (const entry of catalog) {
                if (filter && !entry.name.toLowerCase().includes(filter)
                    && !(entry.description || '').toLowerCase().includes(filter)) continue;
                const button = document.createElement('button');
                button.type = 'button';
                button.textContent = entry.name;
                button.setAttribute('aria-current', String(chosen === entry.name));
                button.onclick = () => {
                  chosen = entry.name;
                  renderActionList();
                  renderActionForm(entry);
                };
                list.appendChild(button);
              }
              if (!list.childNodes.length) {
                const none = document.createElement('div');
                none.className = 'none';
                none.textContent = 'No actions match.';
                list.appendChild(none);
              }
            }
            el('actionFilter').oninput = () => { if (catalog) renderActionList(); };

            function renderActionForm(entry) {
              const detail = el('actionDetail');
              detail.innerHTML = '';
              const title = document.createElement('p');
              title.className = 'verb';
              title.textContent = entry.name;
              detail.appendChild(title);
              if (entry.description) {
                const about = document.createElement('p');
                about.className = 'about';
                about.textContent = entry.description;
                detail.appendChild(about);
              }
              const schema = entry.inputSchema || {};
              const properties = schema.properties || {};
              const required = new Set(schema.required || []);
              const form = document.createElement('form');
              const inputs = [];
              for (const [name, spec] of Object.entries(properties)) {
                const built = buildField(name, spec, required.has(name));
                form.appendChild(built.node);
                inputs.push(built);
              }
              if (!inputs.length) {
                const none = document.createElement('p');
                none.className = 'about';
                none.textContent = 'This action takes no declared input.';
                form.appendChild(none);
              }
              const run = document.createElement('button');
              run.className = 'btn primary';
              run.textContent = 'Run';
              form.appendChild(run);
              const output = document.createElement('div');
              output.className = 'out';
              form.onsubmit = e => {
                e.preventDefault();
                return busy(form, async () => {
                  output.className = 'out';
                  output.textContent = 'Running';
                  const input = {};
                  for (const field of inputs) {
                    let value;
                    try { value = field.read(); }
                    catch (err) {
                      output.className = 'out error';
                      output.textContent = field.name + ': ' + err.message;
                      return;
                    }
                    if (value !== undefined) input[field.name] = value;
                  }
                  try {
                    const answer = await action(entry.name, input);
                    output.textContent = JSON.stringify(answer, null, 2);
                  } catch (err) {
                    output.className = 'out error';
                    output.textContent = err.message;
                  }
                });
              };
              detail.appendChild(form);
              detail.appendChild(output);
            }

            /** One input built from its declared JSON Schema fragment. */
            function buildField(name, spec, isRequired) {
              const wrap = document.createElement('div');
              wrap.className = 'field';
              const label = document.createElement('label');
              label.textContent = name;
              if (isRequired) {
                const star = document.createElement('span');
                star.className = 'req';
                star.textContent = ' *';
                label.appendChild(star);
              }
              wrap.appendChild(label);
              if (spec.description) {
                const hint = document.createElement('p');
                hint.className = 'hint';
                hint.textContent = spec.description;
                wrap.appendChild(hint);
              }
              const type = spec.type || (spec.enum ? 'string' : 'object');
              let control;
              let read;
              if (spec.enum) {
                control = document.createElement('select');
                // An optional enum keeps an unset arm so the field can stay absent, rather
                // than the form silently choosing the first value on the caller's behalf.
                if (!isRequired) control.appendChild(new Option('unset', ''));
                for (const value of spec.enum) control.appendChild(new Option(value, value));
                read = () => control.value === '' ? undefined : control.value;
              } else if (type === 'boolean') {
                wrap.className = 'field check';
                control = document.createElement('input');
                control.type = 'checkbox';
                label.prepend(control);
                read = () => control.checked ? true : undefined;
              } else if (type === 'integer' || type === 'number') {
                control = document.createElement('input');
                control.type = 'number';
                if (spec.minimum !== undefined) control.min = spec.minimum;
                if (spec.maximum !== undefined) control.max = spec.maximum;
                if (type === 'integer') control.step = '1';
                read = () => {
                  if (control.value === '') return undefined;
                  const parsed = Number(control.value);
                  if (Number.isNaN(parsed)) throw new Error('not a number');
                  return parsed;
                };
              } else if (type === 'array') {
                control = document.createElement('textarea');
                control.placeholder = 'One value per line';
                read = () => {
                  const lines = control.value.split('\\n')
                      .map(line => line.trim()).filter(line => line.length);
                  return lines.length ? lines : undefined;
                };
              } else if (type === 'object') {
                control = document.createElement('textarea');
                control.placeholder = '{ }';
                read = () => {
                  if (!control.value.trim()) return undefined;
                  try { return JSON.parse(control.value); }
                  catch (err) { throw new Error('not valid JSON'); }
                };
              } else {
                control = document.createElement('input');
                control.type = 'text';
                read = () => control.value === '' ? undefined : control.value;
              }
              if (control.type !== 'checkbox') wrap.appendChild(control);
              return { name, node: wrap, read };
            }

            // ---------------------------------------------------------------- operations
            el('replayForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                setStatus(el('opsOut'), 'Replaying', 'working');
                try {
                  const input = { workflowName: el('replayWorkflow').value,
                      mappingSubject: el('subject').value, drive: el('replayDrive').value };
                  if (el('replayAccount').value) input.accountId = el('replayAccount').value;
                  const out = await action('replay-documents', input);
                  setStatus(el('opsOut'),
                      `Submitted ${out.submitted} run${out.submitted === 1 ? '' : 's'}`);
                } catch (err) { setStatus(el('opsOut'), err.message, 'error'); }
              });
            };
            el('jobsForm').onsubmit = e => {
              e.preventDefault();
              return busy(e.target, async () => {
                setStatus(el('opsOut'), 'Loading jobs', 'working');
                try {
                  const out = await action('list-jobs', {});
                  const rows = out.jobs || [];
                  const body = el('jobs').querySelector('tbody');
                  body.innerHTML = '';
                  for (const job of rows) {
                    const tr = document.createElement('tr');
                    const cells = [job.jobId, job.workflowName, job.status, job.attempt];
                    cells.forEach((value, index) => {
                      const td = document.createElement('td');
                      if (index === 3) td.className = 'num';
                      text(td, value);
                      tr.appendChild(td);
                    });
                    body.appendChild(tr);
                  }
                  el('jobsWrap').hidden = rows.length === 0;
                  setStatus(el('opsOut'), rows.length
                      ? `${rows.length} job${rows.length === 1 ? '' : 's'}` : 'No jobs yet');
                } catch (err) { setStatus(el('opsOut'), err.message, 'error'); }
              });
            };

            load();
            </script>
            """;
}
