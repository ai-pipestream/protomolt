package ai.pipestream.proto.search.console;

/**
 * The page: no build step, no framework. The subject and lane pickers are populated from
 * {@code /subjects}, so what the page offers is exactly what the service serves; refusals from
 * the service render verbatim, because the service writes them for humans.
 */
final class SearchConsolePage {

    private SearchConsolePage() {
    }

    static final String PAGE = """
            <!doctype html>
            <meta charset="utf-8">
            <title>Search Console</title>
            <style>
              :root { color-scheme: light dark; }
              body { font: 15px/1.5 system-ui, sans-serif; max-width: 56rem;
                     margin: 2rem auto; padding: 0 1rem; }
              h1 { font-size: 1.3rem; }
              h2 { font-size: 1.05rem; margin: 1.6rem 0 .4rem; }
              form.bar { display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; }
              input, select, button { font: inherit; padding: .35rem .5rem; }
              #query { flex: 1 1 16rem; }
              #k { width: 4.5rem; }
              #status { color: #888; margin: .8rem 0; min-height: 1.5em; }
              #status.error { color: #c33; }
              .hit { border: 1px solid color-mix(in srgb, currentColor 25%, transparent);
                     border-radius: 8px; padding: .7rem .9rem; margin: .6rem 0;
                     animation: arrive .35s ease-out; }
              .hit .score { float: right; color: #888; font-variant-numeric: tabular-nums; }
              .hit .id { font-family: ui-monospace, monospace; font-size: .85rem;
                         color: #888; overflow-wrap: anywhere; }
              .hit .text { margin: .4rem 0 0; }
              .hit dl { margin: .4rem 0 0; font-size: .9rem; }
              .hit dt { font-weight: 600; display: inline; }
              .hit dd { display: inline; margin: 0 .8rem 0 .3rem; overflow-wrap: anywhere; }
              @keyframes arrive { from { opacity: 0; transform: translateY(4px); }
                                  to { opacity: 1; } }
              #ops { display: none; }
              header { display: flex; align-items: baseline; gap: 1rem; }
              header h1 { margin-right: auto; }
              #loginStatus { color: #c33; min-height: 1.5em; }
              table { border-collapse: collapse; width: 100%; font-size: .9rem; }
              th, td { text-align: left; padding: .25rem .5rem;
                       border-bottom: 1px solid color-mix(in srgb, currentColor 20%, transparent); }
              #opsOut { font-family: ui-monospace, monospace; font-size: .85rem;
                        white-space: pre-wrap; overflow-wrap: anywhere; color: #888; }
            </style>
            <header>
              <h1>Search Console</h1>
              <button id="signout" hidden>Sign out</button>
            </header>
            <section id="loginPanel" hidden>
              <p>This console is guarded: sign in with a credential the access policy names.</p>
              <form class="bar" id="loginForm">
                <input id="credential" type="password" placeholder="credential"
                       autocomplete="current-password">
                <button>Sign in</button>
              </form>
              <div id="loginStatus"></div>
            </section>
            <div id="app" hidden>
            <form class="bar" id="searchForm">
              <select id="subject" title="mapping subject"></select>
              <select id="lane" title="lane">
                <option value="SEARCH_LANE_LEXICAL">lexical</option>
                <option value="SEARCH_LANE_VECTOR">vector</option>
                <option value="SEARCH_LANE_HYBRID">hybrid</option>
              </select>
              <input id="k" type="number" min="1" value="10" title="max hits">
              <input id="query" placeholder="search…" autofocus>
              <button>Search</button>
            </form>
            <div id="status"></div>
            <div id="hits"></div>
            <section id="ops">
              <h2>Operations</h2>
              <form class="bar" id="replayForm">
                <input id="replayWorkflow" value="parse-and-index" title="workflow">
                <input id="replayDrive" value="intake" title="drive">
                <input id="replayAccount" placeholder="account id" title="account id">
                <button>Replay drive into this subject</button>
              </form>
              <form class="bar" id="jobsForm"><button>Refresh jobs</button></form>
              <div id="opsOut"></div>
              <table id="jobs" hidden>
                <thead><tr><th>job</th><th>workflow</th><th>status</th><th>attempt</th></tr></thead>
                <tbody></tbody>
              </table>
            </section>
            </div>
            <script>
            const el = id => document.getElementById(id);
            const status = el('status'), hits = el('hits');
            let subjects = [];
            function say(text, isError) {
              status.textContent = text;
              status.className = isError ? 'error' : '';
            }
            async function load() {
              let session = {};
              try { session = await (await fetch('/session')).json(); } catch {}
              if (session.loginRequired && !session.authenticated) {
                el('loginPanel').hidden = false;
                el('app').hidden = true;
                el('signout').hidden = true;
                return;
              }
              el('loginPanel').hidden = true;
              el('app').hidden = false;
              el('signout').hidden = !session.loginRequired;
              const resp = await fetch('/subjects');
              if (!resp.ok) { say((await resp.json()).error || 'subjects unavailable', true); return; }
              subjects = (await resp.json()).subjects || [];
              el('subject').innerHTML = subjects.map(s =>
                  `<option value="${s.subject}">${s.subject}</option>`).join('');
              laneOptions();
              const ops = await fetch('/actions');
              if (ops.ok) el('ops').style.display = 'block';
            }
            function current() {
              return subjects.find(s => s.subject === el('subject').value);
            }
            function laneOptions() {
              const vector = !!(current() && current().hasVectorLane);
              for (const option of el('lane').options) {
                if (option.value !== 'SEARCH_LANE_LEXICAL') option.disabled = !vector;
              }
              if (!vector) el('lane').value = 'SEARCH_LANE_LEXICAL';
            }
            el('loginForm').onsubmit = async e => {
              e.preventDefault();
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
            };
            el('signout').onclick = async () => {
              await fetch('/session', { method: 'DELETE' });
              load();
            };
            el('subject').onchange = laneOptions;
            el('searchForm').onsubmit = async e => {
              e.preventDefault();
              hits.innerHTML = '';
              say('searching…');
              const resp = await fetch('/search', { method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ mappingSubject: el('subject').value,
                      query: el('query').value, k: Number(el('k').value),
                      lane: el('lane').value }) });
              const body = await resp.json();
              if (!resp.ok) { say(body.error || resp.statusText, true); return; }
              const found = body.hits || [];
              say(found.length ? `${found.length} hit${found.length > 1 ? 's' : ''}` : 'no hits');
              for (const hit of found) hits.appendChild(render(hit));
            };
            function render(hit) {
              const card = document.createElement('div');
              card.className = 'hit';
              const score = document.createElement('span');
              score.className = 'score';
              score.textContent = (hit.score ?? 0).toFixed(4);
              card.appendChild(score);
              const id = document.createElement('div');
              id.className = 'id';
              id.textContent = hit.chunkId || hit.docId || '';
              card.appendChild(id);
              // Stored values are typed cells (search.v1 StoredValue); the
              // proto3 JSON form carries exactly one populated arm.
              const cell = v => {
                if (v == null || typeof v !== 'object') return String(v);
                if ('stringValue' in v) return v.stringValue;
                if ('int64Value' in v) return String(v.int64Value);
                if ('doubleValue' in v) return String(v.doubleValue);
                if ('boolValue' in v) return String(v.boolValue);
                if ('timestampValue' in v) return v.timestampValue;
                if ('bytesValue' in v) return '(bytes)';
                return JSON.stringify(v);
              };
              const stored = hit.stored || {};
              if (stored.chunk_text) {
                const text = document.createElement('p');
                text.className = 'text';
                text.textContent = cell(stored.chunk_text);
                card.appendChild(text);
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
            async function action(name, input) {
              const resp = await fetch('/actions/' + name, { method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(input) });
              const body = await resp.json();
              if (!resp.ok) throw new Error(body.error || body.message || resp.statusText);
              return body;
            }
            el('replayForm').onsubmit = async e => {
              e.preventDefault();
              el('opsOut').textContent = 'replaying…';
              try {
                const input = { workflowName: el('replayWorkflow').value,
                    mappingSubject: el('subject').value, drive: el('replayDrive').value };
                if (el('replayAccount').value) input.accountId = el('replayAccount').value;
                const out = await action('replay-documents', input);
                el('opsOut').textContent =
                    `submitted ${out.submitted} run${out.submitted === 1 ? '' : 's'}`;
              } catch (err) { el('opsOut').textContent = err.message; }
            };
            el('jobsForm').onsubmit = async e => {
              e.preventDefault();
              try {
                const out = await action('list-jobs', {});
                const rows = out.jobs || [];
                const body = el('jobs').querySelector('tbody');
                body.innerHTML = '';
                for (const job of rows) {
                  const tr = document.createElement('tr');
                  for (const value of [job.jobId, job.workflowName, job.status, job.attempt]) {
                    const td = document.createElement('td');
                    td.textContent = value ?? '';
                    tr.appendChild(td);
                  }
                  body.appendChild(tr);
                }
                el('jobs').hidden = rows.length === 0;
                el('opsOut').textContent = rows.length ? '' : 'no jobs yet';
              } catch (err) { el('opsOut').textContent = err.message; }
            };
            load();
            </script>
            """;
}
