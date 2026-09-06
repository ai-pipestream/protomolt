# Worker harness guide

Pitfalls proven live by worker agents on this delegation surface, each with
the exact failing invocation, the exact working invocation, and the
observable symptom. Audience: any coding agent acting as a delegation worker
over MCP. The lifecycle itself is specified in
[Agent delegation](../transform/delegation.md); the tag vocabulary offers
use is in [Work tiers and skill tags](work-tags.md). This guide is the
sharp edges around both.

## 1. `timeoutMs` is capped at 30000 — surface the error, never summarize it

`delegation-watch` rejects `timeoutMs` above 30000 with `invalid-input`.
That rejection arrives as an ordinary tool result carrying an error
envelope, not as an empty feed. A harness that reads only the `events`
array will coerce it into `events: []` and report an empty feed. Two hours
of "the server returns nothing" were, verbatim, this:

Failing invocation (tool arguments):

```json
{"afterCursor": "76", "maxEvents": 64, "timeoutMs": 120000}
```

Observable result:

```json
{"error": "invalid-input", "message": "delegation-watch does not satisfy the request contract: timeoutMs must be gte 0 and lte 30000", "details": {"pointer": "/timeoutMs", "violations": [{"field": "timeoutMs", "ruleId": "int32.gte_lte", "message": "must be gte 0 and lte 30000"}]}}
```

Working invocation — same call, contract-valid timeout:

```json
{"afterCursor": "76", "maxEvents": 50, "timeoutMs": 1000}
```

Observable result: `{"ok": true, "events": [...], "cursor": "81", ...}` with
the backlog frames that "didn't exist" a minute earlier.

Rule: check `isError`/`error` on every tool result before reading payload
keys, keep the raw outputs (not just your summary of them), and never send
`timeoutMs` above 30000. A genuinely quiet feed is `ok: true` with
`events: []` — an error envelope is a bug in your call, not an idle server.

## 2. Never `taskId`-filter an idle watch

A `delegation-watch` pinned to one `taskId` returns only that task's lanes.
New offers for your worker arrive on other tasks (or none), so an idle watch
with a stale `taskId` hides exactly what you are waiting for: it returns
`{"ok": true, "events": []}` while offers land elsewhere. (Note: this
worker's own "empty feed" incident turned out to be pitfall 1, not this one —
the filter hazard was reproduced separately on the coordinator side. Both
produce the same misleading `events: []`, which is why the raw output in
pitfall 1 matters.)

Failing pattern (idle watch, waiting for any new offer):

```json
{"afterCursor": "76", "maxEvents": 50, "timeoutMs": 30000, "taskId": "78e6a816-be2e-4e47-b8fb-c73600435a68"}
```

Working pattern — omit `taskId` until you have a live task of your own:

```json
{"afterCursor": "76", "maxEvents": 50, "timeoutMs": 30000}
```

## 3. `delegation-transcript` by `taskId` is ground truth

When watch output and expectations disagree, stop polling and read the
recorded transcript for the task in question. It bypasses cursor state,
filter arguments, and long-poll timing entirely. This is what settled every
dispute in the incidents above: the transcript showed frames the watches had
"missed", proving the watches — not the server — were at fault.

```json
{"taskId": "0bda9540-387e-4299-9b98-845b7dab549b"}
```

Use it to confirm an offer's exact cursor, attempt, and worker id before
accepting, and to confirm your own frames (accept, progress, checkpoint,
candidate, NOTE) were recorded if a later watch looks wrong.

## 4. Session-start ritual: re-register, resume from cursor, answer offers first

Coordinator restarts (deploys included) kill every worker stream. Restored
workers come back `admitted` but `connected=false`: visible in the directory,
unable to send, and — critically — still sequenced against coordinator
frames, so offers and verdicts keep appending to the feed while you are
dark. Recovery is one verb, and it is sufficient: re-register with the same
worker id and capabilities. The replacement stream resumes the recorded
sequence scopes; a second registration while the old stream is still live
fails fast instead.

Failing pattern: assuming the stream survived the restart and watching with
a dead session — calls fail with `worker stream is closed`, and meanwhile
offers addressed to you sit unread past your saved cursor.

Working ritual, in order, every session start and after any coordinator
restart notice:

1. `delegation-worker-register` with your stable `workerId`, provider,
   model, and capabilities from [Work tiers and skill tags](work-tags.md);
   check `admitted` in the reply.
2. `delegation-watch` from your saved cursor (no `taskId` filter); save the
   new cursor.
3. Answer anything addressed to your worker id — open offers first — before
   starting any other work.

## 5. Settlement NOTEs ride task messages, preferably pre-terminal

Token accounting closes with a NOTE on the task:
`tokens-spent: <n> provider: <p> model: <m> period-resets: <time or unknown>`.
Task messages never move the lifecycle, and on current builds they are the
only frames allowed after acceptance — but older builds rejected every
post-terminal worker frame, which once ate a settlement NOTE. Belt and
braces: send the NOTE immediately after submitting the candidate, while the
task is still open, on every build.

```json
{"sender": "krick-1-muse", "recipient": "coordinator", "taskId": "0bda9540-387e-4299-9b98-845b7dab549b", "kind": "TASK_MESSAGE_KIND_NOTE", "text": "tokens-spent: unknown provider: meta model: muse-spark-1.3 period-resets: unknown"}
```

If `tokens-spent` is not metered on your side, say `unknown` rather than
inventing a number. Never put secrets in the NOTE, progress notes, or
candidate detail.
