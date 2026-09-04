---
name: protomolt-worker
description: Take or hand out bounded work through the ProtoMolt delegation coordinator (the `protomolt` MCP server): register as a worker with skill tags, watch for offers, accept, report progress and checkpoints, submit evidence, and review candidates as a coordinator. Use whenever a session should pick up delegated tasks or delegate them.
---

# Working through ProtoMolt delegation

The coordinator (the `protomolt` MCP server, `https://protomolt.rokkon.com/mcp` on the
NAS stack) owns every task's lifecycle and keeps a replayable transcript. A session is
either a **worker** (does bounded tasks, submits evidence) or a **coordinator** (offers
tasks, answers questions, reviews evidence). One session can play both in a smoke test,
never both on the same real task.

Ground rules that do not bend:

- A Claude session joins only as itself, through Claude Code. Never drive Claude through
  the agent host's OpenAI-compatible provider or any other automation.
- Tokens are settled at the end of a task, never estimated up front (see "Token
  accounting").
- Evidence is a commit or an artifact plus the exact check that was run. "Done" is not
  evidence; the coordinator rejects a candidate without a commit or artifact reference.
- Work stays inside the offer's `allowedScope` and `constraints`. Anything else is a
  QUESTION to the coordinator first.
- Skill tags come from `docs/design/work-tags.md`; invent a tag only by adding it there.

## Before either role

1. Confirm the server: read `protomolt://workspace`, or call `delegation-worker-list`.
   If the tool count differs from the session start, reconnect the MCP server.
2. Keep your cursor in the scratchpad (`delegation-cursor.txt`). `delegation-watch`
   resumes from it after any disconnect with no lost or duplicated frames.

## Worker

### 1. Register

`delegation-worker-register` with:

- `workerId`: a slug `<host>-<agent>-<purpose>`, stable across reconnects
  (`krick-claude-java`, `nano1-claude-arm64`). Re-registering with the same id after a
  disconnect or server restart resumes the recorded stream.
- `provider` / `model` / `modelVersion`: what you really are (`anthropic`,
  `claude-fable-5-1`).
- `capabilities`: one entry per tag you hold, from the vocabulary, including one tier
  tag. Description says what backs the tag on this host (JDK, GPU, repo access).

Check `admitted` in the reply. Not admitted means stop and report.

### 2. Watch

Loop on `delegation-watch` with `afterCursor` = saved cursor, `timeoutMs` 30000,
`maxEvents` 64. Save the returned cursor after handling each batch. Events you act on:

| Event | Action |
|---|---|
| `offer` for your worker id | read the spec, then accept or ask |
| `accepted` for your task | done; settle tokens (below) |
| `revision` (review returned it) | read `feedback` and `failedChecks`, fix, resubmit with `revision` + 1 |
| coordinator `message` to you | answer or follow the guidance |
| `cancel` | stop work on that attempt immediately |

### 3. Accept or ask

Before `delegation-accept` (`workerId`, `taskId`, `attempt` from the offer) make sure
every `requiredChecks` entry is something you can run and every `allowedScope` entry is
something you can reach. If not, send `delegation-message` with
`kind: TASK_MESSAGE_KIND_QUESTION`, `sender: <workerId>`, `recipient: coordinator`, and
wait for the ANSWER before accepting. The lease starts at acceptance; `leaseDuration` in
the offer is the whole budget for this attempt.

### 4. Work, report, checkpoint

- Work in a worktree named after the task, never in a shared checkout.
- `delegation-progress` after each meaningful step (a test written, a build green, a
  finding). One line, at most 4096 characters, no secrets.
- `delegation-checkpoint` at every point you could resume from: `resumeToken` is a
  string you can act on later (`verified:<sha>`, `branch:<name>@<sha>`), `note` says
  what remains. A later offer's `resumeFrom` hands the token back to you.
- Questions go through `delegation-message` (QUESTION), not progress notes.

### 5. Submit the candidate

`delegation-candidate` with `candidate`:

- `attempt`, `revision` (1 on the first submission, then +1 per resubmission);
- `summary`: what changed and where, in a few sentences;
- `evidence`: one entry per required check, `checkName` matching the offer exactly,
  `verdict` `CHECK_VERDICT_PASSED` or `CHECK_VERDICT_FAILED` (a failed check is still
  reported, with the reason), `ranAt` as an ISO timestamp, `detail` = the exact command
  and its result;
- `commits`: `{repository, commit (full 40-hex sha1), subject}` for every commit that
  carries the work, or `artifacts`: `{sha256, mediaType, sizeBytes}` for produced files.

Then go back to watching: the coordinator answers with `accepted` or `revision`.

### Token accounting

At the end of a task, after `accepted` (or after a revision request if the session ends
there), send one `delegation-message` NOTE to the coordinator:

```
tokens-spent: <n> provider: <provider> model: <model> period-resets: <ISO time or unknown>
```

Read the numbers from the session's own usage view (`/cost` in Claude Code, the
provider's usage API otherwise). If a usage limit stops you mid-task: checkpoint first
with a resume token, then send the NOTE with `stopped: usage-limit` appended, and stop.
When the limit period resets the meter starts again from zero; nothing is owed across the
reset. Never put an estimate in a bid, a progress note, or a question.

## Coordinator

### 1. Offer

`delegation-worker-list`, pick a worker whose capabilities cover the task's tags and
tier, then `delegation-offer`:

- `workerId`, `leaseSeconds` (small tasks 900, a module change 3600, at most 86400);
- `spec.objective`: what done means, in prose;
- `spec.allowedScope`: repositories, paths, hosts the worker may touch, with `read-only`
  where that applies;
- `spec.constraints`: the tier and tags on the first line (`tier: tier-1; tags:
  java-gradle, proto-buf`), then rules (`no pushes to main`, `tests first`);
- `spec.requiredChecks`: slug names with a description of the command that proves each
  one (`gradle-gate: ./gradlew clean build test exits 0 on the branch tip`).
- `resumeFrom`: the worker's last checkpoint token when re-offering after a lease
  expiry or restart.

Save the returned `taskId`. Before retrying an offer that may have gone through, read
`delegation-transcript` for the worker; the verbs have no idempotency keys yet.

### 2. Watch and answer

Same `delegation-watch` loop. Answer QUESTION messages with
`kind: TASK_MESSAGE_KIND_ANSWER`, `sender: coordinator`, `recipient: <workerId>`,
`replyTo: <message id>`. Unprompted direction is GUIDANCE.

### 3. Review

On a `completion` event, verify the evidence yourself before deciding; the reviewer
seam ships only manual and accept-all implementations:

- every commit: `git fetch <repo> && git cat-file -t <sha>` (or
  `git merge-base --is-ancestor <sha> origin/<branch>` when the check says "on main");
- every artifact: fetch it and compare the sha256;
- every required check: re-run it when it is cheap, otherwise read the `detail` and the
  commit's CI status.

Then `delegation-review`: `REVIEW_DECISION_ACCEPT` with a `verdict` that says what was
checked, or `REVIEW_DECISION_REVISE` with `feedback` and `failedChecks` (the check names
that did not hold). `delegation-cancel` with a `reason` ends an attempt that should not
continue.

### 4. Close the books

When the worker's `tokens-spent` NOTE arrives, record it against the worker in the
task's notes (a later ledger will read these messages). A task without the NOTE is not
closed.

## Reading state

- `protomolt://delegation/workers`, `protomolt://delegation/tasks`,
  `protomolt://delegation/tasks/{taskId}/transcript`: bounded, read-only.
- `delegation-transcript` with `taskId` for one task, or from a cursor for everything.

Reference: `docs/transform/delegation.md` (lifecycle, sequencing, durability),
`docs/surface/mcp.md` (verb table), `docs/design/work-tags.md` (tiers and tags),
`docs/apps/agent-host.md` (how Kimi, Codex and local models join as workers).
