---
name: protomolt-worker
description: Take or hand out bounded work through the ProtoMolt delegation coordinator (the `protomolt` MCP server). Invoked with no arguments it registers this session as a worker and starts watching for offers; `/protomolt-worker <worker-id> [tag ...]` sets the identity and tags; `/protomolt-worker coordinator` runs the offering and reviewing side.
argument-hint: "[worker-id [tag ...] | coordinator]"
---

# Working through ProtoMolt delegation

You have been invoked to act, not to read. Do the steps under "On invocation" now, in
order, and tell the user what happened after each one. The reference sections below
explain the frames you will send and receive.

## On invocation

1. **Load the tools.** The `delegation-*` tools are deferred; their schemas are not in
   context until you fetch them. Call
   `ToolSearch` with `select:mcp__protomolt__delegation-worker-register,mcp__protomolt__delegation-worker-list,mcp__protomolt__delegation-watch,mcp__protomolt__delegation-accept,mcp__protomolt__delegation-progress,mcp__protomolt__delegation-checkpoint,mcp__protomolt__delegation-candidate,mcp__protomolt__delegation-message,mcp__protomolt__delegation-offer,mcp__protomolt__delegation-review,mcp__protomolt__delegation-cancel,mcp__protomolt__delegation-transcript`.
   If nothing comes back, the `protomolt` MCP server is not connected in this session:
   stop and tell the user to add it (`claude mcp add --transport http protomolt
   https://protomolt.rokkon.com/mcp --header "Authorization: Bearer <serve token>"`).
2. **Pick the role.** Arguments: `coordinator` means the coordinator role (skip to
   "Coordinator"). Anything else is the worker role; the first argument is the worker id
   and the rest are tags. With no arguments, use the defaults below.
3. **Choose identity and tags** (worker role). Worker id: the argument, else
   `<hostname>-claude-<short random suffix>` as a slug (lower-case letters, digits,
   hyphens). Tags: the arguments, else derive them from this host and repository, and
   always add one tier tag:
   - `tier-1` unless the user says otherwise;
   - `java-gradle` if `./gradlew` exists here; `proto-buf` if `buf` is on PATH;
     `node-vite` if `apps/console/package.json` exists; `rust-cargo` if `cargo` is on
     PATH; `cpp-cmake` if `cmake` is on PATH;
   - `git-forgejo` if `git remote -v` names git.rokkon.com; `forgejo-ci` with it;
   - `docs` and `review` always;
   - `gpu-cuda` if `nvidia-smi` succeeds; `gpu-intel` if the host is krick-1;
     `arm64` if `uname -m` is aarch64.
   Every tag must exist in `docs/design/work-tags.md`.
4. **Register.** Call `delegation-worker-register` with `workerId`, `provider`
   `anthropic`, `model` (your model id), and one `capabilities` entry per tag
   (`name` = tag, `description` = what backs it on this host). Report the reply to the
   user: worker id, `admitted`, `sessionId`. If `admitted` is false, stop and report the
   `reason`.
5. **Save the cursor.** Write `0` to `delegation-cursor.txt` in the scratchpad unless
   the file already exists from an earlier run; then always read the cursor from it.
6. **Watch.** Call `delegation-watch` with `afterCursor` = saved cursor,
   `timeoutMs` 30000, `maxEvents` 64. Each call blocks up to 30 seconds. After each
   reply, save the returned cursor and handle every event (table below). Tell the user
   once that you are watching, then again after every ten empty polls ("still watching,
   no offers in 5 minutes") and whenever an event arrives. Keep polling until an offer
   arrives, the user interrupts, or the user asked for a bounded wait. Do not fall
   silent: an empty batch is a normal result, not a reason to stop.

## Ground rules

- A Claude session joins only as itself, through Claude Code. Never drive Claude through
  the agent host's OpenAI-compatible provider or any other automation.
- Tokens are settled at the end of a task, never estimated up front (see "Token
  accounting").
- Evidence is a commit or an artifact plus the exact check that was run. "Done" is not
  evidence; the coordinator rejects a candidate without a commit or artifact reference.
- Work stays inside the offer's `allowedScope` and `constraints`. Anything else is a
  QUESTION to the coordinator first.
- Skill tags come from `docs/design/work-tags.md`; invent a tag only by adding it there.
- Never print the MCP bearer token.

## Worker

### Events

| Event | Action |
|---|---|
| `offer` for your worker id | read the spec, then accept or ask (below) |
| `accepted` for your task | the task is done; send the token note (below), then keep watching |
| `revision` (review returned it) | read `feedback` and `failedChecks`, fix, resubmit with `revision` + 1 |
| coordinator `message` to you | answer (ANSWER, `replyTo` the message id) or follow the guidance |
| `cancel` | stop work on that attempt immediately, keep watching |
| anything for another worker | ignore, but still save the cursor |

Re-registering with the same worker id after a disconnect or a server restart resumes
the recorded stream; `delegation-watch` from the saved cursor replays nothing twice.

### Accept or ask

Before `delegation-accept` (`workerId`, `taskId`, `attempt` from the offer) make sure
every `requiredChecks` entry is something you can run and every `allowedScope` entry is
something you can reach. If not, send `delegation-message` with
`kind: TASK_MESSAGE_KIND_QUESTION`, `sender: <workerId>`, `recipient: coordinator`, and
wait for the ANSWER before accepting. The lease clock runs from the offer, not from acceptance: `expiresAt` in
the offer is the deadline for the attempt, so accept promptly and plan the
work inside what remains. Tell the user what you accepted.

### Work, report, checkpoint

- Work in a worktree named after the task, never in a shared checkout.
- `delegation-progress` after each meaningful step (a test written, a build green, a
  finding). One line, at most 4096 characters, no secrets.
- `delegation-checkpoint` at every point you could resume from: `resumeToken` is a
  string you can act on later (`verified:<sha>`, `branch:<name>@<sha>`), `note` says
  what remains. A later offer's `resumeFrom` hands the token back to you.
- Questions go through `delegation-message` (QUESTION), not progress notes.

### Submit the candidate

`delegation-candidate` with `candidate`:

- `attempt`, `revision` (1 on the first submission, then +1 per resubmission);
- `summary`: what changed and where, in a few sentences;
- `evidence`: one entry per required check, `checkName` matching the offer exactly,
  `verdict` `CHECK_VERDICT_PASSED` or `CHECK_VERDICT_FAILED` (a failed check is still
  reported, with the reason), `ranAt` as an ISO timestamp, `detail` = the exact command
  and its result;
- `commits`: `{repository, commit (full 40-hex sha1), subject}` for every commit that
  carries the work, or `artifacts`: `{sha256, mediaType, sizeBytes}` for produced files;
- `result`: the typed deliverable, but **only** when the offer's
  `spec.contract` was set. See below.

Then go back to watching: the coordinator answers with `accepted` or `revision`.

### The typed deliverable

When the offer carries `spec.contract`, the task is judged on a message, not only on
prose. The contract gives you `typeName` (the message you must produce) and
`jsonSchema` (that message's shape and bounds, already rendered; you do not need
protoc or the descriptor set). Build the deliverable against that schema and send it as
`candidate.result` in the packed `Any` spelling:

```json
"result": {
  "@type": "type.googleapis.com/<typeName>",
  "<field>": "<value>"
}
```

`@type` is the only accepted spelling, and `<typeName>` must match the contract exactly.
The coordinator unpacks the message and runs it against the rules the contract's own
descriptor set declares, before any human or model reviews it, so a missing required
field, a string that is too short, or a failed cross-field rule refuses the candidate
mechanically. The refusal names the rule and the field (`the deliverable violates
string.min_len at result.headline: ...`); fix it and resubmit with `revision` + 1.

Two matching errors are refused the same way: sending no `result` when the offer named a
contract, and sending a `result` when it did not.

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

Invoked with `coordinator`: load the tools (step 1 above), then call
`delegation-worker-list` and show the user the admitted, connected workers and their
tags. Ask the user what to offer if they have not said; otherwise proceed.

### Offer

Pick a worker whose capabilities cover the task's tags and tier, then `delegation-offer`:

- `workerId`, `leaseSeconds` (small tasks 900, a module change 3600, at most 86400);
- `spec.objective`: what done means, in prose;
- `spec.allowedScope`: repositories, paths, hosts the worker may touch, with `read-only`
  where that applies;
- `spec.constraints`: the tier and tags on the first line (`tier: tier-1; tags:
  java-gradle, proto-buf`), then rules (`no pushes to main`, `tests first`);
- `spec.requiredChecks`: slug names with a description of the command that proves each
  one (`gradle-gate: ./gradlew clean build test exits 0 on the branch tip`);
- `spec.context`: content-addressed inputs the worker starts from (each an
  artifact reference with `sha256` and `mediaType`), by reference only;
- `spec.contract`: the typed deliverable, when the task must produce a message rather
  than only a commit. `descriptorSet` is a serialized `FileDescriptorSet` as base64
  bytes, taken from `reflect` (a live service), `compile` (inline `.proto` sources),
  or a registry read. `typeName` is the full proto name of the message inside it.
  Leave `jsonSchema` empty: the coordinator renders it from the descriptor set and the
  worker reads it off the offer. An offer naming a type its descriptor set does not
  define is refused as `invalid-input`;
- `resumeFrom`: the worker's last checkpoint token when re-offering after a lease
  expiry or restart.

Save the returned `taskId` and tell the user. Before retrying an offer that may have
gone through, read `delegation-transcript` for the worker; the verbs have no idempotency
keys yet.

### Watch and answer

Same `delegation-watch` loop as the worker, with `taskId` set when only one task
matters. Answer QUESTION messages with `kind: TASK_MESSAGE_KIND_ANSWER`,
`sender: coordinator`, `recipient: <workerId>`, `replyTo: <message id>`. Unprompted
direction is GUIDANCE. Relay progress and checkpoints to the user as they arrive.

### Review

On a `completion` event, verify the evidence yourself before deciding; the reviewer
seam ships only manual and accept-all implementations. A candidate that reached you at
all already satisfies the task's deliverable contract, if it declared one: the
coordinator checks `result` against the contract's rules before the candidate becomes
reviewable, so the mechanical part is done and what is left is judgement:

- every commit: `git fetch <repo> && git cat-file -t <sha>` (or
  `git merge-base --is-ancestor <sha> origin/<branch>` when the check says "on main");
- every artifact: fetch it and compare the sha256;
- every required check: re-run it when it is cheap, otherwise read the `detail` and the
  commit's CI status.

Then `delegation-review`: `REVIEW_DECISION_ACCEPT` with a `verdict` that says what was
checked, or `REVIEW_DECISION_REVISE` with `feedback` and `failedChecks` (the check names
that did not hold). `delegation-cancel` with a `reason` ends an attempt that should not
continue.

### Close the books

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
