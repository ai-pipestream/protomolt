# Task console

The task console is the browser view of ProtoMolt delegation. It lists the
workers currently known to the coordinator, projects each durable task into its
current lifecycle state, follows its cursor-ordered transcript, sends structured
task messages to a worker, offers new tasks, judges completion candidates, and
hands a finished task over as a signed record. Open it at `/console/tasks` on a
`protomolt-serve` instance; the console's router redirects its root to that
view.

Everything the console shows is a projection of the coordinator's recorded
transcript. The timeline renders protocol facts: offers, acceptance, progress,
checkpoints, completion candidates, review decisions, cancellation, and task
messages. It does not request or display a provider's private reasoning, and it
never invents a fact the transcript does not carry. Refreshing the page or
reconnecting after a network failure resumes from the recorded cursor.
Repository-backed delegation storage preserves the same transcript across
coordinator restarts, so the console reattaches to a restarted server and finds
the same tasks at the same cursors.

> **Run it in a container.** The console ships inside the server image. Build
> the distributions, then the images, then bring the stack up:
> `./gradlew :protomolt-serve:installDist :protomolt-acp-agent:installDist &&
> docker compose build && docker compose up`. The `serve` container starts with
> `--demo`, which seeds a throwaway git registry, a sample schema, and a sample
> workflow, so every surface has something to answer.
> [Running in Docker](docker.md) covers the images, profiles, and the ACP agent
> in full.

> **Where it listens.** The server exposes 8080 for HTTP — the console at
> `/console`, Swagger at `/docs`, MCP at `/mcp` — 9090 for gRPC with reflection
> enabled, and 8081 for the git-backed schema registry. When something local
> already holds those ports, override the host side without editing any file:
> `PROTOMOLT_HTTP_PORT`, `PROTOMOLT_GRPC_PORT`, and `PROTOMOLT_REGISTRY_PORT`.

## The coordinator behind the page

`protomolt-serve` creates one in-process coordinator per server and gives the
console a bridge onto it. That coordinator admits every worker that says hello
and reviews no candidate on its own: its candidate reviewer is the manual one,
which leaves every completion candidate pending for an external judge. The
console session is that judge. Nothing in the delegation contract lets a worker
mark its own task done, and nothing in the server's default wiring quietly does
it on the worker's behalf.

The same coordinator is reachable as MCP tools on the same process, so a human
in the console and an agent on the wire steer one shared transcript rather than
two views that can disagree.

> **Point an agent at the same server.** The MCP endpoint is stateless
> streamable HTTP, so a client needs only the URL:
> `claude mcp add --transport http protomolt http://localhost:8080/mcp`. The
> delegation verbs and the read-only delegation resources are mounted there;
> see [Agent delegation](../transform/delegation.md) for the verb set.

## Browser authentication

Set the task console login token through the environment:

```shell
export PROTOMOLT_TASK_CONSOLE_TOKEN="$(openssl rand -base64 32)"
export PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS=43200
```

The token must contain 32 to 1024 characters. Session lifetime must be from one
second through seven days, and defaults to twelve hours. The login endpoint
(`POST /api/task-session`) exchanges the token for a random session identifier
returned as a `__Host-protomolt_task_session` cookie scoped `Path=/`, `HttpOnly`,
`Secure`, `SameSite=Strict`, with `Max-Age` set to the session lifetime. The
token is not placed in a URL, command-line option, browser storage, or task
transcript. Serve the login through HTTPS so browsers accept the Secure cookie.
`GET /api/task-session` reports `authenticated` and `loginRequired` — 200 with a
live session, 401 without — and `DELETE` revokes the session and clears the
cookie with a 204.

The console login is separate from `PROTOMOLT_API_TOKEN`, and every session is
bound to a caller for its whole lifetime. The console login token binds to the
`task-console` identity holding `worker-coordinate`, never to the operator; with
an [access policy](../design/authorization-scopes.md) mounted, a credential the
policy names logs in as its own principal instead. The task API answers 401 when
a request carries no live session and 403 when the session's principal does not
hold `worker-coordinate`, naming both the caller and the scope. The operator API
token is never a browser login.

When the process API token is configured, the browser receives access only to
these routes:

- `/console/` for static console assets;
- `/api/task-session` for login, status, and logout; and
- `/api/tasks` for bounded task summaries, workers, transcript events, offers,
  reviews, records, and task messages.

The general `/api/protomolt` registry proxy and `/api/serve` action proxy stay
disabled in this mode, answering with a handler that explains why. The MCP
bearer remains server-side and is never made available to browser JavaScript.
Without a process API token, the existing trusted-network console remains open;
setting a task console token still adds the browser login boundary.

## The workspace

Signed in, the page is three surfaces over one coordinator.

The **worker strip** is the coordinator's directory: one card per worker,
carrying its identity, a presence dot lit only when the worker is both admitted
and connected, an online-or-away chip, its declared provider and model (a worker
that declares neither reads as a deterministic worker), and a chip per declared
capability.

The **task list** holds every durable task the coordinator knows, ordered by
the cursor of its most recent recorded frame so the freshest work sits on top.
Each row carries the objective, the holding worker, the phase, and the attempt
number, with a phase-coloured avatar: accepted is a success tick, a failed,
blocked, cancelled, or expired task is an error alert, a task holding a
candidate is a distinct review icon, and anything else is a clock.

The **timeline** is the selected task's transcript. Each row names its lane —
the worker by identity, or the coordinator — the frame's protocol arm, and the
cursor that addresses it. Under that sits the frame's human line: its message,
summary, reason, feedback, verdict, note, or the offered objective, whichever
the arm carries. Under that again sit the frame's recorded facts, one per line:
acceptance-check evidence with its verdict and detail, named failed checks,
commits as repository, short hash, and subject, artifacts by URI or digest or
object key, and a checkpoint's state reference. Reducer findings for the task —
illegal transitions, stale leases, missing evidence, sequence gaps — render as
warnings above the rows rather than being folded into them.

## Live updates

The browser long-polls `GET /api/tasks/events` with its last recorded cursor,
its task id, a timeout, and a batch bound. The server returns only events after
that cursor, caps the batch at 256 events and the wait at 30 seconds, and
reports `truncated` when it had more than the caller asked for. The wait blocks
on a virtual thread, so an idle watcher costs a thread stack and nothing else.
The client deduplicates by cursor before rendering, so reconnecting cannot
display the same recorded frame twice, and it refreshes the task summaries
whenever a batch arrives.

## Offering a task from the browser

Offering is the one coordinator move a watcher could not otherwise make, so the
console exposes it directly (`POST /api/tasks/offer`). The dialog asks for a
worker from the directory, an objective in the worker's terms, comma-separated
allowed scopes, the acceptance checks that define done, and a lease in minutes.
The browser refuses to submit without a worker, an objective, and at least one
named check, and the requirement is the contract's, not the dialog's: the
coordinator validates every offer against the delegation contract, which
demands at least one acceptance check because a worker saying "done" is never
sufficient.

The server bounds every field of the offer the way it bounds every console body.
The objective is required and holds at most 4096 characters. `allowedScopes` is
an array of at most 64 entries, each a non-empty path of at most 512 characters.
`requiredChecks` is an array of at least one and at most 64 objects, each with
a required `name` of at most 128 characters and an optional `description` of at
most 2048.
`leaseMinutes` runs from 1 through 1440 and defaults to 30. The whole body is
capped at 20 KiB; a larger one answers 413.

The task identity is generated server-side as a UUID and returned with the
worker id under a 201, and the offer lands on the transcript like any other
protocol fact. Offering to a worker the coordinator does not hold as admitted
and connected answers 409 naming the worker, as does offering onto a task that
already has an open attempt. A malformed body — a field over its bound, a task
id that is not a UUID, invalid JSON — answers 400 naming the field.

## Judgement as a first-class act

A worker saying "done" is a claim, not a conclusion. The worker submits a
completion candidate carrying evidence for the required checks and at least one
commit or artifact reference; the manual reviewer leaves it pending; the task
sits in the `candidate` phase until a judge acts. The console renders that state
as a panel of its own, headed by the candidate's revision number and its
summary, because a task waiting on a person should not look like a task waiting
on a machine.

Two decisions are available, and both demand the reviewer's words
(`POST /api/tasks/{id}/review`). Accepting requires a verdict of at most 4096
characters saying why this candidate is done. Requesting a revision requires
feedback of at most 4096 characters saying what the next revision must change,
and optionally names the checks that failed — up to 64 names of at most 128
characters each, which the panel collects by toggling chips drawn from the
contract of done. Both go on the transcript as recorded protocol facts, because
a judgement without a reason is not one the transcript can defend later. The
route answers 200 with the decision and the task's resulting phase.

A review with no open candidate is refused with 409, and a review naming a task
the coordinator does not hold is refused with 400.

## The contract of done

The task header carries the offer's acceptance checks as the contract of done,
each joined with the latest candidate's evidence for it. The join is narrow on
purpose: only the evidence recorded on completion frames whose revision matches
the newest candidate counts, so a superseded revision's proof cannot vouch for
the one under review. A check whose evidence records a passing verdict renders
as passed, a check whose evidence records anything else renders as failed, and a
check no candidate has proved yet renders as unproven.

Absence of evidence is a state of its own, never rendered as passing. Hovering a
chip states what passing was meant to mean and either the recorded detail or a
flat admission that no evidence exists yet.

## The transcript

The transcript exports as a plain-text record. It opens with the task id, the
objective, the phase, the attempt, and the number of recorded frames, then
states on its face what it is: a projection of the recorded protocol frames, in
cursor order, carrying no provider reasoning and no claim beyond what was
recorded. Each frame follows as a block — its cursor, its lane, its protocol
arm, its human line, and its recorded facts as a bulleted list. The browser
saves it as `task-<first eight characters of the id>-transcript.txt`.

## The receipt handover

A terminal task hands over a receipt. `POST /api/tasks/{id}/record` projects the
task's transcript into a work-record manifest under the `delegation-task`
subject kind and signs it ([signed work records](../design/receipts.md)).

The projector adds nothing the transcript did not record. The subject carries
the task id, the worker id, and the fingerprint of the offered spec. The steps
are the lifecycle milestones — the offer with its objective, each accept attempt,
each candidate with its summary, each revision request with its feedback, and
the terminal fact — each with the recorded words as its summary. The accepted
candidate's artifacts ride as digests, and the transcript's own deterministic
bytes ride as a final content-addressed artifact, so a relying party holding the
transcript can check it against the record and one holding only the record knows
exactly which transcript it attests. An accepted task projects as complete; a
cancelled, failed, or expired one projects as partial with a reason that says
what the evidence cannot show. Completeness is evaluated against a committed
evidence policy named by id, version, and digest.

The route answers 200 with the signed record as base64 bytes, the manifest
digest, and the record id (`record-<task id>`); the browser saves the bytes as
`<record id>.pb`. The console offers the button only on a terminal phase —
accepted, failed, cancelled, or expired — and the projector independently
refuses a task still in flight with a 409, because a record claims what a
delegation produced and a live task is still producing. A task with no recorded
offer is refused the same way: there is no contract to attest.

> **Signing is an operator decision.** Exporting a record requires the server to
> hold signing material: `PROTOMOLT_RECEIPT_KEY_FILE`, `PROTOMOLT_RECEIPT_KEY_ID`,
> and `PROTOMOLT_RECEIPT_ISSUER`, the same environment workflow-run records sign
> under. Without them the record route answers 503 naming all three rather than
> emitting anything unsigned. The exported record verifies offline against a
> trust snapshot that authorizes the issuer for the `delegation-task` subject
> kind — including by
> [`protomolt-record-verifier`](record-verifier.md), which shares nothing with
> this runtime but the wire contract.

## Guidance and durable messages

Guidance is a durable `TaskMessage`, not an ephemeral chat packet. The browser
chooses a recipient from the worker directory and one of `guidance`, `question`,
`answer`, or `note`, and writes at most 16384 characters
(`POST /api/tasks/{id}/messages`, answering 201 with the recorded message). The
coordinator persists the message before it becomes visible to watchers. The
worker sees it in its next relevant event batch and its response returns through
the same transcript. A message never moves the lifecycle: it is not progress,
not a checkpoint, and not a review path.

## The bounded API

Every route below sits under the session and the `worker-coordinate` scope,
answers with `Cache-Control: no-store`, and reports failures as a JSON object
carrying a single `error` member.

| Route | Method | What it answers |
|---|---|---|
| `/api/tasks` | GET | Task summaries, the global cursor, and reducer findings |
| `/api/tasks/workers` | GET | The coordinator's worker directory |
| `/api/tasks/events` | GET | A bounded batch after a cursor, long-polled |
| `/api/tasks/{id}` | GET | One task, its recorded frames, and its findings |
| `/api/tasks/offer` | POST | A new durable task offer; 201 with the generated id |
| `/api/tasks/{id}/messages` | POST | A durable task message; 201 |
| `/api/tasks/{id}/review` | POST | Accept or revise the open candidate |
| `/api/tasks/{id}/record` | POST | The transcript as a signed work record |

Request bodies are capped at 20 KiB. A task id that is not a UUID is a 400, a
task the coordinator does not hold is a 404 on the detail route, and a lifecycle
conflict — an unadmitted worker, an occupied task, a candidate that is not
there, a task still in flight — is a 409 that names what it found.
