# Task console

The task console is the browser view of ProtoMolt delegation. It lists the
workers currently known to the coordinator, projects each durable task into
its current lifecycle state, follows its cursor-ordered transcript, and sends
structured task messages to a worker. Open it at `/console/tasks` on a
`protomolt-serve` instance.

The timeline shows protocol facts recorded in the delegation transcript:
offers, acceptance, progress, checkpoints, completion candidates, review
decisions, cancellation, and task messages. It does not request or display a
provider's private reasoning. Refreshing the page or reconnecting after a
network failure resumes from the recorded cursor. Repository-backed
delegation storage preserves the same transcript across coordinator restarts.

## Browser authentication

Set the task console login token through the environment:

```shell
export PROTOMOLT_TASK_CONSOLE_TOKEN="$(openssl rand -base64 32)"
export PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS=43200
```

The token must contain 32 to 1024 characters. Session lifetime must be from
one second through seven days. The login endpoint exchanges the token for a
random `__Host-` scoped, HttpOnly, Secure, SameSite=Strict cookie. The token is
not placed in a URL, command-line option, browser storage, or task transcript.
Serve the login through HTTPS so browsers accept the Secure cookie.

The console login is separate from `PROTOMOLT_API_TOKEN`, and every session
is bound to a caller: the console login token binds to the `task-console`
identity holding `worker-coordinate`, and with an
[access policy](../design/authorization-scopes.md) mounted a credential the
policy names logs in as its principal — the task API refuses a session whose
principal does not hold `worker-coordinate`, naming both. The operator API
token is never a browser login. When the process API
token is configured, the browser receives access only to these routes:

- `/console/` for static console assets;
- `/api/task-session` for login, status, and logout; and
- `/api/tasks` for bounded task summaries, workers, transcript events, and
  task messages.

The general `/api/protomolt` registry proxy and `/api/serve` action proxy stay
disabled in this mode. The MCP bearer remains server-side and is never made
available to browser JavaScript. Without a process API token, the existing
trusted-network console remains open; setting a task console token still adds
the browser login boundary.

## Live updates and guidance

The browser long-polls `/api/tasks/events` with its last recorded cursor.
Each response contains only events after that cursor, capped at 256 events and
a 30-second wait. The server's wait is safe on virtual threads. The client
deduplicates by cursor before rendering, so reconnecting cannot display the
same recorded frame twice.

Guidance is a durable `TaskMessage`, not an ephemeral chat packet. The browser
chooses a recipient and one of `guidance`, `question`, `answer`, or `note`.
The coordinator persists the message before it becomes visible to watchers.
The worker sees it in its next relevant event batch and its response returns
through the same transcript.
