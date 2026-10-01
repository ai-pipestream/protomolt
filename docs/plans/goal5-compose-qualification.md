# Goal 5 authoring Compose qualification

Local AMD64 qualification on 2026-10-01 used locally built images from
`feat/goal5-authoring-compose`, including its uncommitted starter changes.
This is not published-image, ARM64, anonymous-pull, live-model, or NAS evidence.

The Compose stack ran repository-service with conditional writes against
RustFS, separate repository and jobs PostgreSQL databases, a persistent
fixture, coordinator, and scripted author. The first startup exposed a
delegation-only signing profile; the explicit authoring profile now trusts
delegation-task and workflow-run receipts. Existing profiles cannot be
silently changed. Four signing identity tests and six policy seed tests
passed without skips. The Compose static checks passed.

`deploy/authoring/browser-smoke.mjs` completed the browser flow on a fresh
project: connect, author a workflow, wait for independent acceptance,
prepare protobuf JSON input, launch, and observe execution completed.

- Project: `goal5-authoring-qualification-v2`
- HTTP: `http://127.0.0.1:18440`
- Task: `7620e8b3-b791-4e49-9dc6-671501c65c7d`
- Fixture operation: `8c19833b-3ae5-496c-8be4-a5d6186bbfc9`
- Job: `13135245-659b-420c-a116-008096ec461b`
- Result: completed on attempt 1 of 3
- Local browser log: `/tmp/goal5-authoring-browser.log`
- Local screenshot: `/tmp/goal5-authoring-browser.png`

The screenshot was inspected and shows accepted review, the launch input,
job and authorization identities, and execution completed. The author is
scripted; this proof makes no claim about a live model generating code.

Full-stack restart preserved the first completed job at attempt 1. A second
browser workflow completed after restart with fixture operation
`9f4e2bbb-abdd-4b0b-a0aa-e22da484ccda` and job
`46710434-06c1-4521-982c-1bf95a3464b9`. The first restart attempt exposed a
browser harness race against the previous selected task; the harness now
waits for selection to change before using the launch form. Its corrected
run exited successfully, recorded in
`/tmp/goal5-authoring-browser-restart-fixed.log`.

Repository initialization also needed bounded retries when a full-stack
restart bypassed initial dependency ordering. After that change, a second
restart left all initializers at exit 0 and both jobs still COMPLETED at
attempt 1. This is clean restart evidence, not mid-execution crash proof.

Remaining gates include review and landing, restart/crash qualification of
the packaged stack, native AMD64/ARM64 publication and execution, clean
anonymous pulls/downloads, and release-bundle qualification. Existing
installed-process crash tests do not establish those image release gates.
