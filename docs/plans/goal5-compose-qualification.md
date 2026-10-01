# Goal 5 authoring Compose qualification

The published starter passed native and anonymous download qualification on
AMD64 and ARM64. See [release evidence](goal5-release-qualification.md).
The local development evidence below predates that release.

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

## Packaged review recovery gate

`AuthoringComposeReviewTest` uses explicit digest-pinned images in an isolated
Compose project. It pauses the idle scripted author so the test can submit
controlled candidates through that author's scoped RPCs. Chromium starts the
task. An invalid envelope and an invalid typed deliverable must be refused
before review; the latter must leave the persisted events unchanged.

After successful preparation, the test stops the fixture before submission.
Chromium must show the infrastructure failure and the retry action. The fixture
then restarts on its existing volume. A reflection call through the same running
coordinator proves that its DNS resolution and connection to the fixture have
recovered before the browser retries once. The browser must show acceptance,
and a fresh retry key naming the old invocation must be refused without changing
the accepted review or events. The current retry route returns HTTP 400 with
`review retry does not match the latest failed candidate` for this refusal.

Run this opt-in gate with both `PROTOMOLT_AUTHORING_REVIEW_IMAGE` and
`PROTOMOLT_REPO_REVIEW_IMAGE` set to registry digests or local content IDs:

```sh
./gradlew :samples:test --tests '*AuthoringComposeReviewTest' --rerun
```

Node 22 or newer and Chromium (`CHROME_BIN` when not `google-chrome`) are needed
on the qualification host only. Screenshots and browser logs are written under
`samples/build/authoring-review-evidence`. Missing both image variables skips
the test; missing one fails. Publication checks require a passing, unskipped
JUnit report on each native architecture.

Local AMD64 qualification on 2026-10-01 ran this gate together with
`AuthoringComposeCrashTest` against the integrated-source image IDs recorded
in `goal5-packaged-crash.md`. Both tests passed with no skips, failures or errors:
review recovery in 72.512 seconds and crash recovery in 34.279 seconds. The
failure and accepted browser screenshots were inspected. This remains local
package evidence, not published-image or ARM64 qualification.

## Publication and repeat qualification

`.github/workflows/authoring-starter-publish.yml` publishes from an exact
merged main commit with the matching `authoring-starter-<12sha>` tag already
pushed to Forgejo and GitHub. Normal dispatch builds on native AMD64 and ARM64
runners. Each runner records immutable image digests, qualifies those images
through the browser and packaged recovery tests, and publishes a digest receipt
only after those checks pass. Manifest assembly checks both platform children
against those receipts before constructing the digest-pinned download.

The release ZIP and its `.sha256` sidecar are immutable prerelease assets.
An asset may exist before the anonymous downloaded-package checks finish.
Both native download jobs must pass before claiming package qualification.

If a download check fails after both assets were uploaded, dispatch the same
workflow from main with `verify_existing_release=true` and `release_tag` set
to the existing starter tag. This mode verifies the checksum, source ancestry,
Compose and browser content, and image references, then repeats the anonymous
download checks on both architectures. It does not rebuild images or replace
assets. Native build/recovery evidence remains attached to the original run.
A missing checksum or partially uploaded release fails closed and requires
operator investigation; requalification does not fabricate a missing asset.
