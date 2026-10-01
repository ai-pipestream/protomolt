# Workflow authoring starter (source template)

This is an unpublished image-only Compose template for the scripted
NormalizeText → WriteRecord workflow. It is not an installation or release.
No image is built by Compose. A release must provide digest-pinned
`PROTOMOLT_AUTHORING_IMAGE` and `PROTOMOLT_REPO_IMAGE` values, plus digests for
the external images, in a verified `.env`. The source checkout has no working
image tag fallback for ProtoMolt. Building the shared authoring image belongs
in the native AMD64/ARM64 publishing workflow, not on the starter host.

The shared authoring image contains the installed `authoring-coordinator`,
`authoring-fixture`, `authoring-worker`, and `authoring-policy-seed` launchers.
`samples/Dockerfile.authoring` copies the distributions produced by Gradle at
image-build time. The coordinator launcher includes the sample template
provider `normalize-record-v1`; the ordinary Serve image does not.

After a verified bundle supplies its `.env`, run from its extracted directory:

```sh
docker compose config --quiet
docker compose pull
docker compose up -d --pull never
docker compose ps --all
```

`bootstrap`, `signing-init`, `rustfs-init`, `repo-init`, and `policy-seed` must
exit successfully. `repo-postgres`, `jobs-postgres`, `rustfs`, `repo-service`,
`fixture`, `serve`, and `author` remain running. The coordinator's HTTP and
gRPC ports bind to loopback only. The fixture is reachable inside Compose at
`fixture:9778` and stores its operation records in `fixture-records`. The
idle author discovers its own assignments with a persistent state volume.

The policy initializer writes the exact sample descriptor, fixture input,
expected output, and permitted-call artifacts under the persistent workflow
workspace. It writes `policy.sha256` only after verification. Serve reads that
digest without evaluating it as shell code and refuses a missing or invalid
digest. An existing incompatible policy, authorization record, preparation
intent, or signing identity is a startup error, not a reason to replace state.
Retain the Compose project name and named volumes across restarts. Avoid
`docker compose down -v` if you intend to keep evidence or job history.

The signing initializer uses the explicit `authoring` profile, which trusts
`delegation-task` and `workflow-run` receipts from its persisted local key.
The agents starter keeps its original delegation-only profile. Reusing an
identity volume from a different profile fails without changing the key or
trust snapshot; use a separate project for this starter.

If credential bootstrap stops before writing its ready marker, it refuses
the partial secret volumes on the next start. Restore a consistent backup;
for a disposable installation, an operator can explicitly remove that
project's volumes and start again. Bootstrap never rotates partial credentials
automatically. Repository initialization retries temporary connection failures
with a bounded timeout when services restart together.

The token volumes separate authority. The idle worker receives only the
`scripted-author` credential with `workflow-author`; it has no operator,
coordinator-review, browser-launch, database, or signing credential. The
browser launch principal has `worker-coordinate` and `workflow-launch` for
the narrow launch console; the default console token lacks launch authority.
The coordinator-review token has `worker-coordinate` and is kept separate
from the author. The operator token and signing seed stay inside Serve.
For a local browser session, retrieve the appropriate token on the Docker
host, then sign in at `http://127.0.0.1:8080/console/tasks`:

```sh
docker compose exec -T serve cat /run/browser/token
```

Use the browser token for this authoring walkthrough. The separate
`/run/console/token` credential is for the general task console and cannot
launch an accepted workflow.

The flow is browser author start, idle author assignment and candidate
preparation, independent review, accepted workflow launch with a pinned
input, and explicit job-status refresh. A submitted launch does not imply a
completed job. The repository service enables conditional S3 writes only
for the pinned RustFS backend used here; the jobs database and fixture
record volume provide separate durable state. The starter must be qualified
through actual browser and gRPC acceptance, process restart/crash tests,
native AMD64 and ARM64 image runs, and a fresh anonymous bundle download
and image pull before it can be described as published.

The optional accepted-workflow Kafka bridge is intentionally outside this
base Compose stack. It needs a private dedicated launch-intent topic, broker
ACLs, its own `workflow-launch` token mount, and a stable consumer group.
Its topic is distinct from the mutable-name `WorkflowRunRequest` topic; see
`samples/ACCEPTED_WORKFLOW_KAFKA_BRIDGE.md` when that sample is landed.
