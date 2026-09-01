# Next session

Operational handoff, written 2026-09-01. Product backlog lives in
[docs/design/planned-work.md](docs/design/planned-work.md); this page is the
narrower thing that page is not: what is deployed right now, what is mid-flight,
and which traps cost time last session so they cost none this time.

Delete an entry once it is done. An empty page means the handoff is clean.

## Where the tree and the fleet stand

`main` is `b6850989` on both remotes. Forgejo is master; GitHub receives
`sync/forgejo-main` pull requests and its `main` is protected by five required
checks (`build (21)`, `build (25)`, `conformance`, `console`, `integration`).

| Where | Runs | Pinned to |
| --- | --- | --- |
| NAS coordinator (Portainer stack 20) | serve + repo-service | `c0e3d23d` |
| nano1 | mesh publisher, TEI | main, script `cc421cff` |
| krick-1 | kimi and glimmer agent hosts | `dcb8582a` |

The coordinator still runs `c0e3d23d`, which now predates #205, #206, #207 and
the sync merge. None of it is server-side, so nothing is owed there until the
first tag deploy, which is what will move the pin off `edge` digests entirely.

## Start here

### Cut v0.1.0 and let the tag deploy

Everything this needs is in place and verified; what is left is the release
itself, which is deliberately not automated because it publishes to Maven
Central and that cannot be taken back.

`.forgejo/workflows/deploy-nas.yml` runs on `v*` on a Forgejo LAN runner. It
refuses to touch the stack until both images are published, redeploys stack 20
from the tagged `compose.yml`, waits for `UP`, and then asks the Docker endpoint
whether the container is really running the released image.

Before the first release, add the `PORTAINER_TOKEN` Forgejo secret the lane
reads. Nothing else is owed. Then:

1. Confirm GitHub `main` already has `deploy-nas.yml`. Axion tags the GitHub
   commit, and Forgejo runs the workflow file found in the tagged tree, so a tag
   cut before the sync arrives would deploy no stack at all.
2. Dispatch **Release and Publish** on GitHub with the `patch` bump. With no
   `v*` tag in either forge, axion's first release is `v0.1.0`.
3. From the LAN, `git push origin v0.1.0`. That push is the deploy.

**Do not work the propagation question again.** Axion cuts tags on GitHub, and a
tag pushed there does not show up on Forgejo. A test tag was checked on both
forges at 60-second intervals for 12 minutes: GitHub had it in all 13 samples,
Forgejo in zero. The configuration explains that, and rules out a slow sync
later. Forgejo reports `mirror: false` with no push mirror, no webhook exists on
either side, and no GitHub workflow mentions `rokkon.com`.

The deploy cannot move to GitHub either. Portainer and Forgejo both answer on
`192.168.1.211`, which no GitHub-hosted runner can connect to, and the only
GitHub self-hosted runner is nano1, an ARM64 Jetson. Forgejo's `nas-1` and
`nas-2` runners are on the NAS, so the lane belongs there, and one manual
`git push origin v<semver>` is the bridge.

## Traps, each of which cost time last session

**The `nano1-tei` service profile does not survive the coordinator's volumes.**
It lives in the coordinator's registry, not on nano1, and the publisher drops the
processor when `service-inspect` cannot find it. An empty `service-list` after a
coordinator rebuild means the registration is owed again. See
[deploy/nano1/README.md](deploy/nano1/README.md) for the exact call.

**nano1 runs a hand-installed publisher.** `/opt/protomolt-mesh/scripts/` is a
copy, not a checkout, so the tree moving does not move it. It is currently in
sync — the host and repo both hash `cc421cff` — but it was seventeen days stale
once and will drift again. Compare `sha256sum` against the repo before believing
anything about that host.

**krick-1 needs its environment spelled out.** A `BatchMode` ssh has no
`JAVA_HOME`; the JDK is at `/home/krickert/.sdkman/candidates/java/current`. The
agent-host image needs `./gradlew :protomolt-agent-host:installDist` first, and
`docker build` from `apps/agent-host/`, not the repository root. A stale
`build/install` directory looks exactly like a fresh one.

**Recreate workers with `--no-deps`.** Without it, recreating `glimmer-worker`
also restarts `protomolt-glimmer-vllm`, which reloads a 30B model and takes the
worker down for about twenty seconds. This was learned by doing it.

**`aws s3api list-objects` fails against rustfs** with "badly formed help
string". `aws s3 ls s3://protomolt/ --recursive` works, and its timestamps are
local rather than UTC.

## Deferred on purpose

Recorded so nobody relitigates them without new information.

**glimmer's model.** `muse-glimmer-30b` emits a bare `delegation-accept` and no
candidate, so its tasks never complete. This is model capability, not
infrastructure: kimi does accept, answer, and candidate in one turn in under half
a second on the identical image. The host now reports the stall by name, so the
evidence is in the log whenever this is worth picking up. Delegation works
through kimi meanwhile, and glimmer is free local inference rather than anything
load-bearing.

**Delegation transcript compaction.** `InProcessDelegationCoordinator.append`
rebuilds the entire transcript on every entry and carries the same two walls the
mesh log had, with no compaction anywhere in `transform/delegation`. Measured at
9.1 KiB, which is 0.11 percent of the 8 MiB cap, because it grows with task
activity rather than with time. The fix mirrors `DirectoryCheckpoint`. Not a
fire.

**Repository storage.** The claim-check store scales; two of its clients do not.
The mesh directory and the delegation transcript both use `PutBlob`/`GetBlob`
against one fixed key as a mutable register, ignoring the parts model the store
is built around. `PutBlobRequest` carries no etag, version, or precondition, so
concurrent writers clobber each other silently and correctness depends on there
being exactly one coordinator serialized by an in-process lock. That is a
correctness ceiling rather than a capacity one, and it only binds when a second
coordinator is wanted. The cheapest real fix is a conditional write; the honest
one is moving membership into the Postgres ledger that is already deployed.
