# Next session

Operational handoff, written 2026-09-01. Product backlog lives in
[docs/design/planned-work.md](docs/design/planned-work.md); this page is the
narrower thing that page is not: what is deployed right now, what is mid-flight,
and which traps cost time last session so they cost none this time.

Delete an entry once it is done. An empty page means the handoff is clean.

## Where the tree and the fleet stand

`main` is `dcb8582a` on both remotes. Forgejo is master; GitHub receives
`sync/forgejo-main` pull requests and its `main` is protected by five required
checks (`build (21)`, `build (25)`, `conformance`, `console`, `integration`).

| Where | Runs | Pinned to |
| --- | --- | --- |
| NAS coordinator (Portainer stack 20) | serve + repo-service | `c0e3d23d` |
| nano1 | mesh publisher, TEI | main, script `cc421cff` |
| krick-1 | kimi and glimmer agent hosts | `dcb8582a` |

The coordinator is one release behind the tree: `c0e3d23d` predates #205 and
#206. Neither is server-side, so nothing is owed there until the next deploy.

## Start here

### 1. Merge the open GitHub sync

Pull request #275 mirrors #205 and #206. It is a fast-forward and runs the same
five checks that passed four times last session. Merge it, then fast-forward
Forgejo `main` to the GitHub merge commit so the two match exactly, which is the
established shape of every earlier sync.

### 2. Make a tag deploy, so a script does not

This is the highest-leverage item and the one that repeatedly costs time. Three
deploys last session all went through a hand-run script in a scratch directory,
two of which needed a human because automation could not run them. Before that,
the image pin sat two weeks stale because publishing only happens on manual
dispatch. One cause, three symptoms.

The work: make publishing tag-only, add `.forgejo/workflows/deploy-nas.yml` on
`v*`, and cut `v0.1.0`. `.github/workflows/docker-publish.yml` is already closer
than it looks, carrying only `workflow_dispatch` and `workflow_call`; the `edge`
branch trigger is gone.

**Settle this before writing the workflow.** Axion cuts tags on GitHub while
Forgejo is master. If tags do not propagate to Forgejo, a `v*` workflow there
never fires and the whole thing looks broken for a reason that has nothing to do
with the workflow. Check propagation first.

### 3. Retire the DJL stack still running on nano1

`fabe0bfb` deleted the DJL/TensorRT stack from the tree on 2026-08-21. The
container never stopped: `protomolt-djl-jetson` has been up for over two weeks,
holding unified memory on an Orin that TEI now shares. Stop it, remove it, and
confirm `nano1-tei` still renews afterwards.

This is five minutes and it closes the loop on the outage below, since that dead
processor is what made the mesh look populated while the GPU was absent from it.

## Traps, each of which cost time last session

**The `nano1-tei` service profile does not survive the coordinator's volumes.**
It lives in the coordinator's registry, not on nano1, and the publisher drops the
processor when `service-inspect` cannot find it. An empty `service-list` after a
coordinator rebuild means the registration is owed again. See
[deploy/nano1/README.md](deploy/nano1/README.md) for the exact call.

**nano1 runs a hand-installed publisher.** `/opt/protomolt-mesh/scripts/` is a
copy, not a checkout, so the tree moving does not move it. It was seventeen days
stale and advertising a processor whose stack had been deleted. Compare
`sha256sum` against the repo before believing anything about that host.

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
