# Work tiers and skill tags

Delegated work is described with tags from this vocabulary. Workers advertise the tags
they hold as `WorkerCapability` names when they register; an offer names the tier and
the tags it needs on the first line of `spec.constraints`. The vocabulary is ours to
extend: add a row here in the same change that first uses the tag, and keep names as
slugs (lower-case, digits, hyphens).

Routing is by hand for now: a coordinator reads `delegation-worker-list` and picks a
worker whose capabilities cover the tier and tags. Capability-based routing is on the
list in [planned-work.md](planned-work.md); it will match on these same names.

## Tiers

A tier says how much a task may change and what proof closes it. Every offer carries
exactly one tier tag, and a worker holds the tiers it is trusted with.

| Tag | Work | Proof that closes it |
|---|---|---|
| `tier-0` | Read-only: investigate, measure, report. No commits. | An artifact (report, log, table) with a sha256, and the commands that produced it. |
| `tier-1` | A bounded change in one module: a fix with its test, a doc, a config value. | A commit on a branch, the module's tests green, the repo gate green when it is cheap. |
| `tier-2` | A feature or refactor across modules, or a new module. Design notes expected. | Commits on a branch, the full repo gate green, a pull request opened. |
| `tier-3` | Production and contract changes: deploys, releases, wire-level or published-API breaks, data resets. | Everything in `tier-2` plus a named person's acceptance in the review verdict. |

## Skill tags

| Tag | Holder can |
|---|---|
| `java-gradle` | Build and test JDK 25 Gradle modules in this workspace, read JUnit XML. |
| `rust-cargo` | Build and test Rust crates (grpc-pdf-inspector). |
| `cpp-cmake` | Build and test the C++ tree (gRParse), run its stack e2e. |
| `node-vite` | Run the console's npm test, typecheck and build. |
| `proto-buf` | Edit `.proto` files, run `buf build` and `buf lint`, read `buf breaking` output. |
| `grpc` | Call services with grpcurl or the `grpc-invoke` verb, read reflection. |
| `git-forgejo` | Branch, commit and open pull requests on git.rokkon.com; sync the GitHub mirror. |
| `forgejo-ci` | Read Forgejo and GitHub Actions results and logs. |
| `deploy-portainer` | Update the NAS Portainer stacks and verify containers. |
| `docs` | Write and fix Markdown under `docs/`, README and ADRs in the repo's register. |
| `review` | Verify evidence (commits, artifacts, checks) and write review verdicts. |
| `gpu-cuda` | Run CUDA workloads (nano1 Jetson, krick-1). |
| `gpu-intel` | Run workloads on the Intel B70 (krick-1: vLLM, OVMS). |
| `arm64` | Build and test on linux/arm64 (nano1). |
| `local-model` | Serve or call a local model endpoint (OVMS, vLLM, TEI). |

## Writing the constraint line

```
tier: tier-1; tags: java-gradle, git-forgejo
```

Tags after the tier are all required. A worker missing one asks before accepting.

## Provider notes

Claude workers are Claude Code sessions and nothing else; Anthropic does not permit
driving Claude through other automation. Kimi, Codex, Cursor and local models join
through the agent host (see [agent-host](../apps/agent-host.md)) and hold the tags
their host grants them. Token usage is reported by the worker when a task ends, never
predicted; see the `protomolt-worker` skill.
