# Scripts

Two kinds of scripts live here: general contributor tools that work from any
clone, and maintainer-host scripts that serve one named machine in the
maintainer's fleet. The host-only ones are listed so nobody mistakes them for
part of the contributor workflow.

## General contributor tools

| Script | What it does |
|---|---|
| `protomolt-serve.sh` | Start/stop/status a local `protomolt-serve`, detached from the terminal, so the MCP endpoint stays up for MCP clients. See the header for the port defaults. |
| `docker-smoke.sh` | Builds the serve and acp images, brings the Compose stack up, and proves the REST, MCP and ACP surfaces answer. |
| `serve-dhi-smoke.sh` | Builds the hardened public demo image (`apps/serve/Dockerfile.dhi`) and proves `--demo` end to end. |
| `acp-grpc-live.sh` | Self-hosting proof: through the ACP agent container, reflect and invoke ProtoMolt's own running gRPC service over the Compose network. |
| `agent-host-live.sh` | Opt-in live acceptance for the agent host: drives one bounded delegation task with real provider processes. Its default endpoint points at the maintainer deployment; override it to run against your own server. |
| `worker-images-smoke.sh` | Builds both coding worker images and exercises their language-specific gRPC generators. CPU only, no inference service. |
| `service-workspace-smoke.mjs` | Rehearses the service-workspace tutorial against the published authoring starter (Node 22+). |
| `check-deployment-statics.sh` | Static checks for the deployment package: shell syntax, YAML, secret placeholders, Dockerfile layout, no em dashes. |
| `check-proto-compatibility.sh` | `buf breaking` against a baseline git ref. |
| `check-stale-build-output.sh` | Finds `build/` output and `.class` files orphaned by module or package renames. |

## Maintainer-host scripts

These serve one machine each in the maintainer's fleet (`krick-1`, `nano1`,
the forge hosts). They assume hardware, credentials or network reachability a
contributor does not have; the matching deployment configs live under
`deploy/<host>/`.

| Script | Host | What it does |
|---|---|---|
| `krick1-embeddings-live.sh` | krick-1 | Live check of the krick-1 embeddings stack. |
| `muse-glimmer-live.sh` | krick-1 | Live check of the Muse Glimmer worker and its vLLM sidecar. |
| `nano1-build-tei.sh` | nano1 | Builds the TEI image natively on the ARM64 Jetson. |
| `nano1-tei-live.sh` | nano1 | Live check of the nano1 TEI service. |
| `nano1-worker-build-smoke.sh` | nano1 | Builds one coding worker natively on nano1 and checks the ARM64 image. |
| `nano1-mesh-publisher.py` | nano1 | The mesh capacity publisher that runs on nano1. |
| `test_nano1_mesh_publisher.py` | any | In-process contract tests for the nano1 publisher; they run anywhere but exist to guard that host's script. |
| `sync-remotes.sh` | forge admin | Syncs the Forgejo and GitHub mains without ever rewriting either; requires push access to both forges. |
