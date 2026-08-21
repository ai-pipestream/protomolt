# MacBook Air native embedding endpoint

The MacBook Air (`kristians-macbook-air`, Mac14,2, Apple M2, 24 GB,
macOS 26.5.1) hosts a native embedding endpoint through LM Studio's local
HTTP API. There is no Docker on this host by deliberate choice; LM Studio
runs its Metal llama.cpp engine directly on macOS. The endpoint is
loopback-only and unauthenticated, so it serves local processes and SSH
sessions, never the LAN.

The host currently provides:

- LM Studio 0.4.16+2 with the `lms` CLI at `~/.lmstudio/bin/lms`;
- the GGUF engine `llama.cpp-mac-arm64-apple-metal-advsimd@2.28.2` (the only
  GGUF engine family installed, so every GGUF load is a Metal engine); and
- `gpustack/bge-m3-GGUF/bge-m3-Q8_0.gguf` (BERT, 567M params, 634.55 MB),
  served as `text-embedding-bge-m3` with 1024-dimensional normalized
  embeddings and an 8192-token context.

The endpoint complements the Nano1 TEI deployment: Nano1 serves
384-dimensional `bge-small-en-v1.5` over gRPC, this host serves
1024-dimensional `bge-m3` over the OpenAI-compatible HTTP API. The vector
dimensions differ, so an index built from one is not comparable with the
other.

## Running

All commands run on the Mac over SSH. The `lms` binary is not on the
default PATH, so export it first:

```shell
export PATH="$HOME/.lmstudio/bin:$PATH"
```

Start the API on `127.0.0.1:1234` and load the model with full GPU
offload:

```shell
lms server start --port 1234 --bind 127.0.0.1
lms load --exact --gpu max "gpustack/bge-m3-GGUF/bge-m3-Q8_0.gguf"
```

`--bind 127.0.0.1` is the default and is spelled out so a later
`lms server start` without flags cannot silently inherit a wider bind.
Do not use `--bind 0.0.0.0` or the app's "serve on local network" toggle:
the API has no authentication, so any wider exposure must sit behind an
authenticating proxy with TLS, the same rule as the Nano1 endpoints.

Inspect state with:

```shell
lms server status
lms ps --json
```

Stop with:

```shell
lms unload text-embedding-bge-m3
lms server stop
```

## Verification

Verified 2026-08-21 against the running deployment:

```shell
curl -s -X POST http://127.0.0.1:1234/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{"model":"text-embedding-bge-m3","input":"ProtoMolt verifies embedding services on Apple Silicon."}'
```

returned HTTP 200 with `model: text-embedding-bge-m3`, a 1024-element
embedding whose first values are
`[-0.044396, 0.001336, -0.018048, -0.001018, 0.001529]`, in 144 ms cold
and 23 to 29 ms warm. A multi-paragraph input (about 1500 tokens) embeds
in 2.3 to 2.9 seconds. `curl http://<lan-ip>:1234/v1/models` from the host
itself is refused, which confirms the loopback bind.

Metal offload: the model was loaded with `--gpu max`, and
`lms runtime ls` shows only `apple-metal` engine builds installed, so
inference executes on the M2 GPU through Metal. The server log does not
print a per-layer offload line, and a gpu=off versus gpu=max latency A/B
on this small encoder was within noise, so treat the engine identity, not
a benchmark, as the offload evidence.

## Model management

The model was downloaded once with:

```shell
lms get "https://huggingface.co/gpustack/bge-m3-GGUF" --gguf -y
```

which selected the Q8_0 variant for this hardware. Keep this host to the
one embedding model; LM Studio also bundles
`text-embedding-nomic-embed-text-v1.5` under
`~/.lmstudio/.internal/bundled-models`, which is part of the app install
and not an extra download.

## Persistence and headless quirks

The API lives inside the LM Studio.app process. `lms server start` toggles
the app's embedded HTTP listener; quitting the app stops it, and nothing
restarts it after a reboot. `lms` has no launchd or login-item integration
(`lms server` only has start, stop, status), so persistence is manual:
after a login or app relaunch, rerun the two commands under "Running". Do
not hand-write a LaunchAgent for this; if boot persistence becomes a
requirement, use the app's own background-service setting
(`enableLocalService` in `~/.lmstudio/settings.json`, currently false)
from the LM Studio GUI.

Operational quirks learned while bringing this up:

- A stale `~/.lmstudio/bin/lms` (v0.0.41) fails every app call with
  "Invalid passkey for lms CLI client". `lms bootstrap` re-points the shim
  at the app's current CLI and fixes it. Run bootstrap after any LM Studio
  upgrade.
- `lms get <name> -y` searches staff picks only and reported "No staff
  picks found" for bge-m3. Pass the full Hugging Face URL instead.
- `lms load --exact` wants the full model path
  (`gpustack/bge-m3-GGUF/bge-m3-Q8_0.gguf`), not the display identifier.
- Just-in-time loading is active: a request naming an unloaded model loads
  it on demand, and JIT-loaded models unload after one idle hour
  (`jitModelTTL` in settings). An explicit `lms load` has no TTL, which is
  why the load step above matters for a warm endpoint.
- Over SSH, `lms` progress spinners flood captured output; add `--quiet`
  or pipe through `tail` in scripts.
- `lms log stream` and `osascript` over SSH produced nothing useful
  (the stream stayed empty, and GUI automation hangs without a permission
  grant). Use the files under `~/.lmstudio/server-logs/` instead.
