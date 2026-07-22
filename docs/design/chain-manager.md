# The chain manager (design)

Status: phases 1 and 2 implemented (`protomolt-chain`: the runner, the
verifier, and the `run-chain`/`check-chain` verbs — inline chains, serial
unary, fail-fast, nested deadlines, `when` gates, response validation;
named chains stored in the registry under `chains/` with `check-chain` as
the write gate, resolved by `run-chain`'s `chainName`). Terminal streaming
steps, retries, and compensation remain design.

## What it is

A **chain** is a configured, typed composition of gRPC calls: invoke a
method, reshape its response into the next request with mapping rules and
CEL, gate hops with predicates, validate against declared rules along the
way, and return one composed result. A chain turns a suite of services into
a single callable transaction — one endpoint in, one answer out.

The motivating shape is the OpenVINO tutorial's embedding flow: call the
tokenizer model, reshape its output tensors into the embedder's input,
call the embedder, project the vector out of the response. Today an agent
(or a person) drives those hops by hand through `grpc-invoke`. A chain is
that same composition written down once, type-checked, and callable by
name.

## What it is not

**The chain manager is not a pipeline product, and must not grow into
one.** It is a sidecar people bolt onto whatever pipeline they already run
— Kafka Connect, Airflow, their own services. Concretely, that rules out:

- **No persistence of in-flight state.** A chain execution lives inside one
  call. If the process dies, the caller retries. There is no journal, no
  resume, no exactly-once machinery.
- **No scheduling, no triggers, no queues.** Something else decides when a
  chain runs. We only decide what happens when it does.
- **No long-running orchestration.** A chain is bounded by a deadline like
  any other RPC. Workflows that outlive a request belong to workflow
  engines.
- **No fan-out/fan-in graph engine.** Steps run serially. (Reads naturally:
  a transaction is a sequence.) If serial composition ever genuinely
  needs branches, that is a signal to reconsider — not a v1 feature.

The test for any future addition: *does this make the chain a better
sidecar, or does it make it a worse pipeline?* Only the first kind goes in.

## The definition is a protobuf

Chains are defined by a `ChainDefinition` message
(`ai.pipestream.protomolt.chain.v1`), so — as everywhere else in ProtoMolt
— the canonical proto3 JSON form of the definition *is* the configuration
file, the REST body, the MCP tool input, and the registry-stored document.
Sketch (field names indicative):

```json
{
  "name": "embed-text",
  "description": "Tokenize then embed: text in, vector out",
  "schema": {"descriptorSetBase64": "..."},
  "inputType": "demo.embed.v1.EmbedRequest",
  "deadlineMs": 10000,
  "steps": [
    {
      "name": "tokenize",
      "target": "ovms:9000",
      "method": "inference.GRPCInferenceService/ModelInfer",
      "request": {
        "rules": ["model_name = 'tokenizer'"],
        "celRules": [
          {"selector": "input.text", "target": "raw_input_contents"}
        ]
      },
      "deadlineMs": 2000,
      "retry": {"attempts": 2, "on": ["UNAVAILABLE"]}
    },
    {
      "name": "embed",
      "target": "ovms:9000",
      "method": "inference.GRPCInferenceService/ModelInfer",
      "when": "steps.tokenize.outputs.size() > 0",
      "request": {
        "rules": ["model_name = 'embedder'"],
        "celRules": [
          {"selector": "steps.tokenize.raw_output_contents",
           "target": "raw_input_contents"}
        ]
      },
      "validate": true
    }
  ],
  "output": {
    "type": "demo.embed.v1.EmbedResponse",
    "celRules": [
      {"selector": "steps.embed.raw_output_contents[0]", "target": "vector"}
    ]
  }
}
```

The pieces are deliberately the existing pieces:

- **`request` / `output` mappings** are exactly the `map-message` surface —
  text rules plus CEL rules — no new rule syntax.
- **`when`** is a CEL predicate; a false gate skips the step (its variable
  is then absent, and later expressions guard with `has()`).
- **`schema`** is the standard `SchemaSource`: inline sources, a descriptor
  set, registry subjects, or reflection against the step targets.
- **`validate`** runs the declared validation rules on a step's response
  before the chain proceeds — a contract firewall between services.

### The variable scope is the join surface

Expressions and mapping sources see a chain-scoped context:

- `input` — the chain's input message;
- each completed step's response, bound directly under the step's name
  (step names are validated identifiers, so `tokenize.ids` reads exactly
  like any scoped source path — one dialect everywhere).

Every step's request mapping may read from *any* of them, not just the
previous hop. That makes joins ordinary: call two lookup services in
sequence, then build a request (or the output) from both responses plus
the original input. No special join node — composition over the variable
scope is the join.

## Type-checked before anything runs

Because every step's method is resolved from descriptors at registration
time, the whole chain is verified statically, in the spirit of
`check-compat`:

1. every `method` resolves in the schema and has the right shape (unary;
   see streaming below);
2. every mapping target path exists on that step's request type, and every
   source path/CEL expression type-checks against the variables in scope
   at that step (CEL environments are built per step from the actual
   descriptors);
3. the `output` mapping produces the declared output type;
4. `validate: true` steps have validation rules resolvable on their
   response types.

A chain that registers cannot fail on a type error at run time — only on
live-service behavior. `check-chain` exposes this verification as its own
verb so consoles and CI can lint chain definitions without executing them.

## Execution semantics

- **Serial, fail-fast.** Steps run in order; the first failure aborts the
  chain and surfaces the step name, gRPC status, and hop context in the
  error details.
- **Deadlines nest.** Each step has a deadline; the chain has an overall
  deadline; the effective per-call deadline is the minimum of the step's
  and what remains of the chain's.
- **Retries are per step and status-scoped**, defaulting to none. Only
  explicitly listed transient statuses retry, so non-idempotent calls are
  never silently repeated.
- **Compensation is opt-in and best-effort.** A step may declare a
  `compensate` block (target, method, request mapping over the same
  variable scope). When a later step fails, compensations for completed
  steps run in reverse order. This is saga-style cleanup, not a
  distributed transaction: a failed compensation is reported in the error
  details, never retried into oblivion. Chains without compensations are
  simply sequences that stop.
- **Streaming**: v1 steps are unary. A server-streaming method is allowed
  only as the *final* step, making the chain itself server-streaming (each
  streamed response passes through the output mapping). Client-streaming
  and bidi are out of scope, as they are for the connectors.

## Where chains live

Inline and named. An inline chain rides in the `run-chain` request itself —
good for agents composing ad hoc. A named chain is registered and versioned
in the Git-backed registry under its own subject (a `chains/` namespace),
which buys the whole registry story for free: history, diffs, review via
Git, and `check-chain` as the compatibility gate on writes. Runners resolve
a named chain by subject and version, so "what exactly ran" is always
answerable.

## Surfaces (all of them, for free)

The chain manager arrives as verbs in the action catalog — `run-chain`,
`check-chain` — which means every existing mount works immediately: typed
RPCs on `ProtoMoltService`, JSON/REST with OpenAPI and Swagger UI, and MCP
tools. The MCP surface is the quiet headline: an agent that today reflects
and invokes services one hop at a time can instead *author a chain, verify
it with `check-chain`, run it, and register it* — turning an exploration
session into a durable, reviewable artifact.

On the Kafka side, a `ChainTransform` SMT runs a chain per record (the
record as `input`, the output as the new value), and the sink can point at
a chain the same way it points at a method — the per-record
validate → enrich → reshape story without leaving the worker.

## Module layout and phasing

`transform/chain` → `protomolt-chain`, depending on `grpc-invoke`,
`mapper-cel`, `protobuf-validation`, and `descriptors`; the verbs land in
`actions` like every other verb.

1. **Phase 1 — the runner**: `ChainDefinition`, static verification,
   serial unary execution with deadlines and fail-fast errors; `run-chain`
   and `check-chain` verbs; inline chains only.
2. **Phase 2 — named chains**: registry storage under `chains/`,
   resolve-by-subject, `check-chain` as the write gate.
3. **Phase 3 — integration**: `ChainTransform` for Connect; terminal
   server-streaming steps; retries and compensation.

## Future direction: the contract as a language

The longer-range idea this design leaves room for: once services are
descriptors and composition is typed mapping, the proto contract itself
becomes the programmable surface — chains as expressions over descriptors
rather than hand-wired configs. A console (or an agent) that knows two
services' descriptors can propose the mapping between them; `check-chain`
is already the type checker such a surface needs. Nothing in v1 commits to
this, but nothing forecloses it either: the variable scope, the typed
mappings, and static verification are exactly the semantics a small
composition language would compile to.
