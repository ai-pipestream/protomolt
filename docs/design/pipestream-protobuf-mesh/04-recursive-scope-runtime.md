# Recursive scope runtime

## Objective

Execute bounded recursive scatter, gather, checkpoint barriers, and typed
rehydration for entity trees while preserving deterministic state transitions
and evidence.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md),
[routing](03-routing-and-transforms.md), and storage interfaces from
[persistence](08-persistence-and-recovery.md). Integrate security policy before
remote dispatch.

## Ownership

Add a pure `ScopeReducer`, `AssemblyManifest`, barrier evaluator, scheduler,
and rehydration coordinator. Reuse pipeline fan-out, collect, virtual-thread
execution, artifact references, and immutable evidence.

Each child records its stable parent and scope ids, depth, data layer, payload
schema, route decision, processor lease, checkpoint, terminal state, and result
reference. The manifest records expected children, result types, transform
fingerprints, completion policy, and rehydration profile.

## State and completion

Support strict, lenient, best-effort, and quorum completion. A child result
advances a barrier only after payload integrity, schema, transform, policy, and
result validation succeed. Cancellation and deadlines propagate according to
the scope profile. Late results cannot mutate a terminal scope.

Enforce limits for depth, children per entity, total entities per scope,
payload and artifact bytes, active streams, wall time, attempts, and processor
cost. A child may not weaken the inherited security posture.

## Execution model

Use virtual threads for independent blocking processor calls. Retain explicit
concurrency and admission limits so virtual threads do not create unbounded
remote work, memory use, or connection pressure. The state machine must remain
independent of the executor and transport.

## Tests

Use a fake clock, deterministic reducer, and scripted processors. Cover nested
scatter, zero and one child, every completion policy, mixed success, quorum
impossibility, cancellation races, deadline expiry, late and duplicate frames,
checkpoint resume, invalid child result, depth and amplification rejection,
rehydration mapping failures, and deterministic event replay.

## Acceptance criteria

- Reducer replay reconstructs the same scope state and manifest digest.
- A restarted coordinator resumes from the latest valid checkpoint.
- Bounded fan-out runs concurrently on virtual threads without exceeding its
  admission limit.
- Only validated children contribute to the rehydrated protobuf result.

## Exclusions

Do not bind the reducer to PostgreSQL, QUIC, one gRPC implementation, or an LLM
provider. Do not infer missing children or repair invalid transcripts.
