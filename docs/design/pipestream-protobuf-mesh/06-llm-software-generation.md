# LLM software-generation processor

## Objective

Use LLM workers to create and revise software integrations while protobuf
contracts, deterministic tools, policy, and acceptance evidence remain the
authority.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md). Reuse the
structured-generation coordinator, inference catalog, credential resolver,
delegation lifecycle, recipes, artifacts, replay, and generated gRPC clients.
It may initially use the current delegation stream while the generic mesh node
stream is developed.

## Ownership

Define an LLM processor profile, not a separate task transport. Work input and
results are application-specific protobuf messages carried as mesh entities.
The profile binds annotations and descriptor information to prompt assembly,
provider selection, structured output, workspace execution, and evidence
review.

The existing delegation lifecycle supplies admission, offers, leases,
heartbeats, progress, checkpoints, cancellation, completion candidates,
revision requests, and final acceptance. A worker can be another ProtoMolt
node, a Codex or Cursor bridge, or an in-process scripted provider.

## Contract-driven generation

The prompt packet is derived from:

- input and expected result descriptors and fingerprints;
- message and field LLM annotations;
- validation, sensitivity, projection, and PII policy;
- selected service methods and generated client bundle;
- immutable acceptance checks;
- allowed source, tool, network, and workspace capabilities; and
- prior checkpoint and revision feedback.

Instructions and payload data remain separate typed fields. A worker cannot
remove required checks, change the target schema, broaden its own authority, or
declare its candidate accepted.

## Integration workflow

1. Consume a validated service advertisement and immutable descriptors.
2. Generate and compile the requested client language.
3. Create a typed integration specification from method policy and annotations.
4. Lease the implementation task to an eligible LLM worker.
5. Store resumable, content-addressed checkpoints.
6. Run formatter, compiler, tests, static checks, and contract validation with
   deterministic processors.
7. Return precise revision findings when checks fail.
8. Accept only a candidate with passed evidence for every required check.
9. Emit a typed result containing commit, source artifact, dependency
   fingerprints, and evidence references.

## Safety and privacy

Apply projection, sensitivity masking, and the OpenNLP PII processor before
prompt construction and again before any remote provider call. Provider
credentials remain transport-owned. Store prompt and schema fingerprints by
default, not raw prompts or retry feedback. Artifact access is scope-limited
and audited.

## Tests

Use an in-process scripted worker, fake repository, and generated service. Cover
first-pass success, compile failure and revision, incomplete evidence,
checkpoint resume, lease expiry and reassignment, cancellation race,
descriptor drift, forbidden capability, PII redaction, provider failure,
idempotent frames, and accepted typed output.

## Acceptance criteria

- A reflected custom service can produce a compiled integration artifact
  without handwritten client code.
- The worker receives only projected, policy-approved data.
- Failed or missing deterministic checks prevent acceptance.
- Another worker can resume from a content-addressed checkpoint.
- The full decision and evidence record replays offline.

## Exclusions

Do not make prose the wire contract, allow the model to approve itself, persist
unmasked prompts by default, or couple the generic mesh runtime to one vendor.
