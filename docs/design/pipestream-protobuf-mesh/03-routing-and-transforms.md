# Routing and transform engine

## Objective

Select an eligible processor for a typed entity and adapt data across each
boundary with registered mapping, projection, sensitivity, PII, and validation
policy.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md). Reuse
`TypedEdge`, scoped mappings, typed CEL selectors, `MessageProjection`,
`RuleChecker`, `PipelineChecker`, validation, and sensitivity masking.

## Ownership

Add immutable `RouteProfile`, `RouteRule`, `TransformPlan`, and
`RouteDecision` contracts plus a pure static compiler and selector. Execution
may call existing edge and pipeline components but must not duplicate them.

A route rule can match payload type and fingerprint, annotated routing keys,
required processor capability, data layer, trust domain, security posture,
environment, locality, capacity, cost, and a CEL predicate compiled against
the resolved descriptor and normalized context.

## Transform boundary

For every processor edge:

1. Resolve, unpack, and validate the source `Any`.
2. Resolve registered source bindings.
3. Apply statically checked text and CEL mappings.
4. Project to the consumer-visible request type.
5. Apply sensitivity and PII policy.
6. Validate the exact delivered request.
7. Invoke the processor.
8. Resolve and validate its result.
9. Apply the registered result mapping or projection.
10. Validate the resulting mesh entity.

The plan is canonicalized and fingerprinted. Normal traffic names a registered
plan. Only an authenticated authoring surface may submit an inline plan for
review and promotion.

## Policy rules

- A request cannot supply arbitrary CEL, mappings, endpoints, or class names.
- CEL compilation is descriptor-aware and occurs before execution.
- Selection is deterministic for identical catalog, policy, health, and input
  snapshots.
- Route evidence records matched rule, processor, schemas, transforms, policy
  digest, and rejected candidates without secrets or sensitive field values.
- Cardinality is explicit. `ONE` and `MANY` transitions use only declared
  unnest, fan-out, and collect operations.

## Tests

Use in-process processors and custom schemas. Cover deterministic precedence,
ambiguous ties, typed routing keys, CEL type errors, mapping and projection
failures, validation before invocation, result validation, sensitivity
masking, PII hook ordering, all four gRPC cardinalities, fan-out bounds, plan
fingerprint stability, and forbidden inline policy.

## Acceptance criteria

- The static checker rejects every invalid route and transform before a remote
  call.
- Successful decisions are replayable from recorded catalog and policy
  fingerprints.
- No prohibited field reaches a capturing processor.
- Existing recipe and pipeline semantics cross the mesh boundary unchanged.

## Exclusions

Do not add service discovery, durable scheduling, an LLM decision authority,
or a second mapping language.
