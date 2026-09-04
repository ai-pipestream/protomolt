# Compiled directed flows

`protomolt-mesh-runtime` executes directed protobuf message graphs in process.
An authored `FlowDefinition` names exact input, node, edge, and output schemas
with `SchemaReference`: protobuf full name plus the SHA-256 fingerprint of the
canonical descriptor closure. Compilation resolves every reference, compares
each node with its registered `ProcessorContract`, type-checks edge predicates,
compiles descriptor-option projections, rejects cycles and unreachable nodes,
and fingerprints the definition together with the selected contracts.

Nothing in this path converts a payload, plan, event, or persisted record to
JSON. Processor inputs are parsed from the `Any` bytes under their resolved
descriptor. Outputs return as `TypedPayload`, are checked against the exact
declared output identities, and become digest-correct child `EntityEnvelope`s.

```java
DescriptorRegistry descriptors = DescriptorRegistry.create(false);
descriptors.registerFile(ApplicationProto.getDescriptor());

ProcessorRegistry processors = new ProcessorRegistry(descriptors);
processors.register(applicationProcessor);

CompiledDirectedFlow flow =
    new FlowCompiler(descriptors, processors).compile(definition);

EntityEnvelope input = EntityEnvelopes.root(
    entityId, scopeId, request, createdAt, deadline,
    CompletionPolicy.COMPLETION_POLICY_STRICT);
FlowExecutionResult result = new FlowRuntime(descriptors).execute(flow, input);
```

## Execution semantics

- Edges route one exact output schema. An optional CEL `when` expression is
  compiled against that descriptor and must return `bool`.
- An optional `project_to` is a normal `MessageProjection`; its target must be
  the receiving node's exact input schema.
- One message may route to several nodes, and one processor invocation may
  emit several ordered messages. This supplies branch, fan-out, and independent
  fan-in without an implicit zip or Cartesian operation.
- The node graph is acyclic. `max_messages` and the effective deadline bound
  dynamic amplification even when processors emit multiple messages.
- A stable run id produces stable child entity ids, so replay and at-least-once
  delivery retain the same product identity.
- Claim-check inputs require an explicit `PayloadResolver`. The default runtime
  refuses them instead of guessing an artifact source.

`FlowHistory` is the one ordered event stream for the run. It records accepted,
routed, produced, retained, processor, settlement, completion, and failure
events with their schema references and, where applicable, the exact envelope.
A failed `FlowExecutionException` carries the history accumulated through the
failure rather than losing the evidence.

Processor completion and downstream settlement are deliberately distinct. The
runtime retains each invocation's settlement handle and commits it in reverse
dependency order only after the directed run succeeds. The demand-driven remote
transport uses that same seam; local processors use a no-op settlement.
