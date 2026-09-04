# Cluster-backed processor directory and worker control

Status: implemented and locally verified on `descriptor-flow-runtime`; not yet
claimed as merged or released. This increment follows the durable directed-flow
lifecycle.

## Outcome

A workflow coordinator will resolve remote processors from one live, fenced
cluster directory instead of from a manually assembled registry. Workers will
advertise exact processor contracts, renew liveness, publish capacity, drain,
receive cancellation, and reconnect under a new incarnation without changing
the meaning of a processor invocation.

The local and remote paths must still produce the same protobuf outputs,
history events, cancellation outcome, and settlement effect. The directory
chooses placement. It does not own application schema semantics or execute a
second workflow model.

## Existing code to extend

- `mesh/cluster/.../cluster.proto` already defines fenced node and processor
  advertisements, presence states, TTLs, capacity snapshots, locality fields,
  and deterministic snapshots.
- `ClusterDirectory` is the in-memory reducer. `PersistentClusterDirectory`
  persists membership mutations and deliberately treats heartbeat presence as
  renewable soft state.
- `cluster_directory_service.proto` declares unary registration, heartbeat,
  capacity, snapshot, and sweep messages, but `mesh/cluster/build.gradle` still
  treats the module as messages-only. There is no mounted gRPC implementation
  or watch stream.
- `ProcessorRegistry` and `ProcessorInvoker` are the execution seam.
  `DemandProcessorCoordinator` and `DemandProcessorWorker` provide the current
  credit-driven stream, lease-fenced delivery, and exact output validation.
- `FlowLifecycleGrpcService` and `DurableFlowCoordinator` now provide durable
  run ownership, cancellation, and restart checkpoints.
- `apps/serve` mounts the cluster action catalog, but not the directory gRPC
  service or the durable flow lifecycle.

## Non-negotiable invariants

1. Processor compatibility is exact descriptor identity plus the complete
   processor contract. A type name or capability string alone is not enough.
2. A node incarnation and processor lease epoch are fences. Frames from an old
   incarnation cannot revive presence, consume demand, complete work, or alter
   capacity.
3. Only `ACTIVE` and ready processors receive new claims. `SUSPECT`,
   `DRAINING`, and `GONE` processors may not acquire work.
4. Demand is no greater than observed available capacity and the worker's
   explicit credit. Zero capacity means zero dispatch.
5. A directory snapshot and its ordered tail form one resumable view. If the
   requested cursor has been compacted, the server returns a named resync
   response rather than silently skipping changes.
6. Directory loss changes placement availability, not run identity. Accepted
   work stays in the durable channel and run checkpoint until a compatible
   worker returns or the run reaches its declared deadline or cancellation.
7. All internal contracts, persistence, worker frames, conformance fixtures,
   and projections are protobuf binary. JSON is only a REST or human-facing
   rendering.

## Contract changes

### Shared processor contract

Move `ProcessorContract` from the runtime-only flow file into a mesh contract
proto owned by `mesh/proto` or `mesh/contracts`. Keep its protobuf package and
field numbers stable during the move so generated users do not see a wire
change. Both the cluster advertisement and runtime worker protocol must import
that one definition.

Add a deterministic `contract_fingerprint` over processor id, exact input and
output schema references, output bound, and any execution guarantees added in
this increment. Registration and worker hello carry that fingerprint and the
full contract. The directory refuses a same-named processor whose bytes do not
match the active lease.

### Directory service

Extend `cluster_directory_service.proto` and enable gRPC generation in
`mesh/cluster/build.gradle`.

Add:

- `WatchDirectory(WatchDirectoryRequest) returns (stream DirectoryFrame)`;
- a cursor composed of directory generation and event sequence;
- an initial exact `ClusterSnapshot` frame;
- ordered `ClusterEvent` frames after that snapshot;
- an explicit `RESYNC_REQUIRED` frame containing the oldest available cursor;
- optional bounded filters for processor id, exact input schema, capability,
  and node id, while retaining the snapshot identity of the unfiltered state;
- an administrator-authored readiness overlay separate from the worker's
  signed advertisement and observed liveness.

Implement `ClusterDirectoryGrpcService` over `PersistentClusterDirectory`.
Mutations validate descriptor annotations, return the existing
`DirectoryCommit`, and map stale fences to `ABORTED`, missing identities to
`NOT_FOUND`, invalid contracts to `INVALID_ARGUMENT`, and unavailable durable
storage to `UNAVAILABLE` or `INTERNAL` as appropriate.

The persistent repository must expose a bounded ordered event tail. Compaction
records the first retained cursor and a new directory generation. A watcher
that asks before that point receives `RESYNC_REQUIRED`; it never gets a partial
tail presented as complete.

### Worker control stream

Extend `processor_channel.proto` without changing existing field numbers:

- `WorkerHello` adds node id, node incarnation, processor lease epochs, and
  full exact contracts or immutable contract references;
- worker frames add heartbeat, capacity, drain progress, and cancellation
  acknowledgement;
- coordinator frames add claim cancellation, drain request, and directory
  revision acknowledgement;
- every post-hello frame carries the admitted session id and incarnation fence;
- reconnect may resume only claims that remain owned by the same valid lease;
  all other claims return to the durable channel.

Worker cancellation is cooperative first and interrupting second. The worker
stops accepting new work, signals the processor, and reports a typed cancelled
outcome under the active lease. A completion that races after cancellation or
lease loss is refused by fence.

## Runtime components

Implement these narrow components instead of folding the directory into the
flow coordinator:

- `ProcessorDirectoryClient`: snapshot/watch/resync client with one atomic
  immutable local view;
- `DirectoryProcessorResolver`: resolves one `ProcessorContract` plus payload
  size and locality requirements to eligible instances;
- `DirectoryWorkerAdmission`: admits `WorkerHello` only when node incarnation,
  lease, endpoint, and exact contract match the directory;
- `WorkerCapacityController`: converts max in-flight, current in-flight,
  draining state, and local queue pressure into bounded demand;
- `WorkerSessionRegistry`: owns stream/session fences and routes run
  cancellation to the worker holding the claim;
- `DirectoryMaintenance`: sweeps expiry, reports lag, and drives resync without
  blocking read-only placement on durable writes.

`DemandProcessorCoordinator` should receive these collaborators. It must not
import application mappings, flow definitions, or REST models.

## Landing sequence

### Phase 1: one real directory service

Enable generated gRPC, implement every existing unary RPC, add in-process RPC
tests, and mount the service beside the existing action catalog. Prove that the
action and gRPC paths reduce to the same snapshot and commit identity.

### Phase 2: resumable watch

Add snapshot-plus-tail watching, cursor compaction, bounded buffering, slow
watcher eviction, and the named resync frame. Restart tests must reconnect from
the last cursor with no duplicate or omitted event. A compacted cursor must
never degrade to the current snapshot without saying so.

### Phase 3: exact worker admission

Promote the shared processor contract, bind worker hello to directory node and
lease fences, and replace `RemoteWorkerAdmission.allowAll()` in the product
composition. A worker may advertise only processors currently leased to its
node and must prove the exact contract fingerprint.

### Phase 4: capacity and health-driven dispatch

Feed eligible directory records and capacity into dispatch. Effective credit
is the minimum of worker-granted permits, processor free capacity, node free
capacity, and coordinator limit. Health loss stops new claims immediately.
Already claimed work retains its lease until cancellation, expiry, failure, or
completion.

### Phase 5: drain, cancellation, and reconnect

Add server-requested drain and per-delivery cancellation. Drain first removes
the worker from eligibility, then waits for in-flight claims, then closes the
stream. Reconnect under the same session is allowed only inside the declared
grace and matching incarnation; otherwise outstanding claims are released.

### Phase 6: supported composition

Add the directory and lifecycle services to the selected server role with
explicit storage paths, sweep intervals, TTLs, capacity ceilings, and
authorization scopes. Health reports must distinguish directory persistence,
watch freshness, worker readiness, and channel availability.

## Required tests

### Contract and reducer

- exact contract registration and fingerprint stability;
- same processor id with descriptor or output-bound drift;
- stale node epoch, processor lease epoch, sequence, capacity, and heartbeat;
- deterministic snapshot across event replay and compaction;
- old frames remain fenced after expiry and re-registration.

### RPC and watch

- happy path for every unary RPC;
- snapshot followed by ordered changes;
- reconnect from a cursor without gaps or duplicates;
- expired cursor returns `RESYNC_REQUIRED`;
- bounded slow watcher is disconnected without blocking mutation;
- action-catalog and gRPC mutations produce the same commit.

### Worker control

- worker registration, heartbeat, capacity increase/decrease, and drain;
- no claim at zero credit or zero effective capacity;
- health becomes suspect before connection loss and stops new work;
- cancellation before execution, during execution, and after the durable
  descendant-settlement boundary;
- stale worker completion and cancellation acknowledgement are refused;
- disconnect, reconnect within grace, reconnect after grace, and replacement
  by a newer incarnation;
- local, one-worker, replacement-worker, and multi-worker executions produce
  identical ordered outputs, history, and settlement.

### Fault and cost gates

- crash before and after directory persistence, view installation, claim,
  completion, cancellation, and drain acknowledgement;
- repository outage does not block current snapshot reads and cannot leak an
  unpersisted mutation;
- watch fan-out memory is bounded;
- measure registration latency, snapshot size, watch lag, dispatch latency,
  heartbeat CPU, and useful throughput versus worker count;
- fail the gate if directory lookup or capacity accounting adds an unbounded
  per-message scan.

## Done means

The feature is done when a restarted product process can restore the directory,
serve snapshot and watch RPCs, admit only exact healthy workers, dispatch within
real capacity, cancel and drain them under fences, and complete the existing
durable flow tests with byte-identical protobuf results. A manually injected
worker registry, `allowAll()` product admission, or polling-only directory is
not completion.
