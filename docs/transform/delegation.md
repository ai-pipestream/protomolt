# Agent delegation

The delegation module provides a provider-neutral bidirectional gRPC contract,
an in-process coordinator, a worker adapter boundary, and deterministic
transcript validation.

The protobuf contract is
[`delegation.proto`](../../transform/delegation/src/main/proto/ai/pipestream/proto/delegation/v1/delegation.proto).

The LLM does not own the gRPC stream. A worker adapter keeps the stream open,
invokes the configured provider, turns provider activity into structured
frames, and applies coordinator decisions.

## Session handshake

Each worker opens one `AgentDelegationService.Delegate` stream and sends a
`WorkerHello` as its first frame. The hello declares:

- a stable worker identity;
- protocol version;
- provider and model metadata; and
- bounded capability names and descriptions.

The coordinator replies with an admission decision before sending work. One
stream represents one worker session.

## Task lifecycle

The coordinator offers a bounded `TaskSpec` with:

- an objective;
- allowed repository or workspace scope;
- constraints;
- required acceptance checks;
- context artifact references; and
- a lease duration and expiry.

The worker accepts or rejects the offer. An accepted attempt may send
heartbeats, monotonic progress, and resumable checkpoints. The coordinator may
renew or expire the lease. A later attempt can resume from a recorded
checkpoint.

A worker cannot mark its own task complete. It submits a completion candidate
with evidence for every required check and at least one commit or artifact
reference. The coordinator either accepts that revision or requests another
revision with structured feedback.

Blocked, failed, cancelled, rejected, expired, and accepted attempts are
terminal. Reassignment uses a new attempt and lease. Coordinator cancellation
wins a race with a later completion candidate.

## Ordering and idempotency

Every frame has a sender-generated `frame_id` and a sequence number scoped to
its lane, task, and attempt. An identical duplicate is idempotent. Reusing a
frame ID with different bytes is a conflict. Sequence gaps and rewinds are
invalid transcript findings.

This envelope supports at-least-once delivery without allowing duplicate or
reordered frames to advance task state.

## Transcript validation

`DelegationReducer` replays a `Transcript` without external calls or state
changes. It reports findings for:

- illegal lifecycle transitions;
- stale leases, attempts, workers, or revisions;
- missing acceptance evidence;
- conflicting duplicates and sequence gaps;
- checkpoint regression;
- completion after cancellation; and
- changes after a terminal state.

The reducer never repairs input. `DelegationValidation` handles structural
contract validation before lifecycle replay.

## Durable transcripts

`TranscriptRepository` is the coordinator's persistence boundary.
`InMemoryTranscriptRepository` supports tests and process-local deployments.
`RepositoryServiceTranscriptRepository` stores an encrypted snapshot through
the repository service `PutBlob` and `GetBlob` RPCs.

The repository-service adapter:

- validates and reduces the complete transcript before each write;
- encrypts it locally with AES-256-GCM and a unique 12-byte nonce;
- binds the drive, object key, and key reference as authenticated data;
- verifies the repository service's returned byte count, object coordinates,
  and SHA-256 digest before accepting a write;
- verifies the encryption tag, plaintext digest, entry count, structural
  validation, and lifecycle reduction on read; and
- restores the coordinator's task projection, event cursors, duplicate-frame
  state, and coordinator sequence counters during construction.

The repository service and its S3-compatible backing store receive only the
encrypted envelope. If repository service uses its Redis cache, the cached
value is the same ciphertext. ProtoMolt does not depend on an S3 or Redis
client for transcript persistence.

`EnvRepositoryStateKeyResolver` accepts references such as
`env:PROTOMOLT_TRANSCRIPT_KEY`. The environment variable contains a
base64-encoded 32-byte key. The reference is persisted in the envelope; the
key is resolved immediately before encryption or decryption and is never
stored or included in an exception.

```java
TranscriptRepository transcripts = new RepositoryServiceTranscriptRepository(
        repositoryServiceStub,
        "protomolt",
        "delegation/coordinator-a/transcript.pb.enc",
        "env:PROTOMOLT_TRANSCRIPT_KEY",
        new EnvRepositoryStateKeyResolver());

InProcessDelegationCoordinator coordinator =
        new InProcessDelegationCoordinator(
                admissionPolicy, candidateReviewer, clock, transcripts);
```

Every accepted frame is durable before it enters the event feed or reaches a
worker. A failed write does not consume a coordinator sequence, mutate task
state, or expose a memory-only event. On restart, elapsed active leases expire
when their worker reconnects; unexpired leases resume their expiry timer.

The adapter writes a complete snapshot and limits plaintext to 8 MiB because
the repository blob RPC is unary. Assign one logical coordinator as the sole
writer for an object key. Multi-coordinator writes require a fenced or
compare-and-set repository contract and must not share an object key.

## Security boundary

Task frames carry bounded instructions, evidence, and artifact references.
They do not grant filesystem, network, provider, or repository authority.
Worker adapters must enforce the declared scope and obtain credentials from
their host environment.

Transcript fields have validation and sensitivity metadata. They have no
search-index annotations because transcripts are replay records, not search
documents.

## Runtime integration

`InProcessDelegationCoordinator` implements the generated gRPC service. It
handles admission, offers, leases, progress, checkpoints, cancellation, and
candidate review. Each accepted frame is appended to a replayable transcript
and checked against `DelegationReducer`.

`CandidateReviewer` runs on a virtual thread and can accept a candidate,
request a revision, or leave it pending for external review. Reviewers can
inspect referenced commits and artifacts before accepting reported evidence.

`waitForEvent` blocks until a task event appears after a caller-owned cursor or
the timeout expires. It is suitable for an MCP long-poll tool and is safe to
call from a virtual thread.

`DelegationWorker` owns the gRPC stream and invokes a `WorkerRunner` on virtual
threads. The runner receives a task offer, optional revision feedback, and
callbacks for progress, checkpoints, heartbeats, and cancellation. A Kimi,
Codex, Cursor, or local-model adapter only needs to implement `WorkerRunner`.

`ScriptedWorkerRunner` provides deterministic in-process task and revision
scenarios without a provider, container, or GPU.

Deployments choose their admission policy, candidate reviewer, transcript
repository, and provider adapter. The coordinator does not grant repository,
artifact, or provider credentials to a worker.
