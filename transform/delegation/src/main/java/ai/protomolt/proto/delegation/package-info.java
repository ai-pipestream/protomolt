/**
 * Agent delegation contract, coordinator runtime, worker bridge, and lifecycle checker.
 *
 * <p>The contract ({@code delegation.proto}, package
 * {@link ai.protomolt.proto.delegation.v1}) is the engine-neutral boundary between a
 * coordinator and an LLM-backed worker process: one bidirectional gRPC stream per
 * worker, opened with a capability-advertising hello and gated by coordinator
 * admission. The LLM never owns the socket; a runner maintains the stream and invokes
 * the provider, so provider identity is hello metadata, not a protocol peer. Tasks move
 * through an explicit lifecycle: offer and accept grant one attempt's lease, heartbeats
 * and renewals keep it, expiry or cancellation ends it, and completion is a review
 * flow: the worker submits an evidence-carrying candidate revision, and the
 * coordinator accepts it or requests a revision. Every required acceptance check must
 * be proven with a passing verdict, and every candidate must reference at least one
 * commit or artifact; a worker saying "done" is structurally insufficient.</p>
 *
 * <p>{@link DelegationValidation} mirrors the contract's validate.v1 annotations for
 * fail-fast structural checks. {@link DelegationReducer} is the offline lifecycle
 * checker: a pure, in-process reduction of a recorded {@link
 * ai.protomolt.proto.delegation.v1.Transcript} that enforces the state machine,
 * idempotency (identical redelivery replays silently, a changed payload under a known
 * frame id is a conflicting duplicate), per-(lane, task, attempt) sequencing, lease
 * and revision staleness, monotonic progress and checkpoints, evidence completeness,
 * and terminal immutability, reporting precise findings and never repairing input.</p>
 *
 * <p>{@link InProcessDelegationCoordinator} implements the bidi service and exposes a
 * cursor-based blocking event wait for embedded and MCP callers. {@link
 * DelegationWorker} keeps the stream and provider execution separate through {@link
 * WorkerRunner}; worker invocations run on virtual threads. {@link CandidateReviewer}
 * controls acceptance and revision, while {@link ScriptedWorkerRunner} supplies
 * deterministic provider-free scenarios.</p>
 */
package ai.protomolt.proto.delegation;
