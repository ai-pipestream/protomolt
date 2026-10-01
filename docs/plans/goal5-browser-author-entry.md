# Configured workflow authoring entry

Status: reviewed contract-only slice. Native generated/dynamic fixtures and schema
projection checks pass locally. No handler or new operation is available yet.

Coordinator implementation follow-up: `offerOnce` and the authoring binding
function are implemented and locally tested. No entry action or host mount exists
yet. The coordinator checks the final rendered offer binding before publication,
and uses the original transcript offer for replay. Existing generic offer behavior
is preserved.

Local tests cover concurrent identical starts, changed worker/binding, generic
UUID collision, cancellation, lease expiry, later reassignment, serialized
transcript restoration without a connected worker, and a lost save acknowledgement
after commit. The last case proves fail-closed behavior until restoration and one
recovered offer afterward. The delegation suite and binding tests pass. This is
coordinator-level evidence, not a deployed browser or process-kill qualification.

## Existing operations and missing behavior

The task console already lists workers, offers generic tasks, displays durable
progress and renders accepted-workflow launch. Its first-offer form can attach a
DeliverableContract but cannot supply the policy context used by authoring. The
installed author sample constructs that TaskSpec itself, including the complete
WorkflowAuthoringDeliverable descriptor closure, pinned policy reference and all
WorkflowAuthoringReviewer.REQUIRED_CHECKS.

Reuse those TaskSpec, DeliverableContract, ArtifactReference, TaskOffer and worker
list models. Keep the current coordinator, transcript and independent review.
The current offer operation permits a new attempt after a terminal attempt. It
is unsuitable for replaying a first-start request after a lost response.

## Proposed protocol

Add an optional WorkflowAuthoringEntryService with two operations:

- GetWorkflowAuthoringTemplate returns the configured TaskSpec, its deterministic
  SHA-256 identity and a fixed lease duration. The spec includes the result
  descriptor closure, default objective, policy context and required checks.
  This returns one configured template, not a general template registry.
- StartWorkflowAuthoring takes a caller-persisted task UUID, selected worker ID,
  template SHA-256 and objective. The server builds the offer from the trusted
  template, changing only the objective. Return the exact normalized request and
  original TaskOffer, including its original lease and attempt one.

The template identity covers the deterministic template spec and lease duration.
The policy reference is already inside the spec. Changing descriptors, policy,
checks, defaults or lease changes this identity. A successful template response
must validate the complete descriptor closure and its named result type.

The objective is task guidance, not authority to alter permitted calls, required
checks, contract or policy. No request accepts arbitrary replacement TaskSpec,
policy, result descriptor or credentials. Existing worker metadata is descriptive;
it does not grant authority or prove the worker can complete an authoring task.

## Validation and state obligations

Annotate required fields, UUID, worker slug, SHA-256, objective length and lease
bounds. Validate requests and successful responses with the native runtime and
reject unknown fields and unsupported rules. Serialized messages are bounded.
Response cross-field rules bind task identity, original attempt and objective;
handler checks bind the full rendered spec, selected worker and durable record.
JSON Schema/OpenAPI represent scalar and required-field rules; deterministic
hashes, ledger custody and atomic replay remain runtime obligations.

Requires worker-coordinate, matching the existing offer authority. Browser routes
use the authenticated session caller without an operator token or substituted
principal. The author-only credential cannot start or assign tasks. Expose fixed
same-origin routes only when the authoring host and secured console are configured.

Under the coordinator mutation lock, a new task UUID creates exactly one offer.
A matching replay returns the original committed first offer, even after lease
expiry, completion, cancellation, worker disconnection or a later reassignment.
It does not renew a lease or create another attempt. A changed worker, objective,
template or other start intent under that UUID conflicts. An unrelated existing
task also conflicts. Check committed replay before current worker availability or
current template selection, so recovery can return the original historical offer.

Persist enough start identity alongside the first offer to distinguish this entry
from a generic offer and to survive coordinator restart. An in-memory request map
or a read-then-offer adapter is insufficient. Extend the existing durable protocol
with the smallest admission binding needed; do not create another task lifecycle.
The reviewed binding shape is recorded below; handler implementation is pending.

Invalid shape is INVALID_ARGUMENT; missing permission is PERMISSION_DENIED;
changed/reused identity is ALREADY_EXISTS; stale template or unavailable worker
admission is FAILED_PRECONDITION; transport failures are UNAVAILABLE or
DEADLINE_EXCEEDED; corrupt committed evidence is DATA_LOSS. Cancellation or a lost
reply does not undo a committed offer. Retry the exact saved request. Start does
not approve a candidate, execute fixtures or launch a workflow.

## Acceptance backlog

1. Review durable first-start binding and atomic coordinator behavior. Define and
   compile the entry contracts with complete imports, native valid/invalid fixtures,
   lint, compatibility and schema projection coverage before handler work.
2. Implement template construction and start-once admission. Test concurrent equal
   starts, changed starts, generic UUID collision, expiry, terminal and reassigned
   tasks, disconnected worker replay, template rotation and coordinator restart.
3. Add fixed session routes and a console form using the existing worker list.
   Persist the complete start intent before sending. Display the configured result
   type and policy identity. Reuse task progress, review retry and accepted launch.
4. Installed-process plus rendered-browser proof from task creation through author
   execution, independent acceptance, launch and completion. Include lost start
   response, review outage/retry and invalid result refusal.

This slice does not replace the remaining Goal 5 worker-kill proof, Kafka example,
image-only Compose starter or anonymous/native-platform release qualification.

## Reviewed durable binding

Add optional TaskOffer.start_binding_sha256 at field 6. Empty remains valid for
legacy and generic offers. For this entry it is required. Compute SHA-256 over a
UTF-8 domain prefix `protomolt.workflow-authoring-start.v1` followed by three
length-prefixed deterministic protobuf byte strings: StartWorkflowAuthoringRequest,
rendered TaskSpec and google.protobuf.Duration lease. Each length is an unsigned
32-bit big-endian byte count. Exclude the clock-derived expiration timestamp.
Template identity uses the rendered base spec and configured lease, before the
request objective replaces its default. Contract fixtures must distinguish hash
shape from handler-computed equality.

On replay, select the first offer from the trusted transcript, check task ID,
worker ID, attempt one and objective, and recompute this digest using that original
offer. Reject an absent or different binding. Later reassignment cannot change the
original admission. Persist the binding in the same append as the original offer;
no separate ledger transaction is needed. If durable append has an ambiguous
outcome, retain the coordinator's existing fail-closed behavior until restoration.

## Additional starter gap

The current sample AuthoringWorker accepts a task UUID before registration. A
browser-generated task therefore cannot be picked up by an idle packaged worker.
The starter also needs bounded assignment discovery restricted to Caller.name().
Reuse an existing suitable worker feed if available; otherwise define a separate
self-owned assignment read before changing the worker. Do not substitute a hardcoded
shared task UUID or grant global coordinator visibility to an author credential.

Template lookup must also preflight the rendered offer against the existing
1 MiB delegation frame limit, using the maximum supported identity and framing
widths. A response that fits the entry's 4 MiB transport cap can still be too large
to offer; reject that configuration instead of displaying an unusable template.
The registered worker list remains advisory. New admission rechecks the selected
worker under the mutation lock; replay requires no active worker session.
