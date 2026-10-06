# Historical revision restore

Status: contract design; no restore API is available. This is a bounded document
revision operation, separate from interrupted-execution recovery and optional JCR
version restoration.

## Operation and identities

Restoring revision r1 while r3 is current publishes a new revision r4. It does not
change r1, reset a revision counter, or overwrite r3. The caller supplies the
expected current destination revision; a concurrent r4 publication makes that
condition stale and refuses the restore. Existing multi-member publication remains
the atomic visibility boundary, so this design does not force one document per
transaction.

Retain `PublicationReuse` unchanged. Its source is a current pre-change dependency;
`DocumentPublicationCommand.validateAggregate`, `DocumentAdmissionAuthorization`,
and `DocumentReuseAdmission.requireBoundSources` enforce that meaning. An older
same-address revision cannot be represented by changing that dependency check.

The proposed additional `DocumentPublicationPart` content alternative is
`historical_reuse`, with an exact source address, revision UUID, full manifest
ordinal, slot, and immutable `PublicationObjectIdentity`. The ordinal counts empty
and deleted positions, matching historical reads. All selectors must agree with
the retained manifest and catalog. No arbitrary physical key adoption, latest
revision fallback, source revision-number guessing, or provider remapping is allowed.
The first version preserves the source slot at the destination and permits only
same-account selection. Count historical selectors against existing aggregate source,
part and byte bounds; reject noncanonical UUIDs and size overflow.
Field numbers and the final protobuf message name remain subject to contract review.
Do not expose this alternative until shared handlers can enforce it.

Historical selection is not inserted into the current-source revision map. Other
explicit current dependencies retain their existing checks. The canonical command
must bind the exact selector and identity into retries, assessment evidence and
receipts. The destination's expected current revision remains independently bound.
Changed source, destination condition, selected slot or physical identity under the
same operation identity is an idempotency conflict.

## Reuse the existing read foundation

`DocumentReadLedger.captureHistorical` already captures an exact native revision
under current READ authorization and acquires retention pins. Its `PinnedHistory`
issues a `Use` whose `DocumentHistoricalReadPlan` contains immutable manifest
ordinals and original physical bindings. `authorizeDelivery` checks current access
again. Build on these lifetimes rather than creating a parallel reader registry.

Keep the Use through actual provider completion, including cancellation, and through
any returned byte batch. Closing an outer handle does not establish provider
quiescence. Existing bounded admission, drain and failed SQL-release handling must
remain in effect. A selector or retained plan is evidence, never write authority.
A future owned selector helper is justified only where it removes duplicated
selection and lifetime checks in the actual restore path.

## Admission, schema and ownership

Recheck current source READ, destination WRITE, expected destination revision and
current admission policy. Historical ownership is provenance, not a present grant.
The initial bounded implementation should update an existing destination; absent
creation remains dependent on its separately reviewed authorization policy.

For typed occurrences, use the historical bindings and exact complete retained
descriptor closure. Preserve unresolved/opaque classifications as such; an opaque
revision may have no descriptor closure. Never replace definition A with the
current registry definition B merely because their type URLs match. Registry
absence must not prevent reading retained A. Restoring opaque bytes under a current
policy requiring a resolved type must fail explicitly unless a separately specified,
identity-bound resolution operation supplies the missing definition.
`DocumentHistoricalSchemaRows.capture` currently refuses opaque revisions. An
explicit current-policy-allowed raw path is therefore required; do not catch that
refusal and silently report typed validation success.

`DocumentHistoricalSchemas.check` proves historical integrity using the recorded
policy. Its result is not a current admission verdict. Run current admission against
the selected bytes and exact retained definitions, and bind the new assessment to
the restore command, current policy, attempt and destination condition. Refuse an
incompatible policy; do not mutate historical evidence to make it pass.

Preserve original part provenance while recording the new revision's actor,
operation and derivation from the exact historical revision. A restore must not copy
historical ACLs over current ownership as a side effect. Metadata selection needs
an explicit reviewed policy before adding any metadata restoration option.

## Retention and commit

Physical read pins protect selected bytes during preparation. A restore must also
retain its exact schema closure through pending assessment and publication. Current
V48/V55 guards conservatively prevent schema mutation; that is not a pruning
protocol. Before relaxing them, one liveness decision must cover committed revisions,
pending restores/assessments, active reads, recovery and optional future graph
references.

Stage verification outside SQL locks, then atomically check current authorization,
destination conditions, original physical identities and retention witnesses while
publishing the new references and receipt. Reuse existing deterministic lock order;
no provider or registry I/O under publication locks. Bind the source revision UUID,
full manifest ordinal/slot, object ID, complete provider identity and DOCUMENT_HISTORY
reference in that transaction; hold the Use through its completion. A rolled-back
transaction leaves no new visible revision or success receipt. A lost commit
acknowledgment requires exact durable outcome reconciliation before retry.
Release pins only after work drains and either committed
references protect the objects or explicit failure cleanup owns them. Exact retries
must reconcile durable outcomes before repeating work.

## Implementation sequence and acceptance

1. Compile and validate the reviewed historical selector contract with complete
   imports. Exercise required alternatives, UUID/ordinal bounds, slot and identity
   shape through the real runtime validator. Handler checks establish membership,
   account authorization and source retention; annotations cannot establish these.
2. Extend canonical command analysis, source authorization, capture, assessment
   slots/evidence and commit reference checks together. Fail closed on unsupported
   alternatives. Audit all `hasReuse` and implicit upload-or-reuse branches, including
   schema manifests, digest/size calculations, journal codecs and replay.
3. Integrate current admission with exact retained schemas. Preserve byte reuse and
   original backend identity. No restore wrapper may grant process authority.
4. Run real SQL/provider cases locally and through shared transport: r1 selected
   while r3 is current; stale destination; wrong revision/ordinal/slot/object;
   cross-account denial; READ/WRITE revoked at capture and commit; policy changed;
   registry absent or containing B while retained A exists; opaque policy refusal;
   exact retry and changed-selector conflict; failed finalization; cancellation
   during provider reads; held-read cleanup race; shared bytes without reupload.
5. Qualify pruning and backup separately before enabling deletion. Restore matching
   SQL metadata, retained schemas and exact provider content into an isolated host.
   SQL dump success alone does not establish a recoverable repository.

## JCR boundary

The existing [JCR assessment](repository-jcr-compatibility.md) requires reusable
multi-object atomic commits, stable identity and reference retention. This document
preserves those extension points. It does not define frozen graphs, child identity
collision behavior, checked-in restrictions, JCR sessions/workspaces, or strong
reference restoration. Those belong to the optional content-repository extension;
base storage gains no JCR dependency or compliance claim.

## Contract staging checkpoint

`PublicationHistoricalReuse` and the `historical_reuse` content arm (tag 5) now
exist. Real runtime validation covers generated and dynamic messages; JSON Schema
exposes ordinal bounds and required message fields, while CEL remains runtime-only.
The canonical command deliberately refuses this arm until shared execution handles
it. Existing v1 golden bytes/hash fixtures pass unchanged; older consumers will
reject the new field. Before activation, review command version and consumer
capability negotiation rather than assuming additive wire compatibility establishes
executable compatibility. [Evidence](../evidence/repository/2026-10-05-historical-contract/README.md).
