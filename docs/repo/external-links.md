# Versioned external-system bindings and ExternalLink publication receipts

ProtoMolt documents often produce derived artifacts in external search indexes,
graph stores, or downstream APIs. An endpoint URL, a local HTTP 200, or a
stateless NLP/embedding response does not prove which external system
configuration ran, which repository source revision was projected, or whether
external publication completed.

Package `ai.protomolt.proto.repo.accession.external` and wire contract
[external_publication.proto](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/external/v1/external_publication.proto)
record versioned external-system bindings, durable publication and deletion
operations, verified provider receipts, and `ExternalLink` projections on top of
the canonical parts-based [DocumentService](publication.md) and config lane.

## Ownership and non-overlapping concepts

- **ProtoMolt repository (`ExternalBindingStore`, `ExternalPublicationCoordinator`)**
  owns versioned binding definitions, live service-profile and credential
  indirection, exact `(NodeAddress, source_revision)` association, operation
  intent fingerprinting, attempt/reconciliation state, monotonic revision and
  deletion fences, and retention enforcement.
- **Stateless processors (such as gRParse and Turboembed)** supply deterministic
  input digests, output digests, model/bundle identifiers, and stateless
  processing receipt digests (`NlpProcessingReceiptSummary`). They remain
  stateless with respect to external publication: a valid embedding receipt
  never implies that an external index accepted or published a projection.
- **External providers (such as managed Rust search or external graph services)**
  own their internal history, catalog epochs, and serving state, and return
  typed provider receipts verified by `ExternalProviderVerifier` before
  ProtoMolt confirms publication or deletion.

`ExternalLink` is distinct from three other repository concepts:

| Concept | Scope | Authority |
|---|---|---|
| `ExternalLink` | Exact repository `(NodeAddress, source_revision)` to an external derived object under a specific `(binding_id, binding_revision)` | Confirmed only from verified provider evidence in `external-publications:<account_id>` |
| `DocumentLink` | Repository-to-repository call-number edge inside `PipeDoc` | Stored in source document parts |
| `Drive` / `Stack` placement | Physical write-once blob and part storage coordinates | Managed by repository catalog and blob stacks |
| `display_url` on `ExternalLink` | Optional human navigation hint (`http://` or `https://` only) | Display metadata only; never canonical object identity or authorization |

## Versioned `ExternalSystemBinding` and secret-free history

Each `ExternalSystemBinding` (`ai.protomolt.proto.repo.external.v1.ExternalSystemBinding`)
captures:

- `account_id` and stable `binding_id`
- `provider` (`MANAGED_SEARCH`, `EXTERNAL_SEARCH`, `EXTERNAL_GRAPH`, `CUSTOM`)
- `service_profile_id` and `service_endpoint_id`, validated against the live
  `ServiceProfile` via `ServiceProfileValidation`
- `dataset`, `namespace`, and `contract_version`
- monotonic `binding_revision` (`>= 1`)
- `lifecycle` (`ACTIVE` or `RETIRED`) and `retention_policy`
  (`RETAIN_HISTORY` or `PURGE_WITH_SOURCE`)
- non-secret `ExternalEndpointSummary` (`host`, `port`, `tls_enabled`) derived
  from the resolved `ServiceProfile` endpoint at binding save time

### Zero-secret persistence rule

Bindings, binding snapshots, labels, display URLs, and publication ledgers
persist **no** secrets, bearer tokens, URL userinfo, credential query
parameters, or private key material:

1. Live credentials resolve exclusively at call time via
   `ProfileCredentialResolver` against the referenced `ServiceProfile`.
2. `ExternalPublicationCoordinates` rejects userinfo (`@`), non-HTTP(S) schemes,
   and secret-bearing query parameter names or values (including percent-encoded
   variants such as `%74%6F%6B%65%6E`, `access_token`, `api_key`, `sig`,
   `X-Amz-Signature`, `Bearer `, and PEM headers) without regular expressions.
3. Console rendering (`safeExternalLinkHref` in
   [linksCore.ts](../../apps/console/src/repository/linksCore.ts)) re-validates
   `display_url` before emitting an anchor `href`, rendering unsafe values as
   non-navigable text.
4. Keys the caller supplies, operation IDs and purge IDs, are checked the same
   way and refused with `INVALID_ARGUMENT`.
5. An operation's intent is checked the same way before it is first saved:
   any of its string fields, such as `external_object_id` or
   `processing.mapping_id`, that reads as a credential makes the request
   `INVALID_ARGUMENT`.
6. Provider evidence is stored on the ledger, so `ExternalProviderVerifier`
   refuses evidence (`EVIDENCE_CARRIES_CREDENTIAL`) when any string field of it,
   or of the `WorkRecord` manifest inside a signed work record, at any depth,
   reads as a credential. A valid signature proves who wrote the text, not that
   it is safe to keep. The check recognizes the plain cases and cannot prove
   absence.

### Dual config-lane and repository storage

`ExternalBindingStore` writes each binding revision to the canonical store first and
then to the config lane:

1. **Selected-part `DocumentService`**: graph `external-bindings:<account_id>`,
   `graph_address_id = "bindings"`, `doc_id = <binding_id>`, using `SaveDocument`
   with `ExpectedRevision` compare-and-swap so every binding rotation or
   retirement creates an immutable repository revision `1, 2, ...` readable via
   `ReadDocument(RevisionSelector.revision = binding_revision)`.
2. **Config lane (`ConfigDocumentStore` + `ConfigSupport.gate`)**: the envelope is
   gated with both house `validate.v1` and `buf.validate` rules before the
   canonical save, and written only after that save commits, as one immutable
   document per revision named
   `ExternalPublicationCoordinates.configRevisionDocumentName(accountId, bindingId, revision)`
   (`external-binding.<base64url(len(account_id) || account_id || len(binding_id) || binding_id)>.r<revision>`).
   A refused or lost canonical save never leaves a mirror, and concurrent
   rotations never overwrite each other's mirror. If the mirror write fails after
   the canonical save, the call returns `UNAVAILABLE` naming the committed
   revision, and `ExternalBindingStore.mirrorRevision` rewrites the mirror from
   the canonical revision.

Binding documents carry an ACL of `READ` for account members and `WRITE` only for
the `bindingWriters` identities the store is constructed with. A binding decides
where publications go and which credentials they use, so an account member with
a writer grant for the bindings datasource still cannot rotate or retire a
binding unless named as a binding writer. The store refuses an empty writer set
and a `public` writer. Calls that change nothing still prove the caller is a
writer: retiring an already retired binding (accepted only with the same
reason) and `mirrorRevision` save the current binding document again,
unchanged, so the repository checks `WRITE` on it.

When an endpoint changes or a binding is rotated or retired:

- Rotating updates the live `ServiceProfile` reference or endpoint summary under
  `binding_revision = N + 1`. Existing `ExternalLink` projections retain their
  immutable `ExternalBindingSnapshot` from revision `N`, so historical links
  remain interpretable after rotation. A rotation keeps the provider and the
  `retention_policy`; changing either is refused with `FAILED_PRECONDITION`, so
  every ledger under a binding is kept or purged by one policy.
- Retiring transitions `lifecycle` to `RETIRED` at `binding_revision = N + 1`.
  New publications against a retired binding fail immediately with
  `FAILED_PRECONDITION`, while historical links, snapshots, and pending
  reconciliations remain readable.

## `ExternalLink`, processing identities, and managed-search stages

An `ExternalLink` binds:

- `binding_id`, `binding_revision`, and immutable `ExternalBindingSnapshot`
- canonical `external_object_id` and `optional string external_version` (using
  proto3 explicit presence so unversioned external objects report
  `hasExternalVersion() == false` rather than an empty-string sentinel)
- full `source_address` (`NodeAddress`) and exact `source_revision` (`> 0`)
- optional `ExternalProcessingIdentity`:
  - `part_digests`: SHA-256 digests of the exact retained parts read via
    verified `ReadDocument` at `source_revision`
  - `retained_schema_sha256s`: retained schema descriptor digests for typed parts
  - `mapping_id` and `generation_digest`
  - optional `NlpProcessingReceiptSummary` (`provider_id`, `model_revision`,
    `bundle_manifest_sha256`, `input_sha256`, `output_sha256`, `receipt_sha256`)
- optional `ManagedSearchIdentity` aligned with
  [managed-publication.md](../search/managed-publication.md) and
  [repository-rust-integration.md](../search/repository-rust-integration.md):
  - `history_id`, binary `document_key` (bijective length-delimited
    `logicalSourceKey(source_address)`), `index_key`, `accepted_version`, and
    `accepted_sequence`
  - optional `published_catalog_epoch` and `after_manifest_sha256`
  - separate advisory `serving_visibility` (`UNOBSERVED`, `NOT_YET_VISIBLE`,
    `VISIBLE`) and `serving_observed_at`
- verified `ExternalProviderEvidence` (`confirmation_evidence`) and
  `projection_lifecycle` (`ACTIVE`, `SUPERSEDED`, `DELETION_PENDING`, `DELETED`)

For `MANAGED_SEARCH` bindings, source acceptance (`accepted_version` +
`accepted_sequence` without `published_catalog_epoch`) settles the operation in
`PENDING` with phase `ACCEPTED_AWAITING_PUBLICATION` and creates **no** active
`ExternalLink`. Only verified durable publication evidence carrying a positive
`published_catalog_epoch` and `after_manifest_sha256` confirms the operation as
`CONFIRMED` and activates `current_link`. Serving visibility remains a separate
observation and never substitutes for publication proof.

Refreshing serving visibility (`reconcileOperation(..., includeServingObservation = true)`
on a confirmed operation) requires `WRITE` access before querying the provider and
never reports a failed refresh as a replay. An unreachable status endpoint
(`UNAVAILABLE`, `DEADLINE_EXCEEDED`) or an `Unavailable` provider answer fails the
call with `UNAVAILABLE`; any other provider status, such as `PERMISSION_DENIED`, is
returned with its own code; a provider with no record of the confirmed operation or a
receipt that fails verification fails the call with `FAILED_PRECONDITION`. In each
case the ledger keeps the last observed visibility.

## Operation identity, attempt lifecycle, and provider verification

### Stable operation keys and altered-intent refusal

`ExternalOperationKeys.computeIntentSha256(ExternalPublicationIntent)` computes a
canonical length-delimited SHA-256 over:

- `account_id` and `ExternalActionKind` (`PUBLISH`, `DELETE`, `PURGE_DELETE`)
- `source_address` and either `source_revision` (`PUBLISH`, `DELETE`) or
  `purge_id` (`PURGE_DELETE`)
- `binding_id`, `binding_revision`, and `external_object_id`
- sorted `ExternalProcessingIdentity` digests and `ManagedSearchIdentity` pin
  fields (`history_id`, `document_key`, `index_key`)

When callers omit an explicit operation key, `ExternalOperationKeys.deriveOperationId`
derives a deterministic UUIDv5 key from `(account_id, intent_sha256)`. Retrying
under an existing `operation_id` with the exact same `intent_sha256` replays
idempotently (returning settled outcomes without re-calling the provider, or
resuming pending work). Reusing an `operation_id` with any altered source
revision, binding revision, object ID, action, or digest fails deterministically
with `ALREADY_EXISTS`.

### Operation states and lost-reply reconciliation

| State | Pending phase | Meaning |
|---|---|---|
| `PENDING` | `DISPATCHING` | Durable intent recorded in `external-publications:<account_id>` before outbound provider call |
| `PENDING` | `UNRESOLVED_LOST_REPLY` | Outbound call timed out, lost its reply, or hit an unavailable endpoint; external outcome is unknown |
| `PENDING` | `ACCEPTED_AWAITING_PUBLICATION` | Provider durably accepted source input (`accepted_version`), awaiting catalog publication |
| `CONFIRMED` | `NONE` | `ExternalProviderVerifier` verified publication receipt (and optional Ed25519 `SignedWorkRecord`) |
| `FAILED` | `NONE` | Provider rejected the request or verification failed; no active `ExternalLink` is created. When a sent publication's answer failed verification, or the provider rejected an attempt after an earlier attempt's outcome was lost, `provider_may_hold_object` is set: the provider may hold the object, so it stays a deletion target and keeps a `PURGE_WITH_SOURCE` ledger until a verified deletion settles it. A deletion settles only the publications already marked when it was first registered (`covers_uncertain_publication_ids`): registration order does not show which one the provider ran first |
| `DELETION_PENDING` | `DELETION_IN_FLIGHT` | Tombstone or purge deletion recorded durably; outbound delete in flight or awaiting settlement |
| `DELETED` | `NONE` | `ExternalProviderVerifier` verified external deletion receipt; active link retired to `DELETED` |

Confirmation (`CONFIRMED` or `DELETED`) requires `ExternalProviderVerifier` to
validate the provider receipt against the binding snapshot (`provider`,
`dataset`, `namespace`, `contract_version`), expected `external_object_id`,
`source_address`, `source_revision` (or `purge_id`), non-empty `receipt_sha256`,
managed-search catalog epoch invariants, and, when present or required by
policy, cryptographic `SignedWorkRecord` verification against `TrustSnapshot`.
After `RecordVerifier.verify` authenticates the Ed25519 signature,
`ExternalProviderVerifier` verifies that the signed `WorkRecord` manifest has
`COMPLETENESS_STATUS_COMPLETE`, binds `subject.workflow_fingerprint` to the
expected `intent_sha256`, and carries an unredacted
`application/vnd.protomolt.external-settlement-claim.v1+octet-stream`
`RecordArtifact` matching `ExternalProviderVerifier.canonicalSettlementClaimBytes`
over the binding identity/revision, provider/dataset/namespace/contract version,
service profile/endpoint/credential-ref digest, external object ID and version,
provider receipt ID, settlement stage, and durable managed-search identities.

Before every mutating provider call, the operation retains its non-secret
`dispatch_credential_binding_sha256s`. Retries add the credential-reference fingerprint
they actually use. Reconciliation and serving refresh authenticate with live credentials,
but verify durable receipts against those retained dispatch fingerprints. Rotating a
credential reference therefore does not invalidate an earlier signed receipt, a
fingerprint never recorded for a dispatch is refused, and a recorded operation missing
`dispatch_credential_binding_sha256s` fails closed with `DATA_LOSS`.

Publication admission reserves room for cleanup within the existing ledger bounds:
the current projection and pending publications retain space for their eventual historical
links, and each distinct external target retains an operation slot for deletion. A new
publication that would consume this reserve fails with `RESOURCE_EXHAUSTED` before any
provider call. Deleting historical links updates them in place even when history is full.
These are finite ledgers, not unbounded audit logs: repeated failed deletion operations
can still exhaust the operation bound; the reserve covers a cleanup operation per target.

When a reply is lost (`UNRESOLVED_LOST_REPLY`) or the coordinator restarts,
`reconcileOperation` and `reconcilePendingForSource` reload durable ledgers from
`DocumentService`, invoke `ExternalProviderGateway.queryStatus` with live
`ServiceProfile` credentials, and settle the operation without duplicate
external side effects.

## Revision fencing, tombstones, purges, and authorization

Each `ExternalPublicationLedger` maintains three monotonic fences:

- `highest_fenced_source_revision`: advances on every confirmed publication or
  tombstone deletion.
- `tombstoned_at_source_revision`: records the latest tombstoned repository
  revision.
- `source_purged` and `last_purge_id`: records revision-zero repository purges.
- `purge_preparation`: the purge prepared while the source still existed; a
  purge is handled only for this purge ID.

Ordering invariants:

1. **Older binding revisions**: a publication through a binding revision is
   refused (`FENCED_BY_NEWER_BINDING`, nothing sent) when a newer revision
   already holds the same object at the same place for that source revision or
   a later one; sending it would replace the newer projection at the provider.
2. **Superseded revisions**: Once revision `r2 > r1` is published or tombstoned,
   a new publish attempt for `r1` is refused (`FAILED_PRECONDITION`). If an
   earlier in-flight operation for `r1` suffered a lost reply after committing
   on the provider and reconciles after `r2` is already `ACTIVE`, `r1`'s
   operation records its verified receipt truthfully while its `ExternalLink`
   lands directly in `historical_links` as `SUPERSEDED` (with
   `retired_by_operation_id` pointing to `r2`'s operation), leaving `current_link`
   untouched at `r2`.
3. **Deletion claims**: when a deletion is first registered it adds its
   operation ID to `deletion_claimed_by_operation_ids` on every link on record
   that it covers, beside any other deletion's claim still in flight. Only the
   verified evidence of a claiming deletion retires those links, all of them at
   once when late publications left several owed for the same object;
   a retry claims nothing new, and a later supersession, which overwrites
   `retired_by_operation_id`, leaves the claim in place. Whether a superseded
   projection is covered by a later deletion is decided per object at one place
   in the external system (provider, dataset, namespace, endpoint and object
   ID), so a binding revision that only rotated credentials names the same
   object.
4. **Tombstone ordering**: `deleteForTombstone` verifies via `GetDocument` that
   the source document is tombstoned at `tombstone_revision`, marks any active
   link `DELETION_PENDING`, advances `tombstoned_at_source_revision`, and settles
   to `DELETED` only on verified provider deletion evidence. Delayed pre-delete
   publish retries (`source_revision <= tombstoned_at_source_revision`) cannot
   restore `current_link`.
5. **Revision-zero purges** run in two steps. First, while the source still
   exists, `preparePurge(source, purge_id, binding)` copies the access rules of
   the source's latest live revision onto the ledger, replacing whatever rules
   the last publication left there, and records `purge_preparation` (the purge
   ID, the source revision the rules came from, and the time). It is called
   immediately before `PurgeDocument`; rules changed on the source after it are
   not on the ledger. Preparing again under the same purge ID refreshes the
   copy; preparing under another ID while one is recorded is refused, since the
   purge announced under the first ID would then find nothing prepared. Without this step a ledger kept after the purge would
   answer to the rules of the last publication, including grants the source had
   since revoked, and nothing would be left to read the current rules from.
   Second, after `PurgeDocument`, `handlePurge` runs, and only for the purge ID
   that was prepared; any other ID is refused with `FAILED_PRECONDITION`, and the
   ledger's validation rule `ledger-purge-prepared` refuses a purged ledger with
   no matching preparation. `PurgeDocument` emits revision-zero purge identity
   (`purge_id`) rather than a document revision. `handlePurge` never
   fabricates a synthetic `source_revision`: it records `PURGE_DELETE` keyed by
   `purge_id`, sets `source_purged = true`, settles external deletion, and then
   applies the binding's `ExternalRetentionPolicy`:
   - `RETAIN_HISTORY`: retains the ledger with `current_link` cleared and
     non-secret historical receipts preserved for audit.
   - `PURGE_WITH_SOURCE`: invokes `DocumentService.PurgeDocument` on the ledger
     document once `source_purged` is set, nothing is outstanding (whichever
     pending publication or deletion settles last), and every operation on the
     ledger was recorded under `PURGE_WITH_SOURCE`. That decision is made again
     inside a ledger update, which sets `purge_with_source_started_at`; a ledger
     carrying it refuses every further save with `FAILED_PRECONDITION`, so an
     operation registered concurrently is either seen by the check or refused,
     never purged unseen. If the caller settling the last external operation holds
     `WRITE` on the source's retained rules without repository purge-administrator
     authority, external settlement succeeds with `ledgerPurged == false` and
     leaves `purge_with_source_started_at` set; a purge administrator (or any
     retry after a transient `PurgeDocument` outage) completes the repository
     purge by replaying `handlePurge` or `reconcileOperation`.
6. **Repository and account authorization**: An `ExternalLink` grants neither
   repository access nor external provider access. Every read (`getLedgerView`,
   `listActiveExternalLinks`, and pure terminal replay in `reconcileOperation`)
   and mutation (`publish`, `deleteForTombstone`, `preparePurge`, `handlePurge`,
   and pending or serving-observation `reconcileOperation`) checks caller account
   membership and `DocumentAccessPolicy` permissions against the current source
   document (the last live revision's rules when the source is tombstoned, the
   ledger's retained `DocumentSecurity` ACL once it is purged). Every mutating
   operation—including reconciling a non-terminal operation or refreshing serving
   visibility—verifies `WRITE` access before resolving outbound provider
   credentials or calling the provider. A ledger save writes the live source's rules
   exactly, even when they hold no explicit grant (that is a revocation), keeps
   the ledger's own rules only when no live rules exist, and keeps the rules a
   purge preparation copied once one is recorded. Cross-account and unauthorized callers receive
   `PERMISSION_DENIED`. Publishing and preparing a purge also require `WRITE`
   on the source's latest live revision (the revision before a tombstone when the
   source is deleted), checked by the coordinator against
   the caller the host supplies through `ExternalPublicationCaller` (the same
   identity it forwards to `DocumentService`). Reading the source is not enough
   to publish it, as in search engines that tie indexing to write access on the
   index; a separate publish right can be added later without changing the
   ledger.

## Canonical storage layout and avoiding circular publication updates

External bindings and publication ledgers use the canonical selected-part
`DocumentService` (`SaveDocument`, `GetDocument`, `ReadDocument`,
`ListDocuments`, `PurgeDocument`) without introducing a parallel repository SQL
table, guessed S3 key layout, or source-document rewrite:

| Artifact | `account_id` | `graph_id` | `graph_address_id` | `doc_id` |
|---|---|---|---|---|
| `ExternalSystemBinding` | `<account_id>` | `external-bindings:<account_id>` | `bindings` | `<binding_id>` |
| `ExternalPublicationLedger` | `<account_id>` | `external-publications:<account_id>` | `<binding_id>` | `extpub-v1-<base64url(logicalSourceKey(source_address))>` |

`logicalSourceKey(NodeAddress)` encodes a version byte (`0x01`) followed by four
big-endian 32-bit UTF-8 byte lengths and bytes for `(account_id, graph_id,
graph_address_id, doc_id)`, guaranteeing a bijective, collision-free mapping even
when address segments contain colons, slashes, Unicode characters, or NUL bytes.

### Why circular publication updates cannot occur

Writing `ExternalLink` state back into the source document's `CORE` part would
increment the source document's revision on every publication attempt or status
reconciliation, emitting a fresh `Accessioned` outbox event and triggering an
infinite re-publication loop. Storing publication state in
`external-publications:<account_id>` eliminates circular updates by construction:

1. **Zero source-document mutation**: `ExternalPublicationCoordinator` reads the
   source document strictly via `GetDocument` and `ReadDocument` at an exact
   `RevisionSelector.revision = source_revision`. Saving or reconciling a ledger
   mutates only `external-publications:<account_id>` and never increments the
   source document's revision or emits an outbox event for the source address.
2. **Exact immutable source pin**: Every `ExternalPublicationIntent` and
   `ExternalLink` pins `(source_address, source_revision)` plus verified part
   and schema digests from that exact revision.
3. **Explicit internal-graph guards**:
   `ExternalPublicationCoordinates.isInternalExternalGraph(graphId)` and
   `isInternalExternalAddress(NodeAddress)` identify `external-bindings:*` and
   `external-publications:*` addresses so outbox subscribers skip internal
   binding/ledger commits, and `ExternalPublicationCoordinates.requireSourceAddress`
   refuses any call that attempts to publish an internal binding or ledger
   document as a source document.
