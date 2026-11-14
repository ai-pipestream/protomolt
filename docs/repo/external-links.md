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
- monotonic `binding_revision` (`>= 1`) and optional `config_lane_version`
- `lifecycle` (`ACTIVE` or `RETIRED`) and `retention_policy`
  (`RETAIN_HISTORY` or `PURGE_WITH_SOURCE`)
- non-secret `ExternalEndpointSummary` (`host`, `port`, `tls_enabled`) derived
  from the resolved `ServiceProfile` endpoint at binding save time

### Zero-secret persistence rule

Bindings, binding snapshots, labels, display URLs, and publication ledgers
persist **no** secrets, bearer tokens, URL userinfo, credential query
parameters, or private key material:

1. Live credentials resolve exclusively at call time via
   `ProfileCredentialResolver` (`#781`) against the referenced `ServiceProfile`.
2. `ExternalPublicationCoordinates` rejects userinfo (`@`), non-HTTP(S) schemes,
   and secret-bearing query parameter names or values (including percent-encoded
   variants such as `%74%6F%6B%65%6E`, `access_token`, `api_key`, `sig`,
   `X-Amz-Signature`, `Bearer `, and PEM headers) without regular expressions.
3. Console rendering (`safeExternalLinkHref` in
   [linksCore.ts](../../apps/console/src/repository/linksCore.ts)) re-validates
   `display_url` before emitting an anchor `href`, rendering unsafe values as
   non-navigable text.

### Dual config-lane and repository storage

`ExternalBindingStore` writes each binding revision to two aligned stores:

1. **Selected-part `DocumentService`**: graph `external-bindings:<account_id>`,
   `graph_address_id = "bindings"`, `doc_id = <binding_id>`, using `SaveDocument`
   with `ExpectedRevision` compare-and-swap so every binding rotation or
   retirement creates an immutable repository revision `1, 2, ...` readable via
   `ReadDocument(RevisionSelector.revision = binding_revision)`.
2. **Config lane (`ConfigDocumentStore` + `ConfigSupport.gate`)**: validated with
   both house `validate.v1` and `buf.validate` rules and published under the
   collision-free config name
   `ExternalPublicationCoordinates.configDocumentName(accountId, bindingId)`
   (`external-binding.<base64url(len(account_id) || account_id || len(binding_id) || binding_id)>`),
   recording the resulting `config_lane_version` on the saved binding.

When an endpoint changes or a binding is rotated or retired:

- Rotating updates the live `ServiceProfile` reference or endpoint summary under
  `binding_revision = N + 1`. Existing `ExternalLink` projections retain their
  immutable `ExternalBindingSnapshot` from revision `N`, so historical links
  remain interpretable after rotation.
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
| `FAILED` | `NONE` | Provider rejected the request or verification failed; no active `ExternalLink` is created |
| `DELETION_PENDING` | `DELETION_IN_FLIGHT` | Tombstone or purge deletion recorded durably; outbound delete in flight or awaiting settlement |
| `DELETED` | `NONE` | `ExternalProviderVerifier` verified external deletion receipt; active link retired to `DELETED` |

Confirmation (`CONFIRMED` or `DELETED`) requires `ExternalProviderVerifier` to
validate the provider receipt against the binding snapshot (`provider`,
`dataset`, `namespace`, `contract_version`), expected `external_object_id`,
`source_address`, `source_revision` (or `purge_id`), non-empty `receipt_sha256`,
managed-search catalog epoch invariants, and, when present or required by
policy, cryptographic `SignedWorkRecord` verification against `TrustSnapshot`.

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

Ordering invariants:

1. **Superseded revisions**: Once revision `r2 > r1` is published or tombstoned,
   a new publish attempt for `r1` is refused (`FAILED_PRECONDITION`). If an
   earlier in-flight operation for `r1` suffered a lost reply after committing
   on the provider and reconciles after `r2` is already `ACTIVE`, `r1`'s
   operation records its verified receipt truthfully while its `ExternalLink`
   lands directly in `historical_links` as `SUPERSEDED` (with
   `retired_by_operation_id` pointing to `r2`'s operation), leaving `current_link`
   untouched at `r2`.
2. **Tombstone ordering**: `deleteForTombstone` verifies via `GetDocument` that
   the source document is tombstoned at `tombstone_revision`, marks any active
   link `DELETION_PENDING`, advances `tombstoned_at_source_revision`, and settles
   to `DELETED` only on verified provider deletion evidence. Delayed pre-delete
   publish retries (`source_revision <= tombstoned_at_source_revision`) cannot
   restore `current_link`.
3. **Revision-zero purges**: `PurgeDocument` emits revision-zero purge identity
   (`purge_id`) rather than a document revision. `handleSourcePurge` never
   fabricates a synthetic `source_revision`: it records `PURGE_DELETE` keyed by
   `purge_id`, sets `source_purged = true`, settles external deletion, and then
   applies the binding's `ExternalRetentionPolicy`:
   - `RETAIN_HISTORY`: retains the ledger with `current_link` cleared and
     non-secret historical receipts preserved for audit.
   - `PURGE_WITH_SOURCE`: invokes `DocumentService.PurgeDocument` on the ledger
     document once external deletion settles.
4. **Repository and account authorization**: An `ExternalLink` grants neither
   repository access nor external provider access. Every read (`getLedgerView`,
   `getActiveLinks`) and mutation (`publish`, `deleteForTombstone`,
   `handleSourcePurge`, `reconcileOperation`) checks caller account membership
   and `DocumentAccessPolicy` permissions against the current source document
   (falling back to the ledger's retained `DocumentSecurity` ACL when the source
   is tombstoned or purged). Cross-account and unauthorized callers receive
   `PERMISSION_DENIED`.

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
