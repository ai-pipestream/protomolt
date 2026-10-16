# PR 313: Codex bug-hunt findings

Review base: `7769405d52705da059d0bf7fe064731fd9f295b8`.
Working branch: `review/pr313-codex-bugs`.
Review requested from **Argon and Fable**; Antigravity may also review.
This is a bounded repair pass, not approval of the entire PR or later slices.

## Fixed: wildcard-only selection reads unrelated parts

Requirement: selected-part reads return only the requested content (requirements
§8; `GetDocumentRequest.selection`; `PartSelection` semantic wildcards).

Call path: `AccessionGrpcService.getDocument` -> `DocumentReads.getDocument` ->
`selected` -> existing stack read and manifest content-inclusion calculation.
No new codec, validator, storage path, or public contract is needed.

Previously `selected` interpreted an empty explicit-selector list as all parts,
even when `all_segmentations` or `all_embedding_sets` was set. A request for
semantic parts therefore fetched and returned unrelated CORE, BLOBS and PARSED
payloads. The fallback now applies only when neither wildcard is set.

The regression uses the existing public in-process gRPC fixture backed by real
PostgreSQL 18 and LocalStack 3.8. It saves a document with three non-semantic parts
and requests each semantic wildcard and their union. It asserts no content is
returned, no manifest entry says content was included, and no stack objects are
opened. Existing assertions cover the default full read and an explicit BLOBS read.

Evidence:

- Before the fix, `AccessionGrpcIT` failed because the complete document was
  returned: `/tmp/pr313-codex-selection-red.log`.
- After the fix, the full accession suite passed 25 tests with zero failures,
  errors or skips: `/tmp/pr313-codex-accession-green.log`.
- The final targeted run with the additional no-stack-open assertion passed:
  1 test, zero failures/errors/skips, `/tmp/pr313-codex-selection-final.log`.

## Corrections to the architectural review

- The response contract is `RecordedOutcome`, not `OperationOutcomeRecord`.
- `PostgresCatalog.compareAndSet` uses a conditional UPDATE for existing
  documents. This takes database locks; it is not a lock-free implementation or
  an explicit document SELECT FOR UPDATE at that point.
- `DocumentReads.read` calls `ByteString.readFrom`, buffering each selected
  fragment. Streaming verification in Stack does not establish bounded-memory
  unary reads or qualify the future large-streaming path.

## Fixed: nested unsupported command fields were accepted

Requirement §7 requires unsupported command fields to be refused, without
applying that policy indiscriminately to document content.

Call path: public `saveDocument` -> `DocumentSaves.saveChecked` ->
`requireNoUnknownCommandFields`, before account/authority lookup, outcome lookup,
storage writes or commit. The previous check inspected a fixed shallow list of
messages. Unknown fields inside an intake owner or write selector escaped it.

The existing descriptor `Message.getAllFields()` API now walks the full command
tree iteratively, excluding only the request's Document content field. This
reuses protobuf descriptors, not JSON, and keeps content validation in the
existing admission path. There are no new public messages or limits.

The gRPC regression adds real protobuf unknown fields to an intake owner and
write selector. Both must fail MALFORMED_REQUEST, with no outcome, no current
revision and no stack PUT. Before the fix, the first request succeeded and
committed: `/tmp/pr313-command-red.log`. After the fix, all 25 accession tests
passed with zero failures/errors/skips: `/tmp/pr313-command-green.log`.

## Source findings still needing follow-up, not silently repaired here

- `DocumentCandidateAdmission.select` maps schema-binding-count exhaustion to
  `HostFailure`, and `DocumentSaves` returns INTERNAL. A narrow error-policy
  decision is needed to distinguish resource exhaustion from corrupt resolver
  output; this pass does not invent a new terminal rejection or size ceiling.
- `DocumentReads.requireContainer` refuses a retained container fingerprint
  different from the compiled container. Exact retained-schema reads belong to
  #314, not a claim that Slice 1 already implements them.
- Source/lineage access checks in `DocumentSaves` use the source revision's
  ownership, whereas direct historical reads use current ownership. This should
  be resolved with the pending source-scope decision, including a revocation
  fixture. No authorization policy was silently changed in this patch.
- `WriterAuthority` distinguishes OWNER_UNKNOWN from a known owner for which
  the caller has no grant, as the requirements currently specify. The possible
  owner-existence disclosure remains a requirements decision.

## Reviewer checklist

- Confirm wildcard-only selections cannot read unrelated parts, while ordinary
  full reads and explicit selectors still work.
- Confirm nested command unknown fields fail before side effects; Document
  content remains governed by its own schema admission.
- Confirm these changes neither mount another service nor alter part identity,
  retained schemas, receipts, storage format or document limits.
- Review the remaining source findings above against the outstanding tickets;
  this patch does not resolve them or approve PR 313 as a whole.

## Boundaries and unfinished review

Retained-descriptor decoding is assigned to #314 and streaming to #317. No new
features from those tickets were implemented. Source authorization scope and
OWNER_UNKNOWN disclosure remain contract-review questions, not silently chosen
policies. Full container/service regression suites, RustFS and the remaining
code-bug hunt have not been completed in this pass. No push or merge is implied.
