# Conditional transcript persistence

Status: proposed contracts and implementation plan. No conditional blob RPC is
available yet. This is a prerequisite for durable automatic review and retry.

## Failure being addressed

The transcript repository replaces an encrypted snapshot through PutBlob. An RPC
timeout does not prove that storage rejected the write. A delayed old request can
overwrite a newer recovered snapshot. Stopping the original coordinator alone
does not stop that in-flight request.

## Protocol

Add GetBlobForUpdate and CompareAndPutBlob to DocumentService when their handlers
are ready. Define standalone messages first. New method names make an older
server return UNIMPLEMENTED instead of silently ignoring a new precondition on
the existing unconditional PutBlob request.

GetBlobForUpdate addresses the current object by explicit drive name and object
key. Return current bytes, MIME type, SHA-256, size and an opaque backing ETag
from the same authoritative read. Missing objects return NOT_FOUND. Never return
a Redis cache's synthetic ETag for an S3 object. The tag is an equality token,
not a sequence number, content checksum or promise that every write changes it.
Require one strong quoted ASCII ETag. Reject bare wildcards, weak tags, header
lists and control characters; an S3 If-Match wildcard would otherwise bypass
the intended comparison. Backends with incompatible token formats report
unsupported instead of inventing an equivalent token.

CompareAndPutBlob addresses the same explicit coordinates and requires exactly
one precondition: create only when absent, or match a nonempty exact ETag. A
false absent flag is invalid. The backend must evaluate the condition atomically
with replacement. No read-then-unconditional-write implementation is allowed.
Return the new backing ETag, actual coordinates, size and SHA-256 on success.
Require data length to match the declared response size. Handlers check the
request/response coordinates, digest and size against the actual bytes; annotation
validation cannot establish these cross-call facts.

Use native validation for required coordinates, bounded tokens and payloads,
exclusive alternatives, true-only create condition, digest format, and read
response size consistency. Requests and successful responses are validated.
Use a 9 MiB payload limit for this bounded snapshot API, allowing the existing
8 MiB plaintext transcript plus its bounded encryption envelope, rather than changing
the existing bulk/unconditional blob operations. Empty blobs remain valid.

## Errors and uncertain outcomes

Invalid contracts return INVALID_ARGUMENT. Unknown drive or absent read returns
NOT_FOUND. A failed write condition returns ABORTED without changing the object.
Unsupported atomic backend behavior returns UNIMPLEMENTED, never a fallback
write. Preserve the existing raw-blob authentication boundary; the current
handler resolves drive names without per-drive authorization. This work does
not claim to fix that existing limitation. Unknown annotation rules fail
closed. Transport deadlines/cancellation and UNAVAILABLE do not prove rollback.

This API is compare-and-set, not an idempotency-key service. Retrying an applied
write with its old tag can return ABORTED. On an uncertain save the transcript
repository stops accepting writes and dispatching review. Recovery reloads the
authoritative snapshot and its tag before a new coordinator publishes. A delayed
request based on the prior tag can win before recovery's first write, or lose
after it; it cannot overwrite a successful replacement based on a newer tag.
If it wins first, recovery's stale conditional write fails, also stopping dispatch.

ETag equality permits ABA if another actor restores identical bytes. Transcript
snapshots are append-only logical histories with fresh encrypted envelopes;
backend qualification must verify changed encrypted bytes produce a different
ETag. Never restore an older snapshot at the same active object key. Offline restore
requires a new key and stopped writers. Deployment must stop older unconditional
coordinators before enabling this path. Mixed unconditional writers, concurrent
deletes and external object rollback are outside the fencing guarantee.

## Backend and adapter obligations

- BlobStore gains authoritative read and conditional put operations. Defaults
  fail closed; providers explicitly implement supported behavior.
- S3 uses atomic If-None-Match or If-Match PUT, preserving the opaque token.
  Map conditional failures without retrying an unconditional PUT.
  Conditional operations are disabled by default: S3 API compatibility alone
  is insufficient. The operator must explicitly enable a qualified backend.
  LocalStack 3.8 accepted a stale If-Match in the RPC tests, while the pinned
  RustFS image rejected it. The deployment setting must reflect that distinction.
- CachingBlobStore bypasses Redis for authoritative reads and delegates writes
  to the backing store. Invalidate cached data after a successful replacement.
- Redis requires an atomic script for comparison and replacement, or reports
  unsupported. RemoteBlobStore calls the new RPCs, or reports unsupported.
- RepositoryServiceTranscriptRepository remembers the loaded token, creates
  with the absent condition, replaces with its token, and advances the token
  only after a verified successful response. Any uncertain save poisons that
  repository instance. It must not continue from its old snapshot. A fresh
  instance must load existing history before replacing it; saving before load
  can only create an absent object. Reject truncated or divergent histories and
  unknown transcript/envelope fields before adopting or replacing state.

The installed NAS backend is RustFS. Qualification must exercise its pinned
image; SDK header support and LocalStack tests alone do not prove deployment
support. No deployment or availability claim follows from these definitions.
The repository service keeps conditional S3 operations disabled by default;
`DOCUMENT_PLATFORM_S3_CONDITIONAL_WRITES=true` is set only in the checked-in
Portainer configurations for the pinned, qualified RustFS image. Upgrade the
repository service before switching the coordinator to conditional transcript
RPCs, and stop all old coordinators that still issue unconditional PutBlob
writes to the active transcript key before that switch. This is a deployment
order requirement, not evidence that deployment has happened.

## Acceptance backlog

The contract fixture records the current JSON Schema projection: bytes become
base64 strings and message CEL appears as `x-protomolt-cel` metadata. Byte-length
limits are not translated into JSON Schema constraints. CEL still requires the
runtime validator; metadata alone does not enforce the write-precondition or
read-size rule. These standalone messages add no OpenAPI operation. Generator
changes remain outside this work.

1. Contract fixtures: generated and dynamic native validation of valid empty
   data, absent/match alternatives, false/missing conditions, bounds, malformed
   digests, and inconsistent successful read sizes. Record schema projection
   coverage and runtime-only CEL; do not change generators here.
2. Atomic storage: absent create succeeds once; two writers using one tag have
   one winner; rejected writes preserve bytes. Authoritative reads pair bytes
   and tag. Exercise cache mismatch, unsupported providers and old servers.
3. Transcript adapter: confirmed save advances token; ambiguous acknowledgement
   stops further writes/dispatch; delayed writes racing recovery cannot erase
   a newer committed transcript. Verify real encryption and transcript replay.
4. Backend integration: run conditional races against the pinned RustFS image,
   including changed-byte ETags, stale overwrite and concurrent absent create.
   Verify transport limits accommodate the payload plus message framing. Keep existing
   PutBlob behavior compatible for its existing callers.
5. Only then wire durable review start/failure/deferred/retry and its process
   recovery test. Browser, workflow-worker crash, Kafka, image-only Compose and
   anonymous/native architecture qualification remain part of Goal 5.
