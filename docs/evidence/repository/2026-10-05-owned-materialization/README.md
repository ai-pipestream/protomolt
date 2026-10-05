# Owned retained occurrence decoding

Parent commit: `3ac663dd7c8aabe66cf264d52480f897dc9f6706`.
Source hashes, focused JUnit XML and the complete admission XML archive accompany
this evidence. No repository protobuf or public transport operation changed.

```sh
./gradlew :protomolt-repo-admission:test :protomolt-repo-admission:checkRuntimeBoundaries --console=plain
```

All 224 admission tests passed with zero failures/errors/skips. Ten new facade
cases use real serialized nested Any messages and file-backed schema metadata and
descriptors. Their canonical evidence is a fixture, not a successful admission
proof or an authenticated SQL revision.

The cases verify:

- Copied fragment/asset inputs survive reuse of borrowed backing arrays; returned
  value/schema/envelope agree, close releases ownership, and closed access fails.
- An unused association with absent metadata is not loaded.
- Selected root/path, command fragment and retained-row identities cannot be changed.
- Corrupt evidence and missing required assets fail as data loss.
- Every reservation refusal releases prior leases and preserves the host exception.
- Sampled control failures across phases, including final delivery, release ownership.
- Exact decoded-input allowance succeeds; tighter byte/count bounds refuse.
- A real descriptor set with over 256 files reports a resource failure.
- Reader exceptions retain identity, including an internal-looking limit type.
- Malformed stored associations fail before asset reads.

Sol reviewed the implementation and subsequent failure-classification changes.
Reservations account for serialized copies and decoded-input allowance, not actual
JVM object overhead. Hosts must additionally bound parsed heap/concurrency. Result
use/close is not thread-safe. SQL revision authorization, read-pin ownership,
process crash recovery and public read integration remain unproven by these tests.
