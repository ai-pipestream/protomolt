# Retained nested Any path

Local verification on 2026-10-05, based on `d9ab0b8c` plus recorded source hashes.
Sol reviewed transitions, resource bounds, retained-reference identity, callback
guards and the shared map-entry shape check without a material blocker.

```sh
./gradlew :protomolt-repo-admission:test :protomolt-repo-admission:checkRuntimeBoundaries --console=plain
```

Final results: 214 admission tests, zero failures, errors or skips, including 21
new nested-path cases. Final XML is in `results.tar.gz`. The runtime dependency gate
passed. No dependency, protobuf definition or existing public operation changed.

Tests serialize actual dynamic protobufs, split and inventory real Document
fragments, and resolve exact descriptor/metadata files. They cover same-URL child
schema variants, singular/list/map traversal, all 12 map-key types, explicit default
and unsigned keys, duplicate keys, malformed map-entry descriptors, path mismatch,
conflicting full references, missing artifacts, bounds, cancellation and a three-
boundary path whose middle definition disappears. Fault fixtures deliberately
construct invalid retained evidence; they are not successful admitted publications.
The initial targeted invocation had a test-source punctuation error; it was fixed
before these final passing results.

These are internal file-backed composition tests. They do not authenticate a SQL
revision, establish process-restart behavior or qualify a public endpoint. The
caller still verifies canonical stored evidence and its revision membership, owns
serialized/decoded memory and current authorization, and supplies the physical
fragment ordinal. Limits bound aggregate serialized decode inputs and per-boundary
wire/depth work, not exact heap. A direct omitted-key versus explicit-default path
fixture remains a follow-up; the underlying protobuf distinction has existing
`MapOccurrenceSemanticsTest` coverage.
