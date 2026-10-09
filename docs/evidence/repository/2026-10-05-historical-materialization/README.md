# Historical payload materialization

`DocumentSchemaMaterialization` now handles the historical-reuse content arm
explicitly, alongside upload and current reuse. It compares declared size and
SHA-256 with both the copied fragment and retained root evidence before loading
schema artifacts. Empty or unsupported payload alternatives fail closed.

The new regression failed on the prior implementation, which refused the historical
arm. It now decodes actual nested Any bytes using file-backed retained descriptors,
and refuses changed size/hash before any schema read. Reservations return to zero
after success and refusal. The fixture intentionally supplies only the fields this
leaf consumes; it is not evidence of a valid public restore command or authorization.

Validation:

```
./gradlew :protomolt-repo-admission:test --console=plain
```

244 tests passed, with no failures, errors or skips. Runtime dependency boundary
checks passed in the same build. Sol reviewed the change without a material blocker.

The host must still authenticate exact revision/ordinal/slot/physical identity and
retain the historical read Use. The public command still refuses historical reuse.
No restore endpoint, hosted CI, merge or deployment is established by this test.
