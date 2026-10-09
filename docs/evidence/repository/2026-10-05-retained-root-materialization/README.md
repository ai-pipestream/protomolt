# Retained typed-root materialization

Local verification on 2026-10-05, based on `503b5fb7` plus recorded source hashes.
Sol reviewed root selection, guards, resource/lifetime boundaries and fixture scope.

```sh
./gradlew :protomolt-repo-admission:test :protomolt-repo-admission:checkRuntimeBoundaries --console=plain
```

All 193 admission tests passed with zero failures, errors or skips, including eight
new retained-root cases. Archived XML is in `results.tar.gz`. The production
runtime dependency gate passed; no dependency or protobuf contract changed.

Fixtures split real Document protobufs, inventory exact CORE/PARSED fragments and
read canonical metadata/descriptor files through the existing retained reader.
Fresh reader instances decode without a registry. Tests delete and corrupt files,
verify preserve performs zero asset reads, reject wrong root/value/schema bindings,
select the correct PARSED map key, distinguish malformed payload from resource
refusal, and preserve host/storage callback exceptions. The malformed-payload case
deliberately constructs inconsistent recorded evidence; it is not a successful
publication. Fresh instances are not an OS process-restart qualification.

This is a package-private storage-independent adapter. The host must authenticate
revision/part ordinal, verify stored evidence codec/digests, capture the actual
fragment, enforce current access and own all budgets/lifetimes. Parsed evidence
shape failures violate that input contract; the host maps stored codec failures.
No SQL-host integration, nested Any traversal, public read mode, registry cache,
restore, deployment or validation verdict is established by these tests.
