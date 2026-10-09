# Retained definitions under a new admission policy

Local verification on 2026-10-05:

```
./gradlew :protomolt-repo-admission:test \
  :protomolt-repo-container:admissionStorageTest --console=plain
```

243 admission tests passed without failures, errors or skips. The production-JAR
PostgreSQL/LocalStack storage gate also passed; combined Gradle time was 2m39s.
Sol reviewed the extraction and final ordinal-limit correction with no blockers.
This is local qualification, not hosted CI, deployment or restore activation.

The new resolver indexes exact retained occurrences by target ordinal, root,
path, type URL and value identity. Tests cover reordered historical declarations,
selected schema subsets, two contextual definitions under one URL after remapping,
wrong paths and mappings, and fresh-time/current-policy refusal of formerly
accepted content. A two-part source can supply a one-part candidate under a new
one-part policy. Duplicate source ordinals and out-of-range target ordinals fail
before asset reads. The ordinal-3 test checks mapping acceptance; the other
remapping tests exercise actual assessment routing.

Existing replay retains its full-union checks, corruption/cancellation cases and
failure-at-every-reservation coverage. New assessments check the selected union.
Artifact bytes and parsed inputs remain owned by the authenticated source host;
the resolver owns validation scratch and borrows definitions through its lifetime.
No SQL, SPI or provider dependency was added to the admission leaf.

Synthetic protobuf fixtures establish admission behavior. The existing real-provider
gate qualifies the replay refactor, not historical restore publication. The command
still refuses historical reuse. Source authorization, retained Use lifetime, target
slot/physical binding, current policy at commit and pending schema retention remain
host obligations before that operation can be enabled. No performance or replica
scaling claim follows from these correctness runs.
