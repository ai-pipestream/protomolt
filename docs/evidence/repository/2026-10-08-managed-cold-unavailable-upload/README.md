# Unavailable saved upload backend during cold recovery

Three crash phases (initial capture, reserved successor and installed successor)
now qualify the missing original upload backend independently of historical source
availability. The internal writer publishes source objects on generation A, binds
generation B to the same real S3 profile, and records B as the upload placement.
It halts with exit 23 after the selected durable boundary. The supervisor verifies
process exit and disappearance of its SQL sessions before recording termination.

A fresh JVM builds the managed host on A only. It can validate the historical source
before and after the refused publication. Library and authenticated in-process gRPC
calls both return FAILED_PRECONDITION for unavailable B; the initial phase uses RPC
first and the other phases use library first. Retry preserves the exact recovery
claim/owner identity. Every preparation in the lineage is decoded with its digest
checks and must still contain B and its original physical profile.

The refusal occurs after a real successor activation and START, not at source lookup.
The original/recovered attempts have exactly two STARTs and one activation. There is
no fresh schema resolution, assessment owner, revision commit or success receipt.
All real S3 object versions and delete markers are identical before/after the calls,
so recovery did not write to the available current backend instead. Both readers
owned by the managed recovery host quiesce, and that host is fenced on close.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.coldManagedUnavailableUpload' \
  --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.cold*ProcessRestart' \
  --tests '*HistoricalRuntimeQualificationTest.coldPublicCorruptPreparation' \
  --max-workers=2 --console=plain
```

The new three-case gate passed in 1m36s. All ten earlier private/public/managed cold
cases and corrupt-journal coverage also passed. Both XML reports show zero failures,
errors or skips. Sol reviewed the separated source/upload identities, retry checks,
provider version evidence and dedicated negative-case exit path. Two initial fixture
compile errors were corrected: construct placement from its immutable drive snapshot
and keep the selected placement effectively final. No production changes were needed.

The writer uses internal composition because the managed host currently mounts one
generation. This does not qualify multi-backend host routing or provisioning. The
negative path deliberately leaves the original crashed writer's retained capture
in the disposable database: it does not claim a terminal receipt, permission to prune
an unfinished operation, or orphan-root reclamation. Existing positive recovery
tests retain their separate supervisor reclamation checks. Deployment, listener
failure, throughput and horizontal scale remain unqualified here.
