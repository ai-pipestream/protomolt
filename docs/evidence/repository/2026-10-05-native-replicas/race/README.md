# Shared-revision writer race

The native RustFS process fixture now adds two concurrent update JVMs against the
same original document and expected mutation revision. The children rendezvous
inside schema resolution, so both reach the admitted preparation path before
either can proceed to commit. Each submits a distinct valid payload.

The final local run passed with no failures, errors or skips. It requires one
winning publication and one durable PRECONDITION_NOT_MET rejection, with exact
same-runtime retries and fresh-process terminal observations. The loser must not
claim a schema-assessment rejection or create any revision. A fresh historical
reader verifies original and winning payloads, original command/revision bindings,
and that the current head is exactly the winner. SQL must retain exactly two
commits for this document. Across all scenarios, the fixture now produces 15
publications and eight rejections.

This is correctness evidence using a trusted internal caller. It does not measure
sustained contention, mixed read/write throughput or scaling efficiency. The base
artifact in the parent directory preserves the earlier independent-create run;
this directory records the extended fixture and its source hashes separately.

```sh
./gradlew :protomolt-repo-container:nativeReplicaTest --console=plain
```

Sol reviewed the resolver barrier, receipt semantics, loser invisibility and
winner/history assertions without a remaining blocker. No production source or
protobuf contracts changed in this checkpoint.
