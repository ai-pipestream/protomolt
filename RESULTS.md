# RESULTS — publication cancellation repair

- Base: `bed8cf29de6bad75dea11372e1b7efedf35af943` (synchronized Forgejo/GitHub main at assignment creation)
- Branch: `agent/publication-cancellation-repair`
- Head: see `git log` — code change in `dc68752ae`, this document added on top
- Forgejo PR: https://git.rokkon.com/ai-pipestream/protomolt/pulls/309
- Hosted GitHub CI (workflow_dispatch on the branch): https://github.com/ai-pipestream/protomolt/actions/runs/38289935429

## Cause

`DocumentAssessmentStorageRuntimeTest.observedAssessmentAndSqlRunTogetherOnProductionJars`
failed on GitHub run 38121396523 (job 114416690422) with
`StatusRuntimeException: RESOURCE_EXHAUSTED: Publication transport is full` at
`BoundedPublicationRpcCancellationProbe.java:61` — the exact receipt replay issued
immediately after the committed call returned.

`DocumentPublicationGrpcService` admits one call per slot (`admit()`,
repo/service/.../DocumentPublicationGrpcService.java:82) and returns the slot only
when BOTH events have happened (`Call.releaseIfDone`, :147):

1. `producerFinished` — the handler method returned (finally block, :78), and
2. `terminated` — gRPC delivered the server call's terminal callback
   (`setOnCancelHandler`/`setOnCloseHandler`, :58).

The terminal callback is queued onto the server call executor after the response is
on the wire (the failing stack shows the executor hop:
`JumpToApplicationThreadServerStreamListener` → `SerializingExecutor` →
`ThreadPerTaskExecutor`). So **client receipt of the response never implies capacity
release**. The probe's replay assumed it does; under executor lag on the forwarding
runner the replay arrived at `admit()` while the committed call's slot was still
held and was legitimately refused. Base main CI passing on the same source is the
same race run with friendlier scheduling, not a slow-runner artifact.

### Lifecycle inventory (which events imply which)

- request admitted (`activeCalls++`) ⇒ slot held until release; nothing else implies release.
- client terminal callback (cancel observed by the fixture) ⇒ server `Context`
  cancellation propagates (`serverCancelled` latch) and the server terminal callback
  eventually fires; it does NOT stop the producer — the producer exits only when its
  own `control.check()` observes cancellation (worker retention contract).
- producer exit ⇒ `producerFinished`; alone it releases nothing.
- server terminal callback ⇒ `terminated`; alone it releases nothing.
- producerFinished AND terminated ⇒ exactly-once release (`released` flag): byte
  lease closed and `activeCalls--`.
- `awaitIdle()` true ⇒ all admitted calls fully released ⇒ the next admit succeeds.
  This is the documented server drainage event (also used by `RepoServices.close` →
  `ManagedDocumentServices.awaitTransportIdle` and by
  `ManagedJournaledDrainProbe.awaitTransportRelease`).
- `close()` ⇒ new admissions refused (UNAVAILABLE); accepted calls retain capacity
  and resources until they drain.
- client received response ⇒ only that `onNext`/`onCompleted` ran; does NOT imply
  the server terminal callback ran, hence does NOT imply capacity release. This was
  the fixture's invalid assumption.

## Changed behavior

**Test synchronization only; no production code changed.**

- `repo/container/src/test/resources/runtime-inventory/BoundedPublicationRpcCancellationProbe.java`
  (one assertion added): after the committed call drains, the probe waits for
  `service.awaitIdle(Duration.ofSeconds(5))` — "committed delivery returned transport
  capacity" — before issuing the exact receipt replay. This is the established
  convention (`awaitTransportRelease` in `ManagedJournaledDrainProbe`) that the
  newer cancellation probe missed at this one boundary.
- `repo/service/src/test/java/ai/protomolt/proto/repo/service/DocumentPublicationGrpcServiceTest.java`
  (new, 5 tests): drives the adapter with a controlled `ServerCallStreamObserver`
  so terminal callbacks fire only when the test delivers them (explicit barriers at
  the exact boundary). Proves:
  - response delivery does not return capacity before server termination; a replay
    in that window is refused RESOURCE_EXHAUSTED (the CI ordering, made deterministic);
    after the close callback, capacity returns and the replay is byte-identical;
  - client cancellation retains capacity while the producer is still active, and
    capacity returns when the producer actually exits;
  - duplicate terminal signals (cancel + close + close) release capacity exactly
    once (proven by capacity arithmetic: a second active call still occupies the
    only slot);
  - `close()` rejects new calls UNAVAILABLE while accepted work retains capacity and
    completes;
  - producer failure holds capacity until server termination, then releases.
  Byte-lease accounting (`PayloadBudget.reservedBytes()`) returns to zero in every path.

Contract the test now proves: the one-slot publication transport returns capacity
exactly once, only after the producer finished AND the server terminal callback was
delivered; `awaitIdle` is the only reliable capacity-free signal; receipt replay
after that drainage event is byte-identical.

## Deterministic failing baseline

A scratch test (`ProbeAssumptionDemonstrationTest`, run then deleted, not committed)
encoded the old fixture assumption — replay immediately after response receipt —
under the forced legal ordering (server close callback withheld past delivery):

```
java.lang.AssertionError: old fixture assumption fails under forced ordering:
replay refused with RESOURCE_EXHAUSTED: RESOURCE_EXHAUSTED: Publication transport is full
```

Command:
`flock -w 1800 /tmp/protomolt-repository-qualification.lock ./gradlew :protomolt-repo-service:test --tests 'ai.protomolt.proto.repo.service.ProbeAssumptionDemonstrationTest' --max-workers=2 --console=plain`
Exit code: 1 (red), matching the CI failure signature exactly. The committed suite
asserts the same ordering as the contract (green) instead of depending on transport
scheduling.

## Verification (all local runs serialized through the qualification flock)

| Suite | Command (gradlew args) | Exit | Tests | Failures | Skipped |
|---|---|---|---|---|---|
| New regression ×10 | `:protomolt-repo-service:test --tests '…DocumentPublicationGrpcServiceTest' --rerun --max-workers=2` | 0 (10/10 runs) | 5 per run | 0 | 0 |
| admissionStorageTest (full, real providers) | `:protomolt-repo-container:admissionStorageTest --rerun --max-workers=2` | 0 | 5 | 0 | 0 |
| scopedPublicationTest | `:protomolt-repo-container:scopedPublicationTest --rerun --max-workers=2` | 0 | 1 | 0 | 0 |
| repo/service transport+lifecycle subset | `:protomolt-repo-service:test --tests DocumentPublicationGrpcServiceTest --tests GrpcServerLifetimeTest --tests LifecycleShutdownTest --tests UnaryRequestAdmissionTest --tests ManagedPublicationOptionsTest --tests ManagedDocumentHostIT --rerun --max-workers=2` | 0 | 28 | 0 | 0 |

Real-provider evidence: admissionStorageTest runs the production-JAR host against
PostgreSQL 18 (testcontainers `postgres:18-alpine`), Redis 7 (`redis:7-alpine`) and
LocalStack S3; `observedAssessmentAndSqlRunTogetherOnProductionJars` (1130s) asserts
the `BOUNDED_PUBLICATION_RPC_CANCELLATION_OK` marker from the repaired probe — the
cancelled-call retention, occupied-capacity RESOURCE_EXHAUSTED, post-drainage
committed publish and byte-identical replay all executed against the real Redis PUT
gate. ScopedPublicationParityIT's persisted host log
(`repo/container/build/test-results/scopedPublicationTest/scoped-publication.log`,
44 `_OK` markers) covers the same transport under scoped authorization. XMLs:
`repo/container/build/test-results/admissionStorageTest/`, `…/scopedPublicationTest/`,
`repo/service/build/test-results/test/`.

## Status separation

- Local green: yes — table above, this checkout, head commit.
- Published branch: pushed to Forgejo `origin` first, then GitHub `github`.
- Hosted CI: GitHub Actions run 38289935429 dispatched via `workflow_dispatch` on the
  branch (branch pushes do not trigger CI; no workflow policy changed). **Pending** —
  not a completed landing. Forgejo runs no build CI by design.
- Merge: not merged; coordinator owns review and landing.

## PR308 coordination

PR308 (5769f34e7e6b45a9734034c63c55abc3189211e2) was not merged, cherry-picked or
modified. File overlap between the two branches: only root `RESULTS.md` (both add it;
trivial conflict for the coordinator). PR308 also edits sibling probe files that this
probe calls (`BoundedPublicationShutdownProbe.java`, `BoundedDocumentHostProbe.java`);
this change does not modify those files, so no textual conflict there. No constructor
or formatting churn in probe wrappers.

## Remaining limitations

- The race window itself (terminal callback queued after delivery) is inherent to
  gRPC server semantics; the contract is now pinned by the deterministic unit suite,
  and the fixture synchronizes on the documented drainage event. No production
  behavior changed, so hosts that relied on response receipt implying capacity were
  never correct and none exist in-tree.
- The deterministic suite drives the adapter directly (controlled observer); the
  end-to-end in-process transport path is covered by admissionStorageTest and the
  parity suite rather than by forced ordering through gRPC internals.
- Hosted CI result is pending at submission time; this file will not claim landing.
