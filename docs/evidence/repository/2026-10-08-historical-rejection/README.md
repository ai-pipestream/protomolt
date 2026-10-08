# Historical validation rejection

Base: b86df18a0. Initial private rejection passed with real PostgreSQL and LocalStack.
Sol then identified that the wrapper attempted mutation before terminal replay.
The added same-entry retry failed with `Repository operation is terminal`, as
recorded in replay-red.xml (Gradle exit 1, 24 seconds).

After moving authorized terminal replay before mutation/capture, the strengthened
test passed (exit 0, 42 seconds; one aggregate, zero failures/errors/skips).
It also naturally expires both the claim and owner before retry. No SQL guards
are disabled and no provider response is fabricated. The rejection binds the
original assessment, and the invalid candidate creates no published revision.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Sol re-reviewed the fix without another blocker. Shared rejection-gate lock order
now starts with the exact claim when present, without requiring liveness before
authorized terminal observation. Dedicated contention, successor rejection and
historical decision fault cases remain unqualified. Public dispatch stays disabled.

The broader command below passed in 16m15s, exit 0: one aggregate test, zero
failures, errors or skips. Its report is archived in `storage/`. This run excludes
the separately qualified cancellation lock-order fix, which is integrated afterward.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```
