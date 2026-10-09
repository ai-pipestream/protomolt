# Authorized owned upload preparation

The upload coordinator now offers an internal owned preparation callback with an
explicit operation authority. It shares result cleanup with ordinary publication
and enables post-preparation fences. Borrowed bytes stay inside the callback;
only an independently owned result can leave after provider drain and delivery
authorization. Failure closes that result and preserves cleanup errors as suppressed
exceptions on the original failure.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentUploadCoordinatorIT.ownedPreparationTransfersOnlyAfterPostChecksAndDraining' --max-workers=2 --console=plain
```

Passed: nine cases, zero failures, errors or skips; 20-second build. The test uses
real PostgreSQL and LocalStack storage. Both ordinary and explicit-authority routes
exercise success, cancellation, owner expiry and cleanup failure. An additional
explicit-authority case refuses delivery after actual provider drain. Each case
checks one verified upload, exactly one owned-result close, and zero leaked byte
reservations. The owned result is a test resource that injects cleanup failures;
the storage effects are real.

Sol reviewed the implementation and test scope. This is the callback required by
historical assessment composition, not an enabled public historical entry point.
Parent assessment capture, registry transfer, cold routing and public transport
qualification remain open. No protobuf changed.
