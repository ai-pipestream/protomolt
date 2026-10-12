# Exact-version reclamation review repair

Base: d42b158e67101e9321f456f23e85378b8dedc268 (Forgejo PR 308).

The review found two problems: uncertain provider failures discarded their causes,
and wrapper constructors silently removed an advertised capability.

`ExactVersionOutcome` now carries status, failed phase and typed diagnostic failure.
Uncertain outcomes require diagnostics. Provider exceptions remain the original
cause; permission denial during verification stays distinguishable from outage,
while the deletion outcome remains uncertain. Interruption preserves the thread
flag and CANCELLED diagnostic. If the version remains present, ABORTED records that
specific verification failure. Success results cannot carry failure diagnostics.

`OpenedBlobStore` now refuses an exact-version capability without its explicit port.
Existing service/container test wrappers and runtime probes forward the port.
There is no automatic capability removal and no new deletion activation.

Validation on the repaired sources:

- SPI check: 13 tests, zero failures/errors/skips, dependency boundaries pass.
- Full S3 check: 136 tests, zero failures/errors/skips, including real LocalStack,
  pinned RustFS and relay races. Dependency boundaries pass.
- SPI Javadoc and service/container test compilation pass.
- Container admissionRuntimeTest: 3 tests, zero failures/errors/skips. This is
  the runtime-inventory gate, not a claim that the full admissionStorageTest ran.
- Diagnostic tests assert phase, typed code and retained cause; wrapper tests
  assert missing-port refusal and identity-preserving explicit forwarding.

Commands: ./gradlew :protomolt-repo-blob-spi:check
:protomolt-repo-blob-spi:javadoc :protomolt-repo-blob-s3:check
:protomolt-repo-container:compileTestJava :protomolt-repo-service:compileTestJava
:protomolt-repo-container:admissionRuntimeTest --max-workers=2 --console=plain

The final combined command exited 0. During development one compilation failure
caught an extra argument in an unrelated recovery-constructor test; corrected
before the final run. No assertion or operation timeout was weakened.

Hosted CI from the original implementation does not validate this repair;
a new exact-head hosted run is required before landing.

Targeted service wrapper regression: 48 tests, 0 failures, 0 errors, 0 skips;
exit 0. Classes: ManagedRawDocumentIT, ManagedArchiveReadShutdownIT,
BoundedArchiveHostIT, BoundedArchiveTransportIT. Executed under the shared
qualification lock with --max-workers=2.
