# Historical dispatcher qualification checkpoint

Base: `1f322cd9a634e57262e00ba53e04fde3c6a5f07a`, with the implementation and
fixtures in the commit containing this evidence. No protobuf or migration changed.

The internal runtime factory now routes validated historical publication through
retained attempts. The normal public factories still reject historical execution.
The remote client validates requests and responses, leaving execution capability
decisions to the authenticated server. This is not a production enablement claim.

## Verified behavior

`HistoricalPublicDispatchProbe` runs the real production JARs with PostgreSQL and
LocalStack. It exercises mixed uploaded and historical data, both through the
library facade and through `RemoteDocumentPublicationRepository` with an
in-process gRPC server and authentication interceptor. Tokens and the binding
resolver are test fixtures; external identity-provider integration is not claimed.

Both paths publish valid data, retain invalid-schema rejections, validate their
response envelopes, and replay exact receipts without host selection or schema
resolution. The library path additionally loses a real committed START reply and
CREATE reply, then retries through the facade. CREATE recovery uses the retained
assessment without another schema resolution. Each operation releases its
retained generation through runtime maintenance; shutdown returns byte budgets.

The overlap probe holds the predecessor borrow through successor reservation,
checks that maintenance skips it, then holds an independent source worker while
maintenance tries retirement. Only after actual worker drain does maintenance
release the predecessor and its capacity slot, preserving successor identity.

## Commands and archives

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --tests '*HistoricalRuntimeQualificationTest.overlappingGenerations' \
  --max-workers=2 --console=plain
```

Passed in 2m4s, two aggregate tests, no failures, errors or skips. The
`initial-and-overlap` log and XML capture this run, before the additional public
lost-acknowledgement and rejection scenarios were added.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  :protomolt-repo-container:test \
  --tests '*DocumentPublicationRuntimeConfigTest' \
  --tests '*DocumentPublicationScopeCallsTest' \
  --tests '*RepositoryInitialHistoricalAttemptsIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  --max-workers=2 --console=plain
```

Passed in 1m57s: one runtime aggregate plus 28 SQL/configuration/lifecycle tests,
no failures, errors or skips. This adds public START and CREATE acknowledgement
recovery. `retries-and-regressions.log.gz` and the five named XML archives record
this run.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Final gate passed in 1m6s: one aggregate test, no failures, errors or skips. The
`public-decisions` log and XML include the lost-acknowledgement cases plus public
validation rejection through library and gRPC, with the host factory internal.

Sol reviewed routing, authority separation, accepted-call lifetime and retirement.
Its predecessor-cleanup finding was fixed and exercised by the overlap probe.

## Remaining qualification

- Cold restart after each durable reservation/installation boundary through the
  dispatcher, not just the existing private execution probes.
- Same-process takeover through the facade with an old provider worker still
  active, followed by repeated operations at the configured capacity.
- Public-boundary cancellation, revocation and cleanup-error tests, including
  mode/command changes while a local proposal is retained.
- Broader storage and ordinary publication transport regression gates for this
  checkpoint, followed by public host factory exposure and examples.

The full storage run archived at `eea9a8bb1` predates this implementation and does
not qualify these changes. RustFS performance and scale-out are separate work.
