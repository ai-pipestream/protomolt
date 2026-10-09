# Managed historical cold recovery

The full managed host recovers historical publication after a writer process exits
at initial capture, successor reservation, or successor installation. The recovery
process constructs `RepoServices.buildHosted` against real PostgreSQL and LocalStack
S3. The writer uses the internal runtime; this does not qualify a hosted writer crash.
Only caller intent and upload bytes cross the process handoff. Credentials are
independently injected test bindings, not a production identity-provider claim.

The first run reached the public authenticated boundary and failed all three phases
with `RESOURCE_EXHAUSTED`. Preparation retained a fixed 16 MiB lease while activation
required another 53 MiB and the request retained its own allowance. That exceeded the
unchanged 64 MiB host budget even for tiny documents. Preparation now holds temporary
scratch and retains only the next plan's encoded size. Install acknowledgement loss
still retains the exact plan and its lease until retry or cleanup.

Two subsequent fixture corrections were required: seed the installed container
definition, including its type URL and provenance, and expect both document and
archive readers in the full host. Schema identity checks were not weakened.

## Checks

- Three managed cold phases: authenticated gRPC commit, exact library/gRPC replay,
  persisted receipt equality, real provider readback, retained physical identities,
  fresh-upload schema resolution only, and retained-schema validation.
- Unauthenticated recovery is refused before schema resolution.
- Both managed readers quiesce with local drain evidence and the host is fenced.
- The original writer's orphan capture is reclaimed using supervisor evidence.
- Lost reservation/install acknowledgement tests retain small reservations, preserve
  exact retry identity and accounting, and release all budget on cleanup.
- All earlier private/public cold phases and corrupt-preparation refusal still pass.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalPreparationIT' \
  --tests '*RepositoryHistoricalGenerationLimitsIT' \
  :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.cold*ProcessRestart' \
  --tests '*HistoricalRuntimeQualificationTest.coldPublicCorruptPreparation' \
  --max-workers=2 --console=plain
```

Passed in 7m53s: 31 preparation cases, 3 generation-limit cases and 10 cold-process
cases; zero failures, errors or skips. XML is archived alongside the initial red
capacity run. Sol reviewed lease ownership, uncertain acknowledgement retry, the
fixture corrections and shutdown assertions without outstanding findings.

This is correctness evidence for small requests, not maximum-payload, concurrent
throughput, RustFS performance, horizontal scaling, multi-backend routing or
deployment evidence. Unavailable retained backends and shutdown while hosted
historical work is held still need service-level qualification.
