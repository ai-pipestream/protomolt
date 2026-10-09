# Mixed historical, current and uploaded publication

The production-JAR storage harness runs a command with three separate typed members:
fresh upload, current reuse and historical reuse. It uses real PostgreSQL and
versioned LocalStack S3. Fresh bytes are written through `DocumentPartTransfer`;
retained bytes are read from their exact provider versions. Only current and fresh
members may invoke ordinary schema resolution.

With the fresh upload left unverified, publication is refused. All destinations
remain absent, no schema admission is published, and replay remains pending.
With the provider observations verified, all three members publish atomically and
replay returns the exact durable result. SQL assertions bind fresh physical objects
to the selected attempt and observed provider version; reuse retains the declared
physical UUID and provider version. Manifest keys, hashes and producer provenance
are also checked. The source head stays unchanged and reservations and pins drain.

Current and historical reuse deliberately select the same bytes here. The separate
multi-revision SQL test covers differing versions of one source slot. This probe
does not qualify mixed source kinds within one member, public restore sessions,
claimed recovery or performance. Sol reviewed the negative case and the strengthened
physical-origin assertions without finding a blocker.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The harness passed, including required `HISTORICAL_MIXED_UPLOAD_OK` and
`HISTORICAL_UNVERIFIED_UPLOAD_REFUSED_OK` markers. Log:
`/tmp/protomolt-historical-mixed-upload-qualified.log`. Hosted CI, merge and deployment
are separate from this local verification.
