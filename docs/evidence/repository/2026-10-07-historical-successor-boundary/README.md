# Historical successor boundary

Base `d40e9bee72e037d8659c5597b7abfa453c1c9a8b`, plus the tested source
fingerprint retained here, on `refactor/repository-composition`.

The regression registers a historical preparation and admits its original owner,
waits for the database leases to expire, reserves a successor through the actual
expired-unquiesced protocol, then commits successor installation. No guard is
disabled and no reservation, activation or capture row is fabricated.

Ordinary successor activation refuses the historical command with
`UnsupportedOperationException`. There is no successor execution grant or epoch-2
coordinator binding. Only the original capture batch exists; original retained
roots remain EXACT and the successor preparation's root coverage is UNKNOWN.
This is a current boundary test to refine when private historical activation is
implemented, not a claim that successor historical execution is available.

PostgreSQL 18 supplies the actual ledger transitions. The source document fixture
uses synthetic provider observations; this test proves no provider behavior.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCaptureAdmissionClosureIT' --max-workers=2 --console=plain
```

Final result: exit 0 in 19 seconds; 12 tests, zero failures/errors/skips. The first
run expected the wrong exception subtype; the final assertion checks the actual
deliberate unsupported-operation boundary. Retained XML trailing whitespace is
normalized. The historical restore design now specifies atomic activation and
capture registration, the retained original source set and independent completion
for each owner's readers. No production or protobuf change in this checkpoint.

Sol reviewed the test and identified a design requirement: activation must retain
its original source-set and capture association durably, not only in a host object.
The design now requires an immutable activation sidecar, ancestry validation,
exact lost-reply confirmation and fresh captures after process restart.
