# Scoped successor execution qualification

Base: `016cb4417f523b1bc256183982d1038ba8e917cf`.

```sh
./gradlew :protomolt-repo-container:test --tests '*ScopedRepositorySuccessorIT' --console=plain
```

Five parameterized cases passed against PostgreSQL 18, with no skips. Results are
in `activation-green.tar.gz`. Each installs a creation grant before the initial
claim, uses the real handoff/install protocol, then supplies the scoped execution
caller and configured drive gate to successor activation and attachment.

- Live grant: activation and attachment succeed; the new owner is generation two
  and real SQL upload admission creates an attempt.
- Grant revoked before activation: no successor execution record is committed.
- A different live key for the same principal: no successor execution record.
- Host rejects the selected backend: no successor execution record.
- Grant revoked after activation: immutable activation confirmation still succeeds,
  but attachment refuses execution authority.

Process authority is confined to fixture setup and coordinator actions. It is not
substituted for the scoped execution caller. The upload metadata is a synthetic SQL
fixture; this test does not upload bytes or claim recovered provider publication.
Sol reviewed the test without finding a blocker. Manager-level successor activation,
full recovered publication, and concurrent commit/revocation remain to be tested.
