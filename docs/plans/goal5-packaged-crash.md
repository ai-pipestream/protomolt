# Packaged authoring crash qualification

`AuthoringComposeCrashTest` exercises the actual Compose coordinator, fixture,
repository-service, RustFS, and PostgreSQL stores. Both image inputs are required
when the test is enabled:

```sh
PROTOMOLT_AUTHORING_CRASH_IMAGE=registry/image@sha256:... \
PROTOMOLT_REPO_CRASH_IMAGE=registry/repo@sha256:... \
  ./gradlew :samples:test --tests '*AuthoringComposeCrashTest' --rerun
```

Full digest references or explicit local Docker content IDs are accepted; tags
are rejected. Images must already be present. Both absent means an opt-in skip;
one absent is a failure. Native release jobs supply published image digests and
check the JUnit report for exactly one passing test with no skips.

Each run owns a unique Compose project. A test-only override disables automatic
restart for the coordinator and fixture and exposes PostgreSQL only on loopback.
The test starts the scripted author through the browser API, awaits independent
acceptance, and binds the submitted job source to the accepted preparation.

A PostgreSQL advisory lock blocks the write checkpoint transaction. The test
requires both the durable external fixture record and the blocked transaction
before sending SIGKILL to the coordinator. It verifies attempt 1 retained only
the normalize checkpoint, then kills and restarts the fixture on the same volume.
It expires the dead coordinator lease by targeted SQL to accelerate recovery;
this is not evidence of waiting for the natural lease timeout.

The restarted coordinator must complete attempt 2 with the same workflow source,
an unchanged first checkpoint, the expected output, the exact outbox sequence
and attempt identities, and an unchanged durable fixture record. Cleanup removes
only the test project's containers and volumes.

Local AMD64 inputs built from `a4adb706588417356d43343dca50eea02553220c`:

- Authoring: `sha256:79cadb2d47297366623052f91d109511d3faa7dfd11f753014f92e05e503458d`
- Repository: `sha256:e542069cb67e4add2c0b69e221ad684d3d13390f418fe4d4116f8a6a048804ba`

These local image inputs do not establish publication, anonymous pull, ARM64,
live-model behavior, or NAS deployment. Native publication jobs must execute
the same gate against the images they publish before assembling the bundle.

The integrated source `67517e0613e07c32306b677d18174de9a3a40b68` also passed
this gate locally on AMD64 on 2026-10-01: one test, no skips, failures or errors
(33.363 seconds). This source includes the expired-lease retry limit and the
installed-process crash test. Its local image inputs were:

- Authoring: `sha256:b1795737d28dee961bb141ff39801e00a8c0592e35ca838d4694a5cdff90ae4e`
- Repository: `sha256:0cd1a39f24a601962a2f8e575337f1b9e95e394982fa1afe2b83f3ea82879040`

This is local package evidence only. The native publication gates must run
again against the exact merged source and published images.
