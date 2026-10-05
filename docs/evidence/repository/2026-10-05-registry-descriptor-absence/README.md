# Descriptor absence versus registry failure

Local verification on 2026-10-05. Before the fix, three real-filesystem tests failed:
a moved repository, directory at the artifact path and file at its parent path all
returned an empty descriptor lookup. `red.xml` preserves those failures.

Git descriptor lookup now checks repository availability and distinguishes absent
paths from invalid path shapes and I/O failure. Genuine absence in an available
repository remains empty. The shared descriptor reader opens the final component
without following symbolic links. This does not establish race-free traversal of
an untrusted or concurrently replaced ancestor directory.

```sh
./gradlew :protomolt-registry:check :protomolt-schema-registry-git:check --console=plain
```

The final command passed in 5s. `green.xml` records the full Git store test class,
including a fourth regression covering dangling artifact links and missing Git
metadata. Core and Git module checks passed together. Invalid descriptor contents
retain their existing validation exceptions; those are errors, not absence.

This is a prerequisite for accurate cached registry resolution, not evidence of
cache integration, complete registry filesystem hardening, provider performance or
hosted CI. No registry outage is intentionally converted into opaque admission.
