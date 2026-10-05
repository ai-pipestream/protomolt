# Registry schema adapter qualification

Local verification on 2026-10-05:

```sh
./gradlew :protomolt-repo-schema-registry:check :protomolt-repo-admission:check --console=plain
./gradlew :protomolt-repo-schema-registry:generatePomFileForMavenPublication --console=plain
```

Both passed. The combined check completed in 2s; admission tests were up to date
from the prior full suite, while the changed adapter tests ran. Both production
runtime dependency gates executed. `pom.xml` is locally generated publication
metadata, not evidence of a published artifact. It contains only admission and
registry API dependencies; the runtime gate excludes Git/JGit, service/container,
SQL, Kafka and object-store SDKs.

Five tests use the real Git schema store and actual descriptor bundles. They cover
authorization on repeated selections despite cached bytes, pins across shutdown,
missing artifacts versus unavailable registry and later recovery, equal type URLs
with different definitions, isolation between registry hosts, every cold-selection
cancellation checkpoint and bounded attempt capacity.

The admission case validates a real document using a registry-resolved payload
definition and budgeted proof creation. A second candidate with malformed inner
Any bytes, but updated correct publication part hashes, is refused after schema
selection against the warm cache. The accepted proof still owns its copied assets
after both the attempt and cache close; proof closure drains its reservations.

The selector is a trusted host authorization callback, not an authentication
implementation. Source and metadata remain host-owned. Managed service mounting,
concurrent registry-load coalescing, blocked-I/O shutdown qualification, performance
and hosted CI are not established here. The adapter does not close its borrowed
store; hosts must drain active attempts before releasing provider resources.
