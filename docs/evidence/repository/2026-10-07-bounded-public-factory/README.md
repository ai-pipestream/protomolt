# Public bounded document composition

Local validation:

- `./gradlew :protomolt-repo-service:test --tests '*BoundedDocumentOptionsTest' --tests '*BoundedDocumentProfileTest' --tests '*ManagedPublicationOptionsTest' --max-workers=2 --console=plain`: seven tests passed.
- `./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`: complete production-JAR storage regression passed.

The public factory validates required collaborators and the bounded profile before
provider discovery. Invalid providers, retention/lifecycle settings, TTL and object
limits fail before the deliberately unreachable database is opened. Failed
construction does not acquire schema ownership. The options preserve existing
object and shared payload limits; transport budgets remain independent.

An out-of-package consumer compiles against the production-JAR runtime and calls
the public factory with normal provider discovery. Both library-only and RPC hosts
use real PostgreSQL, Redis and Git-backed schema resolution. Typed publication,
exact local/remote receipt replay and validated historical reads pass. Missing and
wrong operator credentials are rejected. RPC mounting follows explicit options;
legacy document, archive, drive and HTTP APIs remain unavailable.

The consumer leaves schema lifecycle with the owning host. Separate enclosing
fixtures qualify offline historical decoding and cancellation during provider and
schema work. The suite also covers Redis restart and direct delayed-write recovery.

Sol reviewed the factory, consumer and documentation. This proves the local
production runtime composition, not a new published-POM/Gradle service-metadata
gate. The full service module retains provider dependencies in its runtime graph.
Scoped-key provisioning, scheduler timing and a standalone bounded-document
launcher are not supplied by this factory. No main merge, release or deployment
is claimed by these local results.
