# Journaled runtime shutdown composition

Base: `5513712770c7b4d6dc623ec9c8446263951700db`, plus the accompanying changes.

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`

Passed in 2 minutes 39 seconds. The compressed XML preserves the production-JAR
test output. The native runtime probe exercises twelve scenarios through actual
PostgreSQL and versioned LocalStack storage: the original six ordinary-runtime
cases and six private journaled counterparts. These cover successful publication
and exact replay, observed-runtime startup refusal, typed rejection, lost stage
and decision acknowledgments, stage rollback, and shutdown during schema resolution.

The journaled runtime stores supplied process authority for shutdown independently
of request ownership. Retained rollback registrations and active calls receive V90
markers; terminal entries already evicted after durable outcomes do not. A cancelled
shutdown attempt creates no marker, and a subsequent attempt succeeds. Every scenario
finishes with no outstanding read pins or payload reservations, and the borrowed
database remains usable.

Shutdown during validation refuses a new assessment start with PostgreSQL SQLState
P0001 and the coordinator-admission error. The probe checks that no start, stage or
rejection receipt was inserted. This differs deliberately from the ordinary runtime's
semantic rejection. The successful journaled case uses a distinct destination and
an explicit absent-destination precondition. Its first fixture incorrectly retained
an existing-revision precondition and was refused before uploads; that fixture was
corrected without changing revision enforcement.

The factory is package-private; public constructors and managed-service activation
remain unchanged. This qualifies runtime composition, not durable LOCAL_DRAINED,
automatic takeover or late remote-effect quiescence. Service-owned schema provider
workers still require their separate shutdown protocol. No throughput, hosted CI,
merge or deployment claim is made.

The final source also passed 38 focused configuration, journaled-session and SQL
drain tests in 34 seconds:

`./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationRuntimeConfigTest' --tests '*DocumentJournaledSessionsIT' --tests '*RepositoryCoordinatorDrainIT' --console=plain`

Sol reviewed the implementation, corrected fixture and documentation with no
remaining blocker. Attached focused XML covers registration races, retained
uncertainty, claim transfer and cancellation alongside the runtime configuration.
