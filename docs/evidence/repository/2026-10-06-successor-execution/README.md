# Successor execution SQL boundary

The focused run passed 51 PostgreSQL cases: 7 successor execution, 8 successor
installation, 7 handoff, 14 local drain and 15 admission drain. Suite XML files
are retained here. Sol reviewed the SQL, Java binding changes and test scope.

The execution tests cover the exact epoch-two binding alongside epoch one,
predecessor ownership refusal, new assessment and upload admission for the current
generation, admission drain with owner renewal, local-drain execution closure,
activation without a binding, owner expiry before activation commit, another
preparation generation, and ungranted epoch-three transfer. The preparation test
hits the live-predecessor check; a separate query proves the general admission
gate stays closed even with a valid successor execution fence.

Tests activate through SQL to exercise the database rules. No production host
activates successors yet. Current authorization, exact retry and lost commit
acknowledgment handling belong to the next Java activation step. No provider
recovery, horizontal scaling or throughput result is claimed by these tests.

Command:
`./gradlew :protomolt-repo-container:test --tests '*RepositorySuccessorExecutionIT' --tests '*RepositorySuccessorInstallIT' --tests '*RepositoryCoordinator*IT' :protomolt-repo-container:admissionStorageTest --console=plain`

The complete command passed, including the production-JAR storage and restart
regression. `runtime-final.xml.gz` records that result. This is local validation,
not hosted CI, deployment or a performance measurement.
