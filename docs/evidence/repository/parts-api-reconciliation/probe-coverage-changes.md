# Probe coverage after removing publication transport

The separate complete-replacement publication RPC and its remote client are removed.
These changes retain the internal publication library, real SQL/provider work,
validation, receipt, authorization, historical recovery and cancellation coverage.
They do not establish that DocumentService now performs typed admission.

## Library cases retained

- ScopedPublicationProbe runs each library scenario, including typed/opaque
  publication, exact credential binding, revocation, expiration, atomic mixed
  batches, exact retries, receipt delivery authorization, concurrency, and all
  three final-check barrier orderings. Its driver requires library markers only.
- HistoricalPublicDispatchProbe retains library CREATE/START/publication lost
  acknowledgements, rejection, takeover, cancellation, shutdown, cleanup and SQL
  cleanup failures, authorization revocation and commit-winner orderings.
- HistoricalPublicTakeoverProbe retains producer lifetime and library recovery.
- ManagedJournaledDrainProbe retains schema-worker cancellation/drain and expired
  operation recovery. Fresh library publication and replay assert valid receipts,
  incomplete-input rejection and scoped credential revocation.
- BoundedPublicationShutdownProbe retains its real provider PUT and schema worker
  lifetime modes.

## Coverage removed with the removed surface

- Scoped publication gRPC duplicates, missing/forged token cases, resolver outage
  and transport identity-mapper substitution. Library identity substitution and
  durable credential/grant cases remain.
- Historical publication gRPC duplicates, remote deadline/cancellation/transport
  close and wire status mapping checks. Library cancellation/shutdown remain.
- ManagedJournaledDrain's scenario 1 tested transport acceptance keeping an RPC
  alive during host shutdown. That transport-specific scenario is removed rather
  than replaced by a weaker library simulation.
- Publication client/server receipt corruption checks, gRPC sanitized errors,
  transport request-size enforcement and transport concurrency/byte permits.
- BoundedPublicationRpcCancellationProbe is deleted; it only exercised the removed
  RPC adapter. Its source compilation entry and expected success marker are removed.

History/materialization gRPC APIs are not removed and their transport tests remain.
Prior evidence with GRPC/parity markers describes the former API, not the current
qualification target. No test is silently skipped to accommodate the change.

## Verification status

Probe compilation and focused runtime results are recorded below and in README.md.
Compilation is not runtime qualification. The first compilation attempt used an
incomplete host classpath and exposed an accidentally removed library grant
revocation method; the method was restored before the next compile.

All runtime-inventory Java sources compile together with the container's real
assessmentTransportRuntime JAR classpath after the corrections (javac exit 0).
Log: /tmp/protomolt-reconciled-probe-javac.log. This includes the preserved history
and materialization transport probes. The bounded public consumer RPC markers
still represent real history RPC reads and remain required in the driver.

The first coordinator runtime invocation completed the scoped probe but failed
its driver's obsolete expected marker count (44 versus the retained 22). The
count and method name now describe library coverage. A targeted rerun was required;
that invocation is not reported as a passing test.

Final coordinator verification: scoped library driver passed with 22 markers;
all four selected storage-host tests passed after correcting managed recovery
startup. See README.md for the failed-attempt history and final results.
