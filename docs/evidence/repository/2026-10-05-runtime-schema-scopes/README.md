# Runtime-owned schema resolution and verified upload retry

The native Java publication runtime owns lazy per-member resolution scopes through
admission and publication. Scope cleanup precedes release of the runtime's outer
call token; shutdown waits for those tokens before releasing upload/read resources.
The existing callback entry remains explicitly borrowed. No transport contract or
managed-service configuration is added by this change.

The real PostgreSQL/LocalStack/Git fixture cancels immediately after registry
resolution. It verifies release of the registry's one-attempt capacity, retries
the same command, publishes successfully and reads retained history through local,
in-process and Netty paths. Opaque execution and terminal replay open no scopes.

That cancellation test initially exposed duplicate upload-attempt insertion on
retry. `retry-red.xml` preserves the actual failure. The fix adds a distinct reuse
path for exact, fully VERIFIED initial selections under current owner, command,
ACL, source, drive and backend checks. It compares the complete initial selection
set, locks attempts in deterministic order, checks token/placement/plan identity,
current revision-1 selection, live database lease and absence of cleanup. It skips
provider workers and observation flushing rather than re-PUTting immutable content.
Original low-level admission remains insert-only. STAGING or uncertain uploads,
replacement selections and expired attempts still require separate recovery.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationScopeCallsTest' \
  --tests '*DocumentUploadCoordinatorIT' \
  --tests '*DocumentOperationUploadAdmissionIT' \
  --tests '*DocumentPublicationCommitIT.hostRuntimePublishesAndReplaysThenDrainsRealProviderResources' \
  :protomolt-repo-schema-registry:check --console=plain
```

The final affected run passed in 39s, with 131 container tests across four suites.
Adapter checks also passed; its unchanged test results were up to date. The attached
green XMLs record the actual affected run. Provider interception counts real PUT
calls and proves the verified retry does not increase them. Negative cases assert
specific fences for STAGING, wrong token, real lease expiry, displaced selection
and changed canonical command. The expiry case waits for PostgreSQL's clock; it
does not shorten a protected lease. Lifecycle tests hold cleanup at a latch and
prove quiescence remains false, plus preserve suppressed cleanup failures.

These are correctness and resource-lifetime results, not throughput measurements,
automatic takeover qualification, full recovery coverage or hosted CI.

The separate production-JAR gate also passed in 2m 35s:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

`provider-green.xml` records that real-provider run. It supplements the 131-test
run above; it does not establish RustFS throughput or automatic claim recovery.
