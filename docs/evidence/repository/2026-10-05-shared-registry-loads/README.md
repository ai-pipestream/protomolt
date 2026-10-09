# Bounded concurrent registry artifact reads

The optional registry adapter shares immutable descriptor loads by digest within
one host-authenticated registry/security context. Every occurrence still invokes
its own authorized selector. Workers are host-owned virtual threads, bounded by
the configured concurrent-attempt limit. A canceled or interrupted caller abandons
only its participation; a pending provider read retains its slot until completion.
The borrowed provider must support concurrent reads and bound its I/O duration.

Four real Git store tests intercept only read timing and counting. They check:

- Two callers share one actual descriptor read while one is interrupted.
- All callers can leave a held read without freeing its capacity; shutdown wakes
  subsequent waiters and drain remains false until the provider returns.
- Different descriptor digests load concurrently and keep independent pins through
  cache close; closing both leases releases retained byte capacity.
- A real missing artifact fails both joined callers with one read; storing it then
  succeeds through a new read, with no persistent negative cache.

Five existing adapter tests cover authorization on warm hits, distinct definitions,
context isolation, outage recovery, cancellation and real candidate admission.
The cancellation fixture now uses a fixed minimum checkpoint count because an
asynchronous wait can add checks; sampling a prior run's count would be racy.

The native PostgreSQL/LocalStack/Git publication fixture also runs typed and opaque
variants against this adapter. It exercises canceled selection, exact verified
upload reuse, commit, retained history and terminal replay.

```sh
./gradlew :protomolt-repo-schema-registry:check \
  :protomolt-repo-container:test \
  --tests '*DocumentPublicationCommitIT.hostRuntimePublishesAndReplaysThenDrainsRealProviderResources' \
  --console=plain
```

Sol reviewed flight lifetime, pin transfer, cancellation, capacity and shutdown.
This is local correctness evidence, not hosted CI, managed service deployment,
registry throughput, RustFS performance or automatic claim recovery qualification.

The final command passed in 18s: nine registry tests and both native publication
variants. The attached XMLs record those results; runtime dependency checks passed
in the same invocation.
