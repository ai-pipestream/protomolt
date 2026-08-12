# Cluster directory

The mesh cluster directory keeps node presence, processor capabilities,
capacity, endpoint posture, schema compatibility, and lease fencing in a
memory-resident projection. `ClusterDirectory` is the deterministic reducer.
It emits a gap-free `ClusterEvent` log and rebuilds the same state by replaying
that log.

`PersistentClusterDirectory` adds a durable boundary without moving hot
lookups out of memory. For every mutation it:

1. replays the current event log into a candidate directory;
2. applies and validates the mutation;
3. saves the complete candidate log; and
4. installs the candidate as the live projection only after the save succeeds.

A repository failure therefore leaves membership, event sequence, fencing
tombstones, and capacity unchanged. Reconstructing the facade loads and
replays the stored log before serving queries.

## Storage adapters

`ClusterEventRepository` is the persistence interface.
`InMemoryClusterEventRepository` supports tests and process-local use.
`RepositoryServiceClusterEventRepository` stores the event log through the
repository service raw blob API.

The remote adapter encrypts the log locally with AES-256-GCM. The encryption
tag authenticates the plaintext media type, key reference, repository drive,
and object key. The encrypted payload also records and verifies the cluster id,
cluster descriptor fingerprint, event count, plaintext SHA-256, repository
byte count, and repository SHA-256 confirmation.

Repository service, its S3-compatible backing store, and its optional Redis
cache only receive the encrypted envelope. The event fields have sensitivity
and validation annotations but no index hints. Directory lookups use the live
projection, not a search index over the replay log.

```java
ClusterEventRepository events = new RepositoryServiceClusterEventRepository(
        repositoryServiceStub,
        "protomolt",
        "cluster/primary/events.pb.enc",
        "env:PROTOMOLT_STATE_KEY",
        new EnvRepositoryStateKeyResolver());

PersistentClusterDirectory directory = new PersistentClusterDirectory(
        clusterDescriptor, clock, events);
```

One logical directory writer owns an object key. Multiple active coordinators
need fenced or compare-and-set repository writes before sharing an object key.
The repository blob API is unary, so the encrypted state codec caps plaintext
at 8 MiB.
