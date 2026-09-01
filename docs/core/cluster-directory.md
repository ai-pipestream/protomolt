# Cluster directory

The mesh cluster directory keeps node presence, processor capabilities,
capacity, endpoint posture, schema compatibility, and lease fencing in a
memory-resident projection. `ClusterDirectory` is the deterministic reducer.
It emits a gap-free `ClusterEvent` log and rebuilds the same state by replaying
that log.

`PersistentClusterDirectory` adds a durable boundary without moving hot
lookups out of memory. For every mutation it:

1. replays the retained event log into a candidate directory;
2. applies and validates the mutation;
3. saves the candidate log; and
4. installs the candidate as the live projection only after the save succeeds.

A repository failure therefore leaves membership, event sequence, fencing
tombstones, and capacity unchanged. Reconstructing the facade loads and
replays the stored directory before serving queries.

Reads take no lock. An installed projection is replaced, never mutated, so a
reader works from one volatile read of the current directory and never waits
behind a mutation's repository round trip. Sharing a lock between the two
starves reads: a monitor makes no fairness promise, and on a cluster where a
node heartbeats steadily the write path was almost always holding it. A read can
observe the state from just before a concurrent commit, which is inherent to
any snapshot of a live directory and is what `snapshot_seq` reports.

## Renewal is soft state

A heartbeat restates a fact the node regenerates every few seconds and that
expires on its own. Persisting it buys nothing a restart could not rebuild by
waiting, and it costs the entire durable write path on the cluster's most
frequent call. So presence lives in memory, emits no event, and reaches the
repository only incidentally, when a fold happens to capture it.

What fences presence is the registered epoch, and that stays durable. A
heartbeat is refused unless its `node_epoch` agrees with the registered
advertisement, so a delayed frame from a superseded incarnation cannot extend a
node's life whether or not the heartbeat history still exists. The fence never
depended on the presence events; it depended on registration, which is why
dropping them is safe.

Two consequences worth stating plainly. An accepted heartbeat does not advance
`snapshot_seq`, because nothing was recorded; `outcome` is what reports that it
landed, and `snapshot_seq` now means durable membership change specifically. And
a coordinator restart begins from whatever presence the last fold captured,
which is usually expired, so nodes are swept and re-register. That is the
recovery the fleet already performed whenever a restart outran a TTL, now on a
predictable trigger rather than an accidental one.

The same reasoning covers a lease renewal. A publisher re-sends its node,
processor, and capacity records every few seconds with nothing changed but the
timestamps and the sequence, and each of those was a durable event, which is
where the log's growth actually came from once heartbeats stopped writing. An
advertisement that differs from the registered one by nothing more than its
refresh fields now updates the lease window without recording anything. The
test for that copies the refresh fields across and asks for equality, so a
field added later counts as identity by default and a genuine change stays
durable unless someone deliberately reclassifies it.

Two consequences follow the same shape as presence. A refreshed lease that was
never recorded does not survive a restart, so the identity is swept and
re-advertised, which is the recovery the fleet already performs. And the fencing
position advances only in memory, so after a restart a delayed frame carrying a
sequence between the last durable one and the live one can be admitted. That
frame is by construction identical except for its refresh fields, so admitting
it sets a slightly older lease window that the next renewal corrects.

Because a durable mutation rebuilds its candidate by replaying the log, and the
log has never seen a heartbeat or a renewal, the live records are carried onto
that candidate explicitly. Without it every unrelated mutation would roll liveness
back to what registration armed and sweep nodes that are heartbeating normally.
A record is carried only when the rebuilt directory still holds the same identity
at the same epoch and the live record differs by nothing more than a refresh,
which is the same fence applied at the same place.

## Compaction

Registrations, processor leases, capacity, and expiries are durable, so the log
grows with membership rather than with time. Kept whole it still grows without
bound, every mutation rewrites all of it, and the 100,000-event cap eventually
refuses each mutation permanently.

`DirectoryCheckpoint` folds the prefix away. It carries the `ClusterSnapshot` at
a sequence plus the fencing tombstones, which the snapshot alone cannot express
because a tombstone outlives the identity it fences: dropping the events without
them would let a delayed frame from a superseded incarnation be admitted after a
restart. Restoring from a checkpoint and the events after it produces exactly
the directory the whole log would have produced.

Once more than `DEFAULT_RETAINED_EVENTS` events accumulate past the last
checkpoint, the next mutation folds and stores a checkpoint with an empty tail.
Sequences stay monotonic and are never reused, so a client watching
`snapshot_seq` for change is unaffected. A log stored before compaction existed
carries no checkpoint and replays from sequence one, so an existing directory
upgrades by folding on its next mutation rather than by migration.

## The wire contract

`cluster_directory_service.proto` declares `ClusterDirectoryService`, the
typed surface over that reducer: `RegisterNode`, `Heartbeat`,
`RegisterProcessor`, `UpdateCapacity`, `GetSnapshot`, and `Sweep`. The same
request messages back the `mesh-*` catalog verbs, whose published input
schemas are derived from them, so the directory presents one contract whether
it is reached as an RPC or as a verb.

Every mutating answer carries two things the caller needs in order to reason
about what happened. `DirectoryCommit` reports the event sequence the mutation
landed at, which is what makes a retry safe to distinguish from a duplicate.
`ApplyOutcome` reports whether the mutation was applied or refused, and a
refusal from a stale fencing token is a distinct outcome rather than an error:
a node that was fenced out while a request was in flight learns that its write
did not take effect, without the caller having to infer it from a failure.

## Storage adapters

`ClusterEventRepository` is the persistence interface, and its
`StoredDirectory` is what one cluster's durable state consists of: the retained
events and, once folded, the checkpoint they replay onto.
`InMemoryClusterEventRepository` supports tests and process-local use.
`RepositoryServiceClusterEventRepository` stores that through the repository
service raw blob API, under a per-call deadline. Blob RPCs with no deadline
turn an unreachable repository service into an unbounded, silent stall, so both
calls bound the wait and name the cause when it expires.

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
