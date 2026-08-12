# Encrypted shared workspace and tiered search

## Goal

ProtoMolt nodes should be able to share agent outputs, checkpoints, source
artifacts, and searchable data without requiring every node to keep a complete
local copy. The workspace is local first. A node may continue while disconnected,
then synchronize encrypted objects through the repository service when it can
reach the cluster again.

RustFS or another S3-compatible store provides durable object storage. The
repository service owns artifact references, authorization, retention, and
claim-check resolution. Object storage never becomes a shared mutable file
system.

## Storage model

Workspace content uses immutable, content-addressed objects. A versioned
manifest names the objects that form one logical workspace view:

- protobuf entities and agent-produced files;
- task checkpoints and evidence;
- descriptor sets and generated clients;
- source bundles and Git references;
- search documents, index snapshots, or index shards; and
- parent manifests needed for conflict detection and history.

Each object reference records its plaintext digest, ciphertext digest, byte
length, media type, schema identity when applicable, encryption envelope, and
provenance. The repository service verifies ciphertext integrity before making
an object available. The consuming ProtoMolt verifies the plaintext digest
after decryption.

Manifests are immutable and form an auditable history. A small mutable pointer
may identify the current manifest for a workspace. Updating that pointer uses a
compare-and-set token so concurrent writers cannot silently replace one
another.

## Encryption and membership

Encryption happens before an object leaves a ProtoMolt node.

- Each workspace has a data-encryption key.
- Each object uses an authenticated encryption mode with a unique nonce.
- The workspace key is wrapped separately for authorized members or nodes.
- Keycloak identities and cluster trust policy determine membership.
- Advertisements and S3 metadata contain key references, never key material.
- Removing a member prevents access to newly written objects. Key rotation can
  rewrite key envelopes without rewriting unchanged ciphertext.

Object names should not reveal source paths, prompts, customer identifiers, or
other sensitive metadata. Workspace manifests are encrypted when their names
or topology are sensitive.

## Synchronization

A node keeps a local journal of immutable objects and manifest updates. Sync is
idempotent:

1. upload missing ciphertext objects by digest;
2. publish the proposed manifest;
3. compare the expected parent with the repository's current pointer;
4. advance the pointer when the parent still matches; and
5. retain both branches as explicit conflict artifacts when it does not.

Version vectors or per-node cursors allow peers to request only missing
workspace events. A Merkle root allows fast agreement checks. Uploads may be
resumed by part and verified without decrypting them in the repository service.

The local cache has quotas and pinning rules. Active tasks, leases, and user
pins keep objects local. Unpinned objects may be evicted after their encrypted
copy and manifest reference are durable. Claim checks hydrate an evicted object
on demand.

## Tiered search

Search is a primary use for unloadable workspace artifacts. A node can build an
index locally, publish it through the repository service, evict the local bytes,
and hydrate it only when a query or update needs that segment.

The synchronized unit is one of:

- canonical protobuf search documents that any node can index locally;
- immutable index segments or shards; or
- a complete immutable index snapshot.

A search artifact manifest records the engine and format version, source
manifest, schema fingerprints, analyzer configuration, document count, segment
digests, and build time. A consumer rejects an incompatible artifact and may
rebuild it from the canonical documents.

Live Lucene, OpenSearch, or other mutable index directories are never merged by
file synchronization. Multiple writers publish independent immutable segments
or new snapshots. A deterministic compaction job produces a replacement
manifest. Readers may keep frequently queried segments local and claim-check
the rest from S3.

## Source code and Git

Git remains authoritative for source history. ProtoMolt can use the existing
Git APIs to create repositories, commits, branches, and reviewable changes.
Workspace storage holds content-addressed source bundles, generated files,
build outputs, and checkpoints when a Git commit is not yet appropriate.

An artifact that represents committed code records the repository identity and
full commit SHA. A disconnected agent may store a Git bundle as an encrypted
artifact, then import and push it after reconnecting. ProtoMolt does not attempt
to synchronize mutable working-tree directories through S3.

## Agent collaboration

Delegated tasks may reference a workspace manifest and a bounded writable
prefix. Workers publish checkpoints and outputs as new immutable objects. The
coordinator receives structured events that name those references, validates
required evidence, and advances the workspace manifest only after acceptance.

This gives Codex, Kimi, Cursor, and deterministic processors the same exchange
mechanism without placing raw files in control frames. Large outputs move by
claim check. Progress, ownership, and acceptance continue to use the delegation
state machine.

## Recovery and safety

The design must include:

- offline journaling and resumable upload;
- fencing for stale writers and expired leases;
- tenant and workspace authorization on every repository operation;
- quotas for local cache, remote bytes, object count, and transfer rate;
- sensitivity masking before an object is admitted to a shared workspace;
- signed provenance for agent, node, task, schema, and source commit;
- garbage collection based on reachable manifests plus a recovery window; and
- audit events for membership, key rotation, hydration, eviction, and deletion.

## Acceptance criteria

An in-process conformance scenario should prove that two nodes can create and
read an encrypted workspace, synchronize after an offline interval, detect a
conflicting manifest update, and resume an interrupted transfer. Neither the
repository service nor S3 receives plaintext or workspace keys.

A search scenario should build an index artifact, persist it, evict every local
index byte, hydrate it by claim check, answer the same query, and reject an
artifact whose engine, schema, source, or analyzer fingerprint does not match.
Tests use an in-memory repository adapter and deterministic keys. Separate
opt-in tests cover RustFS, Keycloak, and larger index snapshots.
