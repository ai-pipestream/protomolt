# Distributed config

`protomolt-config` is the consumer side of
[config distribution](../design/config-distribution.md): typed protobuf
config documents behind a pluggable `ConfigSource`, applied
verify-then-swap.

A node subscribes a subject as a message type and refreshes on its own
cadence (the host owns the timer, the reader-refresh idiom). Each
refresh fetches the subject's current versioned payload, parses it
strictly as the declared type, enforces the type's own declared
validate.v1 rules — the same enforcement the wire doors mount, applied
before anything applies — and only then swaps the current config
atomically and notifies listeners. A document that fails any step is
refused with the reason, and the node keeps serving the config it
already has; a gap in the source is not a removal. The applied version
(a git commit, a topic offset) rides the subscription and the refresh
outcome, so a node can always say which config it runs.

`ConfigSource` is deliberately just a reader: one subject, at most one
versioned payload, no watches and no coordination. The registry plug
reads the git-backed registry; the Kafka plug reads a compacted topic
through the house serde with validation on; a test hands out a map.
Every node is a reader — there are no coordinator nodes, and the writer
is whoever publishes to the source.

## The registry plug

`protomolt-config-registry` reads config documents from the git-backed
registry over its native HTTP surface (`GET/PUT
{prefix}/configs/{name}`, `GET {prefix}/configs`). A document is an
envelope — `{"messageType": "...", "config": {...}}` — whose config is
proto3 JSON of a type the registry already serves: reviewable in git
the way GitOps expects, typed the way protomolt expects. The registry
is the writer's door: a put resolves the type against the registered
schemas (an unregistered type refuses naming it), parses strictly, and
runs the type's own declared rules, so an invalid document never
reaches Git — and the read side re-gates, so even a hand-edited
repository or a bad federation merge serves a refusal an operator can
read, never an invalid document. The version is the commit that last
touched the document, per document, and the served payload is the typed
message's bytes ready for the consumer.

## The Kafka plug

`protomolt-config-kafka` reads a compacted topic as a table: the key
is the house convention, a deterministic name-based UUID over the
subject (the same key on every publish, exactly the identity
compaction needs), the subject itself rides the
`protomolt-config-subject` record header (verified on read: a key
that does not derive from the header subject refuses), and the value
is the typed message through the house serde against the
registry, `validate.on.read` forced on — the config lane never reads an
unvalidated document. Publishers write with the same serde and
`validate.on.write`, so a document violating the type's declared rules
cannot even serialize; a poisoned record smuggled past the writer gate
refuses at read with its `partition:offset` coordinates, and the
consumer keeps serving what it runs until the next honest publish heals
the subject. No consumer group and no membership: the source assigns
the partitions, reads from the beginning, keeps latest-per-subject
itself (compaction lag is invisible), and treats a tombstone as the
source no longer offering a document — which the consumer's
absence-is-not-removal rule turns into "keep serving, note it". The
record's `partition:offset` is the applied version, the evidence.
