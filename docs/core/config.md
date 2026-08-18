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
