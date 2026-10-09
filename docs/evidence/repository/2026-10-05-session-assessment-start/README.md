# Journaled session assessment start

Assessment execution now asks its session for stage coordinates. A journaled
session checks caller and immutable owner/claim identity, sets its local sticky
state, then commits the existing V83 marker before CREATE. It uses the returned
database assessment UUID and deadline. Ordinary unjournaled execution keeps its
existing local coordinates.

Real PostgreSQL tests cover successful marker creation, lost commit acknowledgment
and cancellation after commit. The journal remains readable with the original
coordinates; a new execution scope stays marked started, and another start is
refused. Wrong owner/claim and cancellation before journal I/O leave local staging
clear. The bare local start method cannot bypass a journaled session's marker.

Validation:

- `:protomolt-repo-container:test --tests '*DocumentPublicationSessionIT'`: 16
  cases, including pre-start refusal and registration acknowledgment checks.
- `:protomolt-repo-container:admissionStorageTest`: actual PostgreSQL/provider
  regression gate, including existing committed-stage recovery scenarios.

No protobuf contracts changed. This integrates the private session path but does
not enable it in ordinary runtime creation. A full registered-session provider
execution test, scoped host/journal authority separation, denial retention policy
and successor qualification remain open. An uncertain marker without a committed
assessment does not authorize reconstructing a new stage from replacement input.
