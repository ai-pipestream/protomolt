# Archive version metadata snapshots

Baseline: `6dfe451703cbb06d6df4133d3aa95300236b8ff9`. This implements the descriptive
metadata part of the repository composition design. Archive schema closure,
admission identity and source ownership bindings remain separate required work.

## Operation inventory and contract

- Extend VersionManifest with optional EntryInfo `metadata_snapshot`, tag 7.
  Existing names, tags, imports, Any URLs and content checksums stay unchanged.
- Extend newly created versions from unary put, managed/streaming upload and
  bridge generation to capture the final entry metadata in the same SQL commit
  as the manifest. Use the final version number and timestamps.
- GetEntry, GetEntryManifest, ListVersions and manifest-bearing ListEntries return
  that stored snapshot as part of the manifest. Their existing `info` is current
  metadata; never reinterpret that field as historical metadata.
- Equal-content put and classification updates continue changing current labels
  without a new content version. They do not rewrite any version snapshot.
- Legacy manifests without snapshots remain explicitly unknown by field absence.
  No migration or read path may fill them from current EntryInfo.

The reused EntryInfo has contextual semantics: inside a snapshot, current_version
means the entry's version at capture and equals the containing manifest version.
The snapshot address must equal the manifest address; the UUID must be present
and syntactically valid. Runtime annotations enforce these correspondences.
The address-derived UUID is checked by the implementation, not computed in CEL.
Current authorization continues governing access; the snapshot grants no rights.

This is an immutable descriptive snapshot, not a complete admission/ownership
record, a restore operation or a JCR frozen node. It neither changes repository
transaction boundaries nor adds JCR dependencies. Review against
[the JCR gap assessment](repository-jcr-compatibility.md) found no new capability
claim or incompatible foundation primitive.

## Persistence and bounds

The snapshot is stored in existing manifest JSONB, atomically with the version.
A migration must prevent changing, removing or retroactively adding snapshots on
existing version rows, while permitting the existing rendition-state mutations.
It must preserve existing rows unchanged and retain ordinary version deletion.
Legacy rows without snapshots keep their existing owner-move behavior. New
snapshots also freeze their containing row and manifest identity. The insert
guard checks the parent entry address at capture; this is not a permanent
foreign-key constraint on directly modified parent address fields.

Root checksum remains a rendition-content identity; metadata is outside that
checksum. Existing whole-manifest JSON and complete-response allowances include
the new data. An oversized historical response is refused, never silently stripped
of its snapshot. Test read and version-list boundaries with snapshots present.

Protobuf compatibility does not imply persisted-JSON compatibility. Older binaries
use a strict protobuf JSON parser and can reject metadataSnapshot. This change
requires coordinated deployment without old readers/writers; no mixed-version
rollout or downgrade safety is claimed. No deployment is part of this checkpoint.

## Acceptance

Compile full imports, lint affected protos and compare against the baseline.
Exercise generated and dynamic messages with the real runtime validator: valid
snapshot, legacy absence, mismatched address/version, absent UUID and invalid UUID.
Then verify all three creation paths against real SQL/provider storage, immutable
v1 metadata after current edits and v2, unchanged-byte reuse, and current-vs-frozen
classification. Apply the migration with existing rows and prove legacy absence
survives reads and rendition updates, while direct snapshot rewrites are refused.
Run response-boundary tests and relevant archive deletion/pruning regressions.

Sol reviewed the contract shape, writer ordering, bounds and rollout caveat before
implementation, then reviewed the final diff and corrected SQL guard. The
[qualification evidence](../evidence/repository/2026-10-07-archive-metadata/README.md)
records 89 passing tests, including populated migration, legacy move, bridge,
classification, streaming, and snapshot-bearing response bounds. This descriptive
snapshot checkpoint does not close the broader provenance or restoration work.
