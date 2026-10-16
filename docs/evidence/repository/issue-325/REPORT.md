# Intent digest qualification (#325)

SaveIntents now calls MessageFingerprints.fingerprint, the existing descriptor-library helper that enables CodedOutputStream deterministic serialization. No new library or canonical rules.

The v1.pb and v1.sha256 fixture was generated with the unchanged SaveIntents.sha256 on base 40e48a627. Every top-level SaveIntent field is populated, including exact int64 provenance. The permanent test reads the frozen files without updating them and asserts binary equality, decoded equality and digest equality. A recursive descriptor guard rejects reachable map fields pending #319 cross-language vectors.

Validation: SaveIntentsTest (2 tests) and ReviewFixesIT.reorderedMapEntriesGiveTheSameContentDigestAndReplay (1 test) passed. No cross-language qualification claim. Logs: /tmp/pr325-fixture-baseline.log and /tmp/pr325-final.log.
