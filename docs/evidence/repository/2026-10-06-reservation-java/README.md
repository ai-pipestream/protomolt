# Kind-bound Java reservation and attachment

Private recovery proposals now distinguish graceful and expired-unquiesced
reservations. The expired proposal requires the previous owner generation/nonce.
Installation checks that tuple against retained preparation; the manager retry
fingerprint includes the complete variant. Install, activation and attachment
confirm the kind-specific immutable reservation. Public protobufs and host startup
are unchanged.

On 2026-10-06 this command passed 108 tests with zero failures, errors or skips in
1m 25s:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinator*IT' --tests '*RepositorySuccessor*IT' --tests '*DocumentSuccessor*IT' --console=plain
```

Compressed XML files record individual results. The seven new Java/SQL cases cover:

- Expired reservation, exact live successor claim, retained predecessor loading,
  V93 install, V94 activation and attachment.
- Both directions of recovery-kind substitution refused, plus owner mismatch and
  distinct manager fingerprints.
- Actual JDBC lost commit acknowledgment followed by exact confirmation, with no
  lease renewal.
- Cancellation after commit reported to the caller, with later exact confirmation.
- Confirmation after expiry grants no execution; wrong private authority and a
  changed previous owner are rejected.
- Concurrent identical proposals confirm the same committed reservation; conflicting
  proposals have one winner and leave one source record.

Sol reviewed the implementation and final tests without a blocking finding.
Graceful confirmation dispatches directly to canonical V92 evidence; it cannot
confirm an expired source. This is explicit kind dispatch, not an exception-based
fallback for missing data. Existing migration fixtures still construct old graceful
states before upgrading.

This qualifies private Java/SQL attachment, not automatic host recovery, real
process termination, provider publication through the new uncertain path or RustFS
performance. Recovery after a replacement dies before V94 is still separate work.

The separate `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed as well. `storage-runtime.xml.gz` contains its production-JAR integration
result, including the existing graceful provider publication and schema-retention
checks after this Java refactor. It does not add uncertain-provider recovery proof.
These are local results, not hosted CI, merge or deployment evidence.
