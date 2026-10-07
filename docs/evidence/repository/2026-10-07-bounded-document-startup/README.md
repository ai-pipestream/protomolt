# Internal Redis document host

The profile is package-private. No public builder is available.

9 configuration tests passed (profile.log). The production-JAR suite passed in
3m39s (retained-history.log and XML), using:
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
Sol reviewed the assembly and fixture with no blocker.

Real PostgreSQL, Redis and Git resources exercise typed Any publication, exact
receipt replay and historical validation after the live resolver closes.
Redis uses AOF, always-fsync and no eviction. Provider selection fails on S3.
The fixture checks API restrictions, drive seeding and resource cleanup.

Still required: network parity, restart durability, multiple revisions, invalid
candidates, write failures and shutdown during active calls. Replay equality does
not prove zero provider I/O. This test makes no performance claim.
