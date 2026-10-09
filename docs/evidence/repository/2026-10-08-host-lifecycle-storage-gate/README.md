# Host lifecycle full storage gate

Tested code: `731af65ef454a91da63bb6989d62485e7e4ac0ca`, clean
`agent/reader-host-lifecycle` checkout.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

BUILD SUCCESSFUL in 18m 14s. One aggregate test, zero failures, errors or skips;
test duration 1092.457 seconds. This runs the complete production-JAR storage
qualification harness, including the three writer-crash recovery phases and
verified cleanup of abandoned captures introduced at this checkpoint.

Service host composition (`a588919d7`), bounded hosted-document qualification
(`bad4b830e`) and reader discovery (`c9b4b6bed`) were added afterward. Separate
focused results cover those changes; this full-suite result must not be attributed
to the newer code. The evidence is stored on the composition branch for continuity.
