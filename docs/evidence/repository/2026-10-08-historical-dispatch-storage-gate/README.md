# Historical dispatcher storage regression

Production checkpoint: `d0c886a5d61837ccb4fd78a6754cd0a0a4bf9d49`, clean checkout
`historical-publication-dispatch`. The later cold-dispatch and concurrent takeover
changes are test-only and are qualified separately.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest \
  :protomolt-repo-container:scopedPublicationTest --max-workers=2 --console=plain
```

Passed in 19m33s. Storage: two aggregate tests, 1103.362 seconds. Scoped publication:
one aggregate test, 67.205 seconds. Both XML reports have zero failures, errors and
skips. These aggregates execute the real-provider production-JAR harnesses; their
JUnit counts are not individual scenario counts. Compressed command output and
the two XML reports are retained alongside this file.

This is local correctness and regression evidence, not hosted CI, network load,
RustFS performance or horizontal scaling qualification.
