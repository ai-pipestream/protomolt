# Host composition storage regression

Tested checkout: `agent/host-reader-composition` at
`92d01c2e4616eaa1a4a9c1ea33fe9e44cbbe2f82` (code `750bdca53`).

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Passed in 18m22s. The XML records two aggregate tests, zero failures, errors or
skips, and 1099.985 seconds. The archive contains the Gradle log and test XML.
This qualifies the host composition, hosted bounded document consumer and exact
reader cleanup checkpoint. It does not include the later reader recovery
supervisor or retention inventory changes.
