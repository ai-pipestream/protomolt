# Full storage regression with START reconciliation

Tested commit: `eea9a8bb183d4b0523d0e5dbbe817a71ad34f7eb` on
`agent/reader-recovery-supervisor`.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Passed in 18m23s: two aggregate tests, zero failures, errors or skips; XML suite
time 1101.240 seconds. The archive includes the Gradle log and test XML. This
qualifies the reader recovery supervisor, root inventory and original-handle START
reconciliation at that checkpoint. The production-JAR probe requires successful
CREATE after a lost START reply and refusal of a competing handle.

This run predates `8d7b217be` and `434c21986`. It does not qualify their authorized
owned callback or composed historical assessment inputs, nor the later selected
generation lookup. Their narrower evidence is recorded separately. Public
historical dispatch remains gated.
