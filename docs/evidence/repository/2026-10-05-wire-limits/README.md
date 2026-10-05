# Wire resource-limit classification

Local validation on 2026-10-05:

```sh
./gradlew :protomolt-descriptors:test :protomolt-repo-admission:test --console=plain
./gradlew :protomolt-repo-codec:test :protomolt-repo-spi:test --console=plain
```

Final results: descriptors 97 tests, admission 169, codec 51, repository SPI 31;
all passed with zero failures, errors or skips. New serialized fixtures separate
configured depth/value exhaustion from malformed/truncated wire data, preserve
cancellation identity, and test exact limits and aggregate accounting. Existing
codec fixtures now assert the dedicated limit exception. The first codec run
failed its old assertion that depth exhaustion was malformed data; that assertion
was updated to the intended resource-limit classification before the final run.

`results.tar.gz` contains the final XML results; source hashes cover the changed
Java files. This adds the scanner failure type and regression coverage, not an
optional Any reader, resource-error mapping for every adapter or a public RPC.
Historical adapters still need their own explicit retained-data and resource
outcome mapping when the new materialization boundary is integrated.
