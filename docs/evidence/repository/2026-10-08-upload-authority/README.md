# Upload authority callbacks

Base: `fa783d47c`. Source hashes accompany this report. Sol reviewed the callback extraction and test.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentUploadCoordinatorIT' --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 37s. 40 tests passed with zero failures, errors or skips. The added real SQL/LocalStack case injects a delivery-authority failure after provider completion and SQL verification. Memory returns to baseline; exact replay preserves the verified attempt without another PUT.

Ordinary wrappers retain shared background-failure checks between preparation operations. A test lambda initially required an explicit block to resolve Tx overloads. The authority interface is internal; historical transaction checks and source lifetimes are not qualified by this test. stageAuthorized performs transfer staging, not assessment preparation.
