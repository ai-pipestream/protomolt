# Combined regression

Test source commit: `8788f7216`. Documentation commit `da1496d2d` occurred during execution; no production or test source edits.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalRetentionLoaderIT' --tests '*DocumentPublicationCommitIT.readsExactHistoricalProviderVersionAndKeepsPinsWithReturnedBatch' --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 4m 47s. 18 tests passed across 2 suites; zero failures, errors or skips. Reports and command output are archived here. This qualifies the combined private loader and read interface. Managed historical routing remains unqualified.
