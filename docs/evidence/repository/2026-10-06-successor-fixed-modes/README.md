# Preserve fixed modes across successor generations

Base: `8f92f8cb5fa836f5458de1bc1a82ac3830ba98b3`.

The regression test initially failed because an altered mode map installed
successfully under the same operation. `red.tar.gz` records that one expected
failure. V101 now compares each proposed successor's mode hash with its exact
predecessor generation and owner nonce. The existing install trigger takes claim
then owner locks before the new guard reads the immutable predecessor mode row.
The new generation cannot change TYPED to OPAQUE or the reverse. Current policy
still applies; a different mode requires a new operation.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositorySuccessorInstallIT' --tests '*RepositoryCoordinatorSupersessionIT' --tests '*DocumentPublicationProcessRecoveryIT' --console=plain
```

All 39 affected cases passed without skips: 9 install, 18 supersession and 12
process recovery cases. `affected-green.tar.gz` contains the results. The mode
change regression checks rollback of the owner, install and new journal rows,
then installs the original unchanged plan successfully. The additional journal-row
assertions were added after this affected run and are exercised by the install
regression below.

The migration checks existing installations before adding the trigger; it fails
on absent or inconsistent predecessor modes rather than silently repairing
history. Sol reviewed the invariant, exact SQL binding and lock order with no
blocker. This closes the mode-change gap; it does not complete managed recovery
proposal retention or automatic scheduling.

The final install/upgrade and production-JAR checks also passed:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositorySuccessorInstallIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

`upgrade-packaged-green.tar.gz` contains 11 passing install/upgrade cases and one
passing packaged-runtime case, with no skips. The upgrade fixtures install real
generation-two records at V100: unchanged modes survive V101 with identical owner
state, while a mode change allowed by V100 causes V101 to fail explicitly. These
fixtures do not bypass database guards. Local checks do not establish hosted CI,
merge or deployment.
