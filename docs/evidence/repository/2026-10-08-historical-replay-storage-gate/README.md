# Historical replay checkpoint storage regression

Tested clean checkout: `historical-dispatch-qualification`.
Exact source: `4b45f4521a82e991be18eb28de379a05c53ac0e1`.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest \
  :protomolt-repo-container:scopedPublicationTest \
  --max-workers=2 --console=plain
```

Passed in 19m36s. Storage: two aggregate JUnit cases, 1106.763 seconds.
Scoped publication: one aggregate case, 66.624 seconds. Both reports contain zero
failures, errors or skips. The Gradle log and original XML reports are archived here.
These are packaged runtime drivers, not counts of individual internal assertions.

This run qualifies the replay-refusal checkpoint, including its public conflict
mapping. It does not qualify the later cleanup rotation or SQL rollback fixture;
those changes have separately recorded focused runs. It is local correctness
regression evidence, not hosted CI, deployment or performance qualification.
