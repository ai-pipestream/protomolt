# Fresh-registry historical recovery

Base: 90f86e0e8. Five PostgreSQL cases passed in 37s with zero failures or
skips:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalPreparationIT.fresh*' --max-workers=2 --console=plain
```

Warm and cold entries recover both reserved and installed unactivated attempts.
A separate case creates three successive cold registries, disposes each registry's
metadata before creating the next, and installs each successor using a decoded
command and SQL discovery. The last generation acquires fresh sources and STARTs.
Assertions cover generation progression, reservation/install/activation counts,
source ownership and zero remaining metadata reservations.

Sol reviewed the cases without blocking findings. The test uses a single JVM;
the final source-capture helper uses the original fixture's address and revision.
This is fresh-registry metadata recovery, not process-crash recovery. Source
fixtures use synthetic provider observations.
