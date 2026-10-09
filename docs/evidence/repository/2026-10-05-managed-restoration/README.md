# Bounded manager integration for original-owner restoration

Local qualification on 2026-10-05, based on `653d47c1` plus this checkpoint.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationSessionIT' --console=plain
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The session suite passed eight cases. The production-JAR PostgreSQL/LocalStack
gate passed, including same-process and fresh-process crash recovery through the
bounded session manager. Injected read interruptions propagate as failures while
retaining the loaded preparation and command capacity. A competing reentrant
resume is refused; changed owners, ordinary execution and takeover cannot replace
the restored entry. Capacity refusal happens before preparation is loaded.

Closing the manager inside an accepted read does not release that call's borrowed
preparation. Once the failing call leaves, idle shutdown releases restoration
reservations and command accounting. A fresh manager restores the same stage,
produces the exact durable rejection, evicts the handle, and replays without
additional assessment reads. Upload resolution is wired to fail if invoked.

Sol found a missing account comparison before terminal replay. The retained red
result demonstrates that a process-authority caller's wrong-account owner reached
the command's receipt. The implementation now checks owner account, operation and
claim digest before replay; the same case passes in the final gate. Sol's final
review found no remaining blocker. A fixture compile error from an undeclared
checked exception was corrected before the final run.

This is a package-private coordinator bridge. It does not add a public RPC,
automatic token discovery, durable registration of ordinary runtime sessions,
or automatic successor takeover. No hosted CI, merge, deployment or performance
claim follows from these local correctness runs.
