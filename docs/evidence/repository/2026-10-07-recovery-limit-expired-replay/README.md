# Committed recovery decision after lease expiry

Base: `2d60befbdcdb16ef761638e55b293af9a2353be7`. Only tests and documentation
change. Source hashes identify the tested handler and test class.

At the real 16-batch capture bound, a decision commits while its installed
claim/owner leases are live. The test waits until PostgreSQL confirms both have
expired. The same private handler then returns the exact original receipt,
without renewing either lease. A prospective V98 proposal, constructed from the
exact committed installation, is rejected with `Terminal operation cannot
supersede reservation`. No reservation or supersession appears, and recovery
discovery reports TERMINAL with neither candidate type.

One rejection/decision pair and the original 16 capture batches remain. No
execution or capture drain is created; the payload budget is released. The
caller here has process authority. Current scoped READ/credential checks are
qualified separately in the handler evidence, not newly established by this case.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitSupersessionIT' \
  --max-workers=2 --console=plain
```

Both cases passed, zero failures/errors/skips, in 39s: this committed-terminal
case and the prior expired tentative-decision rollback race. `results.tar.gz`
contains JUnit XML; `gradle.log` contains the executed build output.
Sol reviewed the exact prospective identity, replay authorization and terminal
checks with no blocker. Local tests and review do not imply hosted CI or merge.

The SQL state, lease expiry and handlers are real. Initial provider observations
are synthetic. No provider performance, deployment or fresh-process execution
authority is claimed. Retained-source release and corrupted-evidence checks
remain unfinished; historical execution remains unmounted.
