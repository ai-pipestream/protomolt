# Internal Any materialization

Local verification on 2026-10-05, based on `7dd66afe` plus the recorded source
hashes. Sol reviewed the pure decoder and unit coverage without a material blocker.

```sh
./gradlew :protomolt-repo-admission:test :protomolt-repo-admission:checkRuntimeBoundaries --console=plain
```

The full admission suite passed: 185 tests, zero failures, errors or skips,
including 16 new materialization cases. `results.tar.gz` contains final XML results.
The production dependency gate passed; this increment adds no dependencies.

Tests use real serialized protobuf values and complete descriptor closures linked
by the existing binder. Resolver lambdas deliberately exercise the pure lookup
interface; there is no external registry or storage qualification in this run.
They check exact value identity, same URL with different occurrence-selected
schemas, preserve without lookup/parse, explicit unavailable reasons, proto2
required-field absence without a validation claim, nested unknown Any, malformed
payload versus bounds, exact depth 100, outer unknown-field preservation, host
control identity and thread interruption.

The helper is internal and borrowed. The host must authenticate the claimed root
and path, budget input/descriptors/decoded heap, verify retained associations and
keep result ownership alive. It supplies no public RPC, custom JSON representation,
cache, automatic schema discovery, generated-code loader or validation verdict.
Historical adapter integration and missing/corrupt retained-asset mapping remain
required; this unit evidence does not establish them.
