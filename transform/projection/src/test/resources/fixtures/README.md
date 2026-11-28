# Language-Neutral Map Projection Conformance Fixtures (`#981`, `#988`)

`map-projection-conformance.binpb` is a deterministic `ai.protomolt.proto.projection.fixture.v1.ProjectionFixtureBundle` binary protobuf payload shared between Java (`protomolt`) and Rust (`protomolt-rs`).

## Contents

- `descriptor_set_bytes`: self-contained `google.protobuf.FileDescriptorSet` bytes containing `google/protobuf/*`, `ai/protomolt/proto/projection/v1/projection.proto`, and the projection test schemas with `(sources)`, `(from)`, and `(default_from)` custom options intact as raw wire bytes.
- `cases`: positive and negative conformance vectors covering all 12 protobuf map-key scalar domains, unsigned `uint32`/`fixed32`/`uint64`/`fixed64` boundaries, enum wire-number conversion, nested child `MessageProjection` map values, empty-vs-absent path fallthrough vs explicit empty CEL/literal maps, `targetMask`/`sourceMask` validity (`Struct` sub-path truncation and child-CEL completeness), post-coercion duplicate-key refusal, null map value refusal, cross-shape refusal, and order-independent `(target, source)` map cycle detection.

## Refreshing & Verifying

`ProjectionLanguageNeutralFixturesTest` verifies that the checked-in `map-projection-conformance.binpb` matches `ProjectionFixtureCatalog.buildDeterministicBundleBytes()` byte-for-byte and executes every vector using `ClosedDescriptorSet.load(...)` and `DynamicMessage` without generated Java classes.

To regenerate `map-projection-conformance.binpb` after an intentional schema or fixture change:

```bash
PROTOMOLT_WRITE_PROJECTION_FIXTURES="$PWD/transform/projection/src/test/resources/fixtures/map-projection-conformance.binpb" \
  flock /work/.gradle-krick.lock ./gradlew :protomolt-projection:test \
  --tests ai.protomolt.proto.projection.ProjectionLanguageNeutralFixturesTest \
  --max-workers=2
```
