# Quality scoring

`protomolt-protobuf-quality` makes data quality a property of the schema. A
message declares its own quality dimensions as CEL expressions over the
message. Anything holding the descriptor can
measure any instance against them. Validation says whether data is
admissible; quality says how good the admissible data is.

```protobuf
import "ai/protomolt/proto/quality/v1/quality.proto";

message Article {
  option (ai.protomolt.proto.quality.v1.quality) = {
    dimension: { id: "titled" cel: "this.title != ''" }
    dimension: { id: "sized"  weight: 3.0
                 cel: "clamp(double(this.body.size()) / 500.0, 0.0, 1.0)" }
    dimension: { id: "fresh"
                 cel: "exp(-double(this.age_days) / 365.0)" }
  };
  string title = 1;
  string body = 2;
  int32 age_days = 3;
}
```

Like the validation rules, the dimensions travel with the descriptor:
git-reviewable, registry-free, readable by any tool that can read a
descriptor set. They survive descriptors linked without the extension
registered, because the annotation is reparsed from its unknown fields.

## Scoring

```java
QualityScorer scorer = QualityScorer.create();
QualityReport report = scorer.score(message);
report.composite();   // weighted average, 0..1
report.dimensions();  // score per dimension id
report.failed();      // dimensions that could not be measured on this message
```

Each expression's result is coerced to a double and clamped to `[0, 1]`; a
bool scores 1 or 0. The composite is the weighted average of the scored
dimensions (weight defaults to 1). The helpers `exp(x)` and
`clamp(x, lo, hi)` are available for decay curves and hand-built formulas.

Failure semantics mirror the validator's. An expression that does not compile
against the message type, such as a misspelled field, is a schema error and
throws `QualitySchemaException` deterministically on the type's first
scoring. An expression that compiles but fails on a particular message marks
that dimension *failed* rather than scored: a measurement gap, weighing
nothing in the composite, because a quality measurement should never take the
data path down. Dimensions are compiled once per message type and cached.

## In the Kafka serde

The serde measures automatically: `protomolt.quality.on.write` is on by
default and costs nothing for types that declare no dimensions. Scores flow
to the [metrics listeners](../sink/kafka-serde.md): with `protomolt-kafka-serde-micrometer`
on the classpath that means `protomolt.serde.quality.score` and
`protomolt.serde.quality.dimension` distributions per topic and type, which
is a quality dashboard of every stream that flows through the serde.

Quality is a measurement, not a gate: until the deployment says otherwise:

```properties
protomolt.quality.min=0.5
```

sets a floor; a write whose composite falls below it is refused the way a
validation failure is. Reads are never rejected on quality
(`protomolt.quality.on.read` measures only): a consumer cannot improve what a
producer already wrote.
