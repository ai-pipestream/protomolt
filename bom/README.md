# bom/

The bill of materials: a Gradle `java-platform` (Maven artifact
`ai.pipestream:protomolt-bom`) that aligns the versions of every published
ProtoMolt artifact, plus a published version catalog. Consumers import it
and then omit versions on individual `protomolt-*` dependencies:

```groovy
dependencies {
    implementation platform('ai.pipestream:protomolt-bom:<version>')
    implementation 'ai.pipestream:protomolt-mapper-cel'
}
```

| Gradle project | Directory | What it is |
|---|---|---|
| `:bom` | `bom` | The platform and version catalog; constraints only, no code. Its `check` task fails the build when a published module is missing from the constraints |
