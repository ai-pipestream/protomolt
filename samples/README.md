# samples/

Runnable examples for the toolkit, exercised as tests so they cannot rot:
`./gradlew :samples:test --max-workers=2` runs them. The module is
unpublished. Example sources live under `samples/src`; the markdown files
beside them are scenario notes for specific walkthroughs, and
`Dockerfile.authoring` builds the authoring starter image those scenarios
use.

| Gradle project | Directory | What it is |
|---|---|---|
| `:samples` | `samples` | Runnable, test-enforced examples for the published libraries |

For a standalone consumer project (outside this build), use the
[protobuf toolkit example](../examples/protobuf-toolkit/README.md).
