# Contributing to ProtoMolt

Thanks for your interest in ProtoMolt. Contributions of all kinds are
welcome: bug reports, documentation fixes, new descriptor sources, engine
plugins, and validation dialects.

## Getting set up

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew build
```

You need JDK 21+ and [buf](https://buf.build/docs/installation) on the
`PATH`. `./gradlew build` runs the full test suite, `buf lint`, and the
protovalidate conformance gate. See
[docs/building.md](docs/building.md) for the optional registry integration
tests.

## Ground rules

- Every behavioral change comes with tests. The project's bar is high:
  server hosts have real HTTP tests, the validator is gated at 100% of the
  protovalidate conformance suite, and registry loaders have integration
  tests against live registries.
- `.proto` changes must pass `buf lint` (STANDARD rules), and pull requests
  are checked with `buf breaking` against the target branch.
- Public API carries Javadoc. Follow the surrounding code's style; there is
  no separate style guide to memorize.
- Keep modules decoupled: `core/` must not depend on any framework, engine
  plugins communicate through `search/index/spi`, and validation dialects plug in
  via `ValidationRuleSource` — new capabilities should arrive as new
  modules behind existing seams rather than new dependencies in existing
  ones.

## Extension points

The most useful contributions tend to be implementations of existing SPIs:

| SPI | Module | What it adds |
|---|---|---|
| `DescriptorLoader` | `core/descriptors` | A new place to load schemas from |
| `ProtoGatherer` | `acquire/gather/core` | A new place to gather `.proto` sources from |
| `SchemaPublisher` | `core/sources` | A new registry to publish schemas to |
| `ValidationRuleSource` | `protobuf/validation` | A new annotation dialect |
| `IndexingHintSource` | `search/index/spi` | A new way to resolve indexing hints |
| `SearchEngineIndexerProvider` | `search/index/spi` | A new search engine backend |

## Submitting changes

Open a pull request against `main`. CI must pass (build, tests, lint,
breaking-change check). For anything larger than a bug fix, consider opening
an issue first to discuss the approach — especially for new modules, where
naming and placement matter.

## License

By contributing, you agree that your contributions are licensed under the
project's [Apache License 2.0](LICENSE).
