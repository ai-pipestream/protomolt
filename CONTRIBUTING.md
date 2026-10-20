# Contributing to ProtoMolt

Thanks for your interest in ProtoMolt. Contributions of all kinds are
welcome: bug reports, documentation fixes, new descriptor sources, engine
plugins, and validation dialects.

## Where the repository lives

ProtoMolt has two homes:

- **Forgejo, the canonical repository:**
  [git.rokkon.com/ai-pipestream/protomolt](https://git.rokkon.com/ai-pipestream/protomolt).
  `main` moves only here, through gated pull requests; CI, review and merges
  all happen on Forgejo.
- **GitHub, the public mirror:**
  [github.com/ai-pipestream/protomolt](https://github.com/ai-pipestream/protomolt).
  After a pull request merges into Forgejo `main`, the
  `forward-main-to-github` workflow opens a short-lived GitHub pull request so
  the required checks run on the exact Forgejo commit, fast-forwards GitHub
  `main` to it, and closes that pull request. Nothing merges into GitHub
  `main` directly, and neither `main` is ever force-pushed.

Cloning from GitHub is fine for building, exploring and opening pull
requests; it carries the same history as Forgejo.

## Getting set up

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew build
```

You need JDK 25 and [buf](https://buf.build/docs/installation) on the
`PATH`. `./gradlew build` runs the full test suite and the protovalidate
conformance gate; `buf lint` runs standalone or through the root
`./gradlew check` task. The full prerequisites list (JDK, buf version, Node
for the console, Docker for the integration suites) is in
[docs/operations/building.md](docs/operations/building.md).

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
  via `ValidationRuleSource`: new capabilities should arrive as new
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

Target `main` in either forge:

- **If you have a Forgejo account**, push your branch to Forgejo and open a
  pull request against `main` there. That is the gate: CI must pass (build,
  tests, lint, breaking-change check), and the merge happens on Forgejo.
- **If you contribute through GitHub**, open a pull request against `main` on
  the GitHub mirror. GitHub CI runs there as a first signal, but the pull
  request is not merged on GitHub. A maintainer lands it by pushing the branch
  to Forgejo and opening the gating pull request against Forgejo `main`; once
  that merges, the forward workflow brings the commit back to GitHub `main`
  and your GitHub pull request shows as merged or is closed with a reference
  to the landing commit. Work that reached GitHub first is kept: a guard
  workflow refuses a Forgejo merge whose head does not already contain GitHub
  `main`, so the forges never diverge.

For anything larger than a bug fix, consider opening
an issue first to discuss the approach: especially for new modules, where
naming and placement matter.

## License

By contributing, you agree that your contributions are licensed under the
project's [Apache License 2.0](LICENSE).
