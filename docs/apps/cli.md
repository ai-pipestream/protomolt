# The command line

`protomolt-cli` is the [action catalog](../surface/actions.md) on the command line. It
adds no verbs of its own: it builds the same full catalog as the gRPC service
and dispatches to it with one JSON document in and one JSON document out. The
[generated action inventory](../generated/action-inventory.json) records the exact
surface names. Verbs whose jobs, inference, or service-workspace backends are
not configured remain discoverable and answer `unavailable`. The standalone
[MCP server](../surface/mcp.md) exposes a smaller host-independent subset.

```shell
./gradlew :protomolt-cli:installDist
cli/build/install/protomolt-cli/bin/protomolt-cli list
```

The entry point is `ai.protomolt.proto.cli.ProtoMoltCli`, so the module can
also be run from a jar or embedded directly. `ProtoMoltCli.run` takes its
streams and its catalog as arguments; `main` only wires the process streams
and the exit code.

## macOS (.dmg)

Each release attaches `protomolt-cli-<version>-macos-aarch64.dmg`: the GraalVM
native darwin-aarch64 binary, built on a GitHub-hosted `macos-15` Apple-silicon
runner by `.github/workflows/macos-cli-dmg.yml`. The release workflow calls it
with the released version; it can also be dispatched manually from any ref, in
which case the DMG is a workflow artifact named from `git describe` instead of
a release asset. It is native-image (the same binary the Linux CLI images
ship), not jpackage: jpackage would bundle a JRE app-image, which is larger,
starts slower, and buys nothing for a single self-contained executable.

Install: open the DMG and copy `protomolt-cli` onto your `PATH`.

Signing and notarization are optional and turn on automatically when the
repository secrets exist; none of them are required to produce a working DMG.

| Secret | Holds |
|---|---|
| `MACOS_SIGNING_CERT_P12` | base64 of a Developer ID Application certificate exported as .p12 |
| `MACOS_SIGNING_CERT_PASSWORD` | the .p12 password |
| `MACOS_SIGNING_IDENTITY` | optional identity string; defaults to the first Developer ID Application in the imported certificate |
| `APPLE_NOTARY_KEY_ID` | App Store Connect API key id |
| `APPLE_NOTARY_ISSUER_ID` | App Store Connect issuer id |
| `APPLE_NOTARY_KEY_P8` | base64 of the App Store Connect API private key (.p8) |

With the first two set, the binary is codesigned with the hardened runtime;
with all six, the DMG is also notarized and stapled, so Gatekeeper accepts it
with no prompts. Without them the DMG is unsigned and macOS quarantines the
downloaded binary; clear it once after copying:

```shell
xattr -d com.apple.quarantine /usr/local/bin/protomolt-cli
```

## Invocation

| Command | Does |
|---|---|
| `protomolt-cli` (or `--help`, `-h`, `help`) | Prints usage and the verb count |
| `protomolt-cli list` | Prints every verb's name and description, one per line |
| `protomolt-cli console` | Opens the interactive console on stdin |
| `protomolt-cli <verb> [input]` | Runs one verb and prints its result |

Anything that is not `list`, `console`, or a help flag is treated as a verb
name and looked up in the catalog; an unrecognized name exits 2 with a
pointer to `protomolt-cli list`.

## Input

Every verb takes a JSON object: the envelope documented in
[Actions](../surface/actions.md). Four ways to supply it:

| Form | Reads from |
|---|---|
| `protomolt-cli compile '{"sources": {...}}'` | A bare positional argument |
| `protomolt-cli compile -i '{"sources": {...}}'` | `-i` / `--input`, an inline JSON string |
| `protomolt-cli compile --input-file input.json` | A file |
| `cat input.json \| protomolt-cli compile` | Standard input |

Arguments after the verb are scanned left to right. Each `--input` or
`--input-file` replaces whatever has been read so far, so the last one
wins; a bare positional argument is taken only when nothing before it has
already supplied the input. Stdin is read only when no argument did.
`--input` and `--input-file` each require a following value, and a missing
one is a usage error.

Blank input becomes an empty object. A JSON array, string, or number is
rejected before dispatch with its detected shape.

## Output and exit codes

Results go to stdout as pretty-printed JSON. Failures go to stderr as
`code: message`, using the catalog's stable kebab-case error codes
(`unknown-type`, `invalid-input`, `compile-failed`, …), so a shell can
branch on the code without parsing prose.

| Exit code | Means |
|---|---|
| `0` | The verb succeeded, or usage/`list`/`console` completed |
| `1` | The verb failed (an action error, or an unexpected internal failure) |
| `2` | Usage error: unknown verb, or input that could not be read or parsed |

## The console

`protomolt-cli console` reads lines from stdin at a `protomolt> ` prompt.
A line is `<verb> <json>`, run against the same catalog and printed the same
way. `list` or `help` prints the verb table; `exit` or `quit` ends the
session, as does end of input. A failing verb prints its error and the
session continues, so a typo does not end the console. The console always
exits 0.

```
protomolt> list
protomolt> compile {"sources": {"p/m.proto": "syntax = \"proto3\"; package p; message M { string id = 1; }"}}
protomolt> exit
```

## Schemas on the command line

Wherever a verb takes a schema it accepts `{"type": ...}`, inline
`{"sources": {...}, "root": ...}`, or `{"descriptorSetBase64": ...}`. The
CLI builds a context with a fresh descriptor registry holding only the
well-known types, so `{"type": ...}` resolves nothing a caller registered
elsewhere. On the command line, pass `sources` or `descriptorSetBase64`,
the latter is what `compile`, `reflect`, and `gather-git` return, and what
the registry's descriptor-set endpoint serves. Inline `sources` are
compiled per call and must carry every file they import, including
`ai/protomolt/proto/meta/v1/metadata.proto` and the other ProtoMolt option
files if the schema uses them.

Two verbs behave differently here than they do in a server, because the CLI
constructs the catalog with no operator configuration:

- `gather-git` caches its clones under the library default,
  `~/.cache/protomolt/gather/git`. A server takes `--gather-cache` instead;
  the CLI has no equivalent flag.
- `run-workflow` is inline-only. `workflowName` needs a workflow repository, which
  is mounted by the registry, so on the CLI it returns
  `{"ok": false, "error": "No workflow repository is mounted; …"}`.
