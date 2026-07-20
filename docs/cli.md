# The command line

`protomolt-cli` is the [action catalog](actions.md) on the command line. It
adds no verbs of its own: it builds the same twenty-three-verb catalog the
gRPC service, the [MCP server](mcp.md), and the [ACP agent](acp.md) serve,
and dispatches to it with one JSON document in and one JSON document out.

```shell
./gradlew :protomolt-cli:installDist
cli/build/install/protomolt-cli/bin/protomolt-cli list
```

The entry point is `ai.pipestream.proto.cli.ProtoMoltCli`, so the module can
also be run from a jar or embedded directly. `ProtoMoltCli.run` takes its
streams and its catalog as arguments; `main` only wires the process streams
and the exit code.

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

Every verb takes a JSON object — the envelope documented in
[Actions](actions.md). Four ways to supply it:

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

Input that is blank becomes an empty object. Input that parses as JSON but
is not an object — an array, a string, a number — is rejected before
dispatch, naming the shape it was given.

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
elsewhere. On the command line, pass `sources` or `descriptorSetBase64` —
the latter is what `compile`, `reflect`, and `gather-git` return, and what
the registry's descriptor-set endpoint serves. Inline `sources` are
compiled per call and must carry every file they import, including
`ai/pipestream/proto/meta/v1/metadata.proto` and the other ProtoMolt option
files if the schema uses them.

Two verbs behave differently here than they do in a server, because the CLI
constructs the catalog with no operator configuration:

- `gather-git` caches its clones under the library default,
  `~/.cache/protomolt/gather/git`. A server takes `--gather-cache` instead;
  the CLI has no equivalent flag.
- `run-chain` is inline-only. `chainName` needs a chain repository, which
  is mounted by the registry, so on the CLI it returns
  `{"ok": false, "error": "No chain repository is mounted; …"}`.
