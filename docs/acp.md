# ACP agent

The `protomolt-acp` module exposes the action catalog as an Agent Client
Protocol agent, so an ACP-capable IDE can run ProtoMolt verbs without leaving
the editor. It uses the official
[ACP Java SDK](https://github.com/agentclientprotocol/java-sdk) (sync API)
over stdio; the same catalog serves gRPC/REST, MCP, the CLI, and now ACP.

Each session is a console: a prompt of the form `<verb> <json>` runs the verb
and the JSON result streams back as message chunks. `list` or `help` names the
verbs. Errors print their code and keep the session going. The agent declares
no file, terminal, or permission capabilities; it is read-only. New verbs
(search actions, pipeline runs) appear in the IDE automatically when they
register into the catalog.

## Streaming

Verbs that implement `StreamingAction` stream results as they are produced:
each emission is its own message chunk, so the IDE renders responses live
instead of at the end of the call. `grpc-invoke` is the first: a
server-streaming method emits one chunk per response message, and every run
ends with a terminal status document (`{"ok": true, "status": "OK"}` on
success, or the gRPC status on failure). Unary verbs emit their single result,
so the console contract is the same either way.

## Run it

```shell
./gradlew :protomolt-acp:installDist
acp/build/install/protomolt-acp/bin/protomolt-acp
```

The process speaks JSON-RPC on stdio; logs go to stderr.

## Wire it into JetBrains

In the IDE's AI chat, add a custom ACP agent whose command is the launcher
above (see JetBrains' ACP documentation for the current UI). Each AI chat
session then maps to a catalog console: type `list` to see the verbs, then run
them inline, for example:

```
compile {"sources": {"shop.proto": "syntax = \"proto3\"; package demo.shop.v1; ..."}}
```
