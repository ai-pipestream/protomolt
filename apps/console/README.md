# ProtoMolt console

ProtoMolt's own frontend: a Vue 3 / Vuetify application whose sections are
the platform's surfaces. Tasks follows durable agent work; Schemas browses,
diffs, and compatibility-checks registry subjects; Workflows edits, verifies,
and runs stored workflows with findings attributed to their steps; Services
registers gRPC services and calls every reflected method through generated
forms; Search and Metrics query a platform node found by its contract through
a registered service profile; Receipts verifies and evaluates signed work
records; Repository reads a repository host's accounts, documents, revisions
and files over binary Connect.

## Running it

```shell
npm install
npm run dev          # against protomolt-serve's registry on localhost:8081
PROTOMOLT_REGISTRY_URL=http://host:port npm run dev   # any Confluent-compatible registry
PROTOMOLT_REPOSITORY_URL=http://host:port npm run dev # a repository host, for /connect
npm test             # vitest (jsdom for the component and view suites)
npm run build        # static bundle in dist/
```

The dev server proxies `/api/protomolt/*` to the registry, so the app is
same-origin in development. In production the app is served by
`protomolt-serve` itself at `/console`: build `dist/` first and the serve
build bundles it, with the same-origin API bridges provided by the server
(`/api/protomolt` to the in-process registry, `/api/serve` to the verbs).
Any reverse proxy that mirrors those paths works too.

## Layout

The application is a standalone Vue app that talks only to `protomolt-serve`.
The schema-form and descriptor utilities under `src/lib/` are part of this
app; `x-protomolt-lookup` is the schema annotation its reference pickers
resolve, with the resolver registry in `src/lib/schema-form/lookups.ts`.

## Protobuf inspection and WASM previews

Repository part reads now expand nested `Any` using the selected revision's
retained schemas. Download the original protobuf or run a local WASM text
preview without writing repository data. See the [preview guide and binary
ABI](examples/README.md) for the example module, limits, and browser checks.
