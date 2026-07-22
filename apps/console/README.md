# ProtoMolt console

The schema-registry console: a Vue 3 / Vuetify application for browsing
subjects and versions, exploring types, diffing versions, checking
compatibility, and trying the verbs — ProtoMolt's own frontend.

## Running it

```shell
npm install
npm run dev          # against protomolt-serve's registry on localhost:8081
PROTOMOLT_REGISTRY_URL=http://host:port npm run dev   # any Confluent-compatible registry
npm test             # vitest (jsdom for the component and view suites)
npm run build        # static bundle in dist/
```

The dev server proxies `/api/protomolt/*` to the registry, so the app is
same-origin in development. In production the app is served by
`protomolt-serve` itself at `/console`: build `dist/` first and the serve
build bundles it, with the same-origin API bridges provided by the server
(`/api/protomolt` to the in-process registry, `/api/serve` to the verbs).
Any reverse proxy that mirrors those paths works too.

## Provenance

The application source (views, components, services, tests) was originally
written inside the platform frontend's working tree by mistake and recovered
from the snapshot taken before that tree was reset; this scaffold makes it a
standalone app. `reference/platform-integration.patch` records how it was
once mounted in the platform, and `reference/schemaRegistryProxy.ts` is the
BFF proxy the vite dev proxy replaces. The schema-form and descriptor
utilities under `src/lib/` are vendored from the platform packages they came
from.
