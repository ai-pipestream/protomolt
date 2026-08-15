# Document platform

The document platform is the one-container deployment of the document
pipeline: `apps/document-platform` wires repo-service, the authenticated
intake door, the parsing coordinator, the durable jobs worker, the schema
registry, the search door with its console page, and the streaming parser
playground into one JVM
over the in-process transport. It is the productized form of what
`GoldenPathSystemTest` proves. The same binary boots as specialized nodes
via `PROTOMOLT_ROLES` — see [Role nodes](role-nodes.md).

## Running it

```shell
./gradlew :protomolt-document-platform:installDist
docker compose -f deploy/document-platform/compose.yml up --build
```

The compose file brings PostgreSQL (two databases: the repository ledger and
the jobs store) and RustFS for object storage. Ports:

| Port | Surface |
| --- | --- |
| 9090 | repo-service gRPC (DocumentService, DriveService, health, reflection) |
| 9092 | intake gRPC (`IntakeService`, API-key authenticated) |
| 9093 | parsing coordinator gRPC (`ParseCoordinatorService`) |
| 9094 | search door gRPC (`SearchService`, `SearchIndexService`) |
| 8081 | schema registry HTTP, with the jobs verbs on `/protomolt/actions` |
| 8095 | parser playground |
| 8096 | search console (the search page + operations panel) |

## What first boot does

- The git-backed registry initializes at `DOCUMENT_PLATFORM_REGISTRY_GIT`
  (default `/data/registry.git`) and **publishes the fleet document model**
  (`ai/pipestream/document/v1/document.proto`) from the build's own
  classpath, so the registry serves it as a subject from the start. The
  registry is not optional here: the platform runs it by default.
- The `parse-document` workflow registers under that name, so a durable parse
  is one `submit-workflow` call:
  `POST /protomolt/actions/submit-workflow` with
  `{"workflowName": "parse-document", "input": {"address": {...}}}`; poll with
  `get-job`. The platform's own worker fleet claims and completes it.
- The `parse-and-index` workflow registers too: the same submission with
  `{"workflowName": "parse-and-index", "input": {"address": {...},
  "mappingSubject": "repo-document"}}` parses the document and indexes it
  under the [search door](../search/door.md)'s `repo-document` subject, so a
  completed run means the document answers queries on the search port.
- The `replay-documents` action re-runs a stored workflow over a drive's
  documents (one durable run each): the operation behind a chunking-policy
  or mapping change, with the door's atomic replace-by-identity keeping
  replays duplicate-free.
- The search door serves the `repo-document` mapping subject over the index
  at `DOCUMENT_PLATFORM_SEARCH_INDEX_DIR` (default `/data/search-index`).
  The lexical lane always works; the vector lane activates when a Model2Vec
  model directory is configured (`PROTOMOLT_MODEL2VEC_PATH`), with the
  policy's dims read from the loaded model.
- The embedded reference text parser serves text and markdown. A fleet of
  external parsers replaces it by pointing
  `DOCUMENT_PLATFORM_PARSE_PROFILES` (+ `..._PROFILE_ENDPOINT`) at a
  service-profile store; routing rules come from
  `DOCUMENT_PLATFORM_PARSE_RULES_JSON` when the defaults (text/markdown to
  the reference parser) are not enough.
- With `DOCUMENT_PLATFORM_SEED_ACCOUNT_ID` set, the account's `intake` and
  `pipeline` drives provision at boot; the compose file seeds account
  `demo` with API key `demo-key`.

Key stores follow the intake door's convention: OIDC introspection
(`DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL` + client id/secret) or
the env-seeded table (`DOCUMENT_PLATFORM_INTAKE_KEYS`).

## Configuration

The repository half of the platform reads the `DOCUMENT_PLATFORM_*` family
exactly as [`repo/README.md`](../../repo/README.md) documents it. The
platform's own variables are javadoc'd on `DocumentPlatformConfig`; the
compose file is the worked example.

`DocumentPlatformSmokeIT` drives every external surface over real TCP:
registry subjects, authenticated ingest, submit-workflow to completion, the
parsed result read back, parse-and-index to a lexical search hit on the
search port, replay without duplication, the playground page, and the
search console (page, subjects, a search hit through the JSON bridge, and
the operations proxy).
