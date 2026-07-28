# Confluence domain model

`protomolt-acquire-confluence` is the protobuf schema for a Confluence Cloud
connector. It is a **domain model of Confluence content**, not a
serialization of the REST API: every message is a content concept, and only
information-bearing fields survive. The entity shapes are grounded in the
official Confluence Cloud REST API v2 spec (field-level fidelity: ids as
strings, ISO-8601 date-times as Timestamps, spec-declared enums), but REST
response decoration is deliberately not modeled. On top of the schema the
module carries the crawler core: `ConfluenceClient` (REST v2 transport),
`ConfluenceMapper` (REST JSON to domain protos), `ConfluenceCrawler`
(virtual-thread orchestration), the `ChangeSink` SPI it emits into, and the
env-driven `ConfluenceConnectorConfig`. Kafka, repo-service, and Lucene
wiring land behind the sink in later modules. On top of the crawler core the
module ships the gRPC facade: `ConfluenceService` (`confluence_service.proto`)
implemented by `ConfluenceGrpcService`, plus the standalone
`ConfluenceProxyServer` launcher that serves it over Netty with reflection
and health on.

## Coverage

Every content area of Confluence has its messages, in
`src/main/proto/ai/pipestream/proto/acquire/confluence/v1/`:

- Content: Page (`page.proto`), BlogPost (`blogpost.proto`), Comment
  (`comment.proto`), Attachment (`attachment.proto`), CustomContent,
  Whiteboard, Database, Folder, SmartLink (`content.proto`)
- Structure: Space, DataPolicy (`space.proto`), Ancestor, ContentTreeEntry,
  ContentIdToContentTypeResponse (`content.proto`)
- People and work: User (`user.proto`), Task (`task.proto`), Label, Like
  (`label.proto`), Operation (`operation.proto`)
- Governance: SpacePermission, SpaceRole, permission transition
  (`permission.proto`), ClassificationLevel (`classification.proto`),
  Redaction (`redaction.proto`), AdminKeyResponse (`admin_key.proto`)
- Properties: ContentProperty, SpaceProperty, AppProperty (`property.proto`)
- Shared: Body/BodyType/BodyFormat, Version, ContentStatus (`common.proto`)

## Design philosophy

- **Domain first.** No hypermedia: `_links` blocks, `self`/`editui`/`tinyui`
  navigation URLs, and pagination transport (`next`, cursors, meta blocks)
  are not modeled because URLs are derivable from the tenant base URL plus
  the entity id at the crawl edge. Two URLs survive because they carry real
  information: `web_url` on every content entity (the human-facing URL, for
  search results and UIs) and `download_url` on Attachment and SpaceIcon
  (content pointers, not navigation). Cursors live only on the crawl
  envelopes.
- **One canonical message per entity kind.** Where the API splits Bulk vs
  Single read shapes that differ only in expansions, the canonical message is
  the richer shape; a list is just `repeated`. One `Comment` covers footer
  and inline comments; one `ContentTreeEntry` covers children and
  descendants; one `Version` covers current versions and history entries.
- **Bodies are content in a declared format.** `BodyType { format, value }`
  with a `BodyFormat` enum (STORAGE_XHTML, ATLAS_DOC_FORMAT, RENDERED_XHTML,
  EXPORT_XHTML, WIKI, RAW, PLAIN_TEXT, ...). The value stays a string because
  it is serialized content in a declared format, not schema-less data.
- **Typed properties.** A property is a typed key-value pair, not a bag of
  JSON. The key is a `PropertyKey` enum of the well-known Confluence keys
  (editor, content-appearance-published, content-appearance-draft) with
  CUSTOM as the escape hatch plus a `custom_key` field; the value is a
  `PropertyValue` oneof (string, integer, bool, double, json) whose arm
  matches the value's actual type. `google.protobuf.Struct` appears exactly
  once in the schema, as the `json_value` arm, for values that genuinely are
  structured JSON. The REST-edge mapper translates the API's string keys and
  JSON values into this typed model; that translation is the mapper's job,
  not the schema's concern.
- **Attachment bytes.** `Attachment.content` (`optional bytes`) holds the
  binary when the crawler fetches it, empty for metadata-only records; the
  crawler may later route bytes through the repo blob store. `title` carries
  the filename.
- **Field-level fidelity.** Spec-declared types everywhere: ids are strings
  (Confluence declares them as strings holding numbers; account ids are
  opaque strings), Timestamps for date-times, hierarchy (`parent_id`,
  `space_id`, ancestors), authorship, lifecycle status, labels, permissions,
  roles, classification. Field names are the snake_case of the spec's
  camelCase, so protobuf JSON (camelCase by default) matches the REST
  payload names; enums serialize by name, so the client maps enum names to
  the API's wire values at the edge.
- **Indexing hints** on the fields that matter for search: titles TEXT with
  the english analyzer, body values TEXT, labels and space keys KEYWORD,
  author/owner ids KEYWORD, timestamps DATE.
- **`metadata = 99`** on every top-level message, per house convention.

## Validation

Every message carries `validate.v1` rules (`protomolt-protobuf-validation`),
so a `ConfluenceEntity` can be checked before it ever reaches Kafka:

```java
ValidationResult result = ValidationResult.validate(entity);   // or ProtoValidator.create().validate(...)
result.violations();  // [Violation[path=id, ruleId=required, message=...], ...]
```

The rules come in four families:

- **Identity is required.** An entity you cannot address is un-crawlable, so
  every content id is `required: true`: `Page.id` + `space_id`,
  `BlogPost.id` + `space_id`, `Comment.id`, `Attachment.id`,
  `Space.id` + `key`, `User.account_id`, `Task.id`, `Label.name`,
  `Like.account_id`, the five content types and `ContentTreeEntry`,
  `ClassificationLevel.id`, `DataPolicy.id`, both property ids,
  `AdminKeyResponse.account_id`, and the envelope identities
  (`ConfluenceEntity.entity_id` + `ingested_at`,
  `ConfluenceSnapshot.snapshot_id`, `ConfluenceChange.change_id`).

  ```protobuf
  string id = 1 [
    (ai.pipestream.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_KEYWORD },
    (ai.pipestream.proto.validate.v1.field) = { required: true }
  ];
  ```

- **Numeric floors, spec-backed where the spec speaks.** The OpenAPI spec
  declares `Redaction.from`/`to` minimum 0 and `RedactionPointer.pointer`
  required; those are annotated verbatim. Wire reality supplies the rest:
  `Attachment.file_size >= 0`, version `number >= 0` (plain int32 renders
  absence as 0, so 0 must pass), `prev_version`/`next_version >= 1` (explicit
  presence, so the 1-based floor is checkable).

- **Cross-field facts the spec cannot express**, as message-level CEL:

  | Rule id | Invariant |
  |---|---|
  | `body_type.format_declared` | a populated body value must declare its `BodyFormat` |
  | `property.custom_key` | `custom_key` is set **iff** `key == PROPERTY_KEY_CUSTOM` |
  | `change.upsert_has_entity` | an UPSERT `ConfluenceChange` must carry the entity it upserts; a DELETE need not |
  | `redaction.range` | a redaction range must not end before it starts |

  ```protobuf
  option (ai.pipestream.proto.validate.v1.message) = {
    cel: {
      id: "change.upsert_has_entity"
      message: "an UPSERT change must carry the entity it upserts"
      expression: "this.operation != 1 || has(this.entity)"
    }
  };
  ```

- **Present-only formats**, as field-level CEL. The framework's format rules
  (`string.email`, `string.uuid`) treat empty as a violation, which is right
  for mandatory fields and wrong for these two: `User.email` (Confluence
  omits it for privacy; `user.email_format` validates only a populated
  value) and `Redaction.redaction_id` (absent on request shapes;
  `redaction.id_uuid` accepts empty, rejects a populated non-UUID).

`ConfluenceValidationTest` exercises every family in both directions, the
violating shape and the passing shape, against the real `ProtoValidator`.

## Wrapper and envelopes

- `ConfluenceEntity` (`confluence_entity.proto`) is the keystone wrapper: a
  `oneof` with one arm per entity kind, plus `entity_id`, an `ingested_at`
  timestamp, and the metadata map. This is the Kafka / parquet / round-trip
  unit.
- `ConfluenceSnapshot` (`events.proto`) is the full-sync marker: space key,
  per-kind entity counts, the resumption cursor, started/completed
  timestamps.
- `ConfluenceChange` (`events.proto`) is the live-update envelope: UPSERT or
  DELETE, the entity, the position it was observed at (CQL cursor or webhook
  event id), and a `ChangeSource` of CRAWL, CQL_INCREMENTAL, or WEBHOOK.

## Intended pipeline

crawl -> `ConfluenceEntity` on Kafka -> repo service `Document` (pages and
blog posts become Documents; bodies, labels, and authorship land in
SearchMetadata) and parquet export -> document-events -> Lucene indexing via
the hints above. `ConfluenceChange` feeds the incremental lane so the mirror
and the index stay live between crawls.

## The gRPC facade

`confluence_service.proto` defines `ConfluenceService`, a thin read proxy
over the crawler core: every rpc delegates to `ConfluenceClient`,
`ConfluenceMapper`, and `ConfluenceCrawler`, and answers in the domain model
above, so a gRPC client can read the workspace and stream changes without
knowing the REST API exists. `ConfluenceGrpcService` is the implementation;
every handler is plain blocking code on a virtual-thread executor.

| rpc | shape | notes |
|---|---|---|
| `ListSpaces` | unary | all spaces, or filtered by `keys`; `limit` caps the result |
| `GetPage` / `GetBlogPost` | unary | by id, body in the requested `BodyFormat` (default storage XHTML) |
| `ListPages` / `ListBlogPosts` | server stream | one entity per message, REST pagination hidden; UNSPECIFIED body format = metadata-only listing |
| `GetAttachment` | unary | metadata always; `include_content` inlines bytes, refused with FAILED_PRECONDITION above the size cap |
| `Sync` | server stream | one bounded pass: changes, a snapshot per space on a full crawl, then a terminal `resume_cursor` |

`Sync` is deliberately bounded (the resume-token idiom): an empty
`since_cursor` runs a full crawl, a set one runs the incremental
newest-first walk, and the stream closes with the cursor to hand back on the
next call. A continuously-tailing `Watch` rpc is a possible additive later.

### Running the proxy

`ConfluenceProxyServer` is the standalone launcher, configured entirely from
the environment:

- `CONFLUENCE_BASE_URL` (with `/wiki`), `CONFLUENCE_EMAIL` or its
  `CONFLUENCE_USER` alias, `CONFLUENCE_API_TOKEN` or its `CONFLUENCE_TOKEN`
  alias: the basic-auth credentials. Canonical names win when both forms are
  set; the token never reaches a log line.
- `CONFLUENCE_SPACES`, `CONFLUENCE_PAGE_SIZE`, `CONFLUENCE_BODY_FORMAT`: the
  crawl defaults, as on `ConfluenceConnectorConfig`.
- `CONFLUENCE_GRPC_PORT`: listen port, default 9095.
- `CONFLUENCE_ATTACHMENT_MAX_BYTES`: inline attachment cap for
  `GetAttachment(include_content)`, default 25 MiB.

```bash
CONFLUENCE_BASE_URL=https://example.atlassian.net/wiki \
CONFLUENCE_USER=me@example.com CONFLUENCE_TOKEN=... \
  java -cp protomolt-acquire-confluence.jar \
  ai.pipestream.proto.acquire.confluence.ConfluenceProxyServer
```

The server registers the gRPC health service and server reflection, and shuts
down gracefully on SIGTERM.

### Agent workflow

Reflection is always on, which is what makes the proxy agent-friendly: any
descriptor-driven client can discover and call the surface without generated
code. Inside protomolt, an LLM reaches it through the MCP reflect verb (list
the services and messages from the running server) and the grpc-invoke verb
(call an rpc with a JSON payload against the reflected schema). Outside
protomolt, `grpcurl` works the same way
(`grpcurl -plaintext localhost:9095 list`), and a Python agent can pull the
file descriptor set over reflection (or from a `protoc
--descriptor_set_out` build of these protos) and generate stubs from it, so
no hand-written client ever sees the REST API. Responses carry the same
validation rules as the crawler output, so an agent can also validate what it
receives with the platform validator.

### Testing

`ConfluenceGrpcServiceTest` runs the facade end to end against
`FakeConfluenceServer` over an in-process gRPC channel, including the
reflection round-trip. `ConfluenceLiveSmokeIT` does one cheap read
(`ListSpaces` with limit 1) against the real workspace; it is excluded from
the default test task and skips unless credentials are in the environment.
Run it explicitly with:

```bash
./gradlew :protomolt-acquire-confluence:liveSmokeTest
```
