# Confluence Cloud REST API v2 schema

`protomolt-acquire-confluence` is the protobuf schema for a Confluence Cloud
connector, modeled from the official Confluence Cloud REST API v2 OpenAPI
3.0.3 spec. It contains no client code; the crawler and sinks land in later
modules.

## Coverage

Every API group of the v2 spec has its messages, in
`src/main/proto/ai/pipestream/proto/acquire/confluence/v1/`:

- Admin Key (`admin_key.proto`)
- Ancestors, Children, Descendants, Content (`content.proto`)
- App Properties, Content Properties, Space Properties (`property.proto`)
- Attachment (`attachment.proto`)
- Blog Post (`blogpost.proto`), Page (`page.proto`)
- Classification Level (`classification.proto`)
- Comment (footer + inline) (`comment.proto`)
- Custom Content, Database, Folder, Smart Link, Whiteboard (`content.proto`)
- Data Policies, Space (`space.proto`)
- Label, Like (`label.proto`), Operation (`operation.proto`)
- Redactions (`redaction.proto`), Task (`task.proto`), User (`user.proto`)
- Space Permissions, Space Roles, Space Permission Transition
  (`permission.proto`)
- Shared bodies, versions, links, and enums (`common.proto`)

## Design: parity, not 1:1

The schema models the API's information faithfully, but does not copy the
OpenAPI document's mechanical duplication:

- **One canonical message per entity kind.** `Page`, `BlogPost`, `Comment`,
  `Attachment`, `Space`, `Label`, `Task`, `User`, `Whiteboard`, `Database`,
  `Folder`, `CustomContent`, `SmartLink`, `ClassificationLevel`,
  `ContentProperty`, `SpaceProperty`, `AppProperty`, `Operation`, `Redaction`,
  `DataPolicy`, `Version`, `Ancestor`, `AdminKeyResponse`, `Like`. Where the
  spec splits Bulk vs Single read shapes that differ only in optional
  expansions, the canonical message is the richer Single shape (extended with
  the few fields the Bulk variant alone declares, e.g. `Space.current_active_alias`,
  `Page.subtype`).
- **A list is `repeated`.** The spec's `MultiEntityResult<T>` transport
  wrappers (results plus pagination meta/links) are not modeled; expandables
  on entities are plain `repeated` fields, and the crawl envelopes carry
  cursors.
- **No write-side shapes.** The crawler is read-side; body-write variants and
  create/update request models were dropped deliberately and can come back
  deliberately if a write flow ever lands.
- **Shared sub-structures are factored once.** One `Body` (per-representation
  optional fields of `BodyType { representation, value }`), one `Links` for
  every entity whose `_links` is just `webui` (per-entity links remain only
  where fields actually differ: `AbstractPageLinks`, `AttachmentLinks`,
  `WhiteboardLinks`), one `ContentTreeEntry` covering the Children /
  Descendants / ChildPage / ChildCustomContent responses, one `Comment`
  covering footer and inline comments.
- **Field-level fidelity stays.** Every information-bearing field the API
  returns keeps its place, its spec-declared type (ids are strings holding
  numbers, `accountId` a string, no numeric coercion), Timestamps for
  ISO-8601 date-times, and indexing hints on the fields that matter for
  search (titles TEXT with english analyzer, body values TEXT, labels and
  space keys KEYWORD, author/owner ids KEYWORD, timestamps DATE). Proto field
  names are the snake_case of the spec's camelCase, so protobuf JSON
  (camelCase by default) round-trips with the REST payloads; `_links` maps to
  a field named `links`. Enums mirror the spec's values but serialize by
  name in protobuf JSON, so the client maps enum names to the spec's wire
  values at the edge.
- **`metadata = 99`** on every top-level message, per house convention.

### Where Struct is (and is not)

Content, space, and app property values are arbitrary JSON per key in the
spec (`additionalProperties: true`), so they are `google.protobuf.Struct`.
That is the only Struct usage in the schema, and it is there precisely
because those fields genuinely are schema-less JSON.

Bodies are the deliberate opposite: `BodyType.value` stays a string because
`storage` is XHTML and `atlas_doc_format` is serialized ProseMirror JSON -
serialized content in a declared format, not schema-less data. No raw-JSON
passthrough, no pretending non-JSON is JSON.

### Attachment bytes

`Attachment` carries `optional bytes content` alongside the full spec
metadata (media_type, file_size, comment, version, download link; `title`
carries the filename). Populated when the crawler fetches the binary, empty
for metadata-only records; the crawler may later route the bytes through the
repo blob store instead of inlining them.

## Wrapper and envelopes

- `ConfluenceEntity` (`confluence_entity.proto`) is the keystone wrapper: a
  `oneof` with one arm per canonical entity kind, plus `entity_id`, an
  `ingested_at` timestamp, and the metadata map. This is the Kafka / parquet /
  round-trip unit.
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
