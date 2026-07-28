# Confluence domain model

`protomolt-acquire-confluence` is the protobuf schema for a Confluence Cloud
connector. It is a **domain model of Confluence content**, not a
serialization of the REST API: every message is a content concept, and only
information-bearing fields survive. The entity shapes are grounded in the
official Confluence Cloud REST API v2 spec (field-level fidelity: ids as
strings, ISO-8601 date-times as Timestamps, spec-declared enums), but REST
response decoration is deliberately not modeled. The module contains no
client code; the crawler and sinks land in later modules.

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
