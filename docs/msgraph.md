# Microsoft Graph

`protomolt-msgraph` speaks Microsoft Graph directly — no Microsoft SDK, no
Windows agent. Two lanes:

- **Files and metadata** (input *and* output): OneDrive and SharePoint Online
  documents through the `driveItem` model, including the SharePoint list-item
  columns behind every document — read them as data-rich JSON that
  `infer-schema` turns into typed messages, write content and patch columns
  back.
- **Copilot connectors** (output): the external connections API — the same
  door Microsoft's closed-source, Windows-only Graph Connector Agent walks
  through, spoken directly. Create a connection, register a schema rendered
  from indexing hints, PUT external items; Microsoft 365 Search and Copilot
  index them. This is the roll-your-own-agent lane: cross-platform, no MSI,
  no agent enrollment.

The classes live in `ai.pipestream.proto.graph`: `GraphAuth` (OAuth2),
`GraphClient` (authorized JSON with Graph's `Retry-After` throttling contract
honored), `GraphFiles`, `GraphConnections`, and `GraphSchemas`.

## One-time tenant setup

1. In Microsoft Entra admin center, **App registrations → New registration**
   (single tenant is fine).
2. For the operator lane (device code): under **Authentication**, enable
   *Allow public client flows*; under **API permissions** add delegated
   `User.Read`, `Files.ReadWrite.All`, `Sites.ReadWrite.All`.
3. For the service lane (connectors): create a **client secret**; add
   application permissions `ExternalConnection.ReadWrite.OwnedBy` and
   `ExternalItem.ReadWrite.OwnedBy`; click **Grant admin consent**.
4. Note the *Directory (tenant) ID* and *Application (client) ID*.

## Probing what a tenant can do

Before integrating anything, one command reports what the license and
permissions actually allow — signed-in user, OneDrive, SharePoint sites,
connectors — each as OK, permission-denied, or absent:

```shell
# device code, delegated
./gradlew -q :protomolt-msgraph:graphProbe -Ptenant=<tenant-id> -Pclient=<app-id>
# app-only, client credentials
./gradlew -q :protomolt-msgraph:graphProbe -Ptenant=<tenant-id> -Pclient=<app-id> -Psecret=<secret>
```

A OneDrive-only license reports SharePoint as reachable with zero visible
sites rather than failing: OneDrive for Business *is* a SharePoint document
library underneath, so the same `driveItem` and list-item APIs work either
way. Every probe is a read; nothing is written to the tenant.

## Files and metadata

```java
GraphAuth.Token token = new GraphAuth(GraphAuth.Config.delegated(tenant, client))
        .deviceCode("Files.ReadWrite.All offline_access", p -> System.out.println(p.message()));
GraphFiles files = new GraphFiles(new GraphClient(token::accessToken));

JsonNode drive = files.meDrive();
JsonNode children = files.children(drive.path("id").asText(), "/Reports");
byte[] doc = files.download(driveId, itemId);

// The SharePoint columns behind a document - infer-schema input.
JsonNode fields = files.listItemFields(driveId, itemId);

// Output: upload content, patch metadata columns.
files.upload(driveId, "/Reports", "q3.pdf", bytes, "application/pdf");
files.updateListItemFields(driveId, itemId,
        (ObjectNode) mapper.readTree("{\"Status\": \"Approved\"}"));
```

The `listItemFields` payload is exactly the data-rich JSON `infer-schema`
reverse-engineers into a proto — SharePoint metadata becomes a typed,
validatable, mappable message in one verb call.

## Read to a typed schema

`listItemFieldsOnly` returns just the `fields` object, ready to hand straight to
`infer-schema` as one sample — read a folder's documents, infer one message from
their columns:

```java
List<Struct> samples = new ArrayList<>();
for (JsonNode child : files.children(driveId, "/Shared Documents").path("value")) {
    ObjectNode fields = files.listItemFieldsOnly(driveId, child.path("id").asText());
    if (!fields.isEmpty()) {
        Struct.Builder sample = Struct.newBuilder();
        JsonFormat.parser().merge(fields.toString(), sample);
        samples.add(sample.build());
    }
}
var shape = new SchemaInferrer().infer("sharepoint.v1.Documents", samples);
System.out.println(shape.protoSource());
```

Integer columns become `int64`, multi-choice columns `repeated`, person and
lookup columns nested messages; the exact SharePoint column names are preserved
as `json_name`, so the inferred schema round-trips the very documents it was
inferred from. The inferred message feeds back into `GraphSchemas.connectionSchema`
to register the connection — SharePoint metadata in, a search schema out.

The runnable sample does the whole live round trip:

```shell
./gradlew -q :samples:runGraphInferSchema \
    -Ptenant=<tenant-id> -Pclient=<app-id> -Pfolder="/Shared Documents"
```

## The connectors lane (agentless)

```java
GraphAuth.Token token = new GraphAuth(GraphAuth.Config.application(tenant, client, secret))
        .clientCredentials();
GraphConnections connections = new GraphConnections(new GraphClient(token::accessToken));

connections.create("protomoltorders", "Orders", "Orders from the order service");

// The schema comes from indexing hints - declared once in the proto.
IndexingPlan plan = IndexingPlanFactory.inferringOnly().create(orderDescriptor);
GraphSchemas.Rendered rendered = GraphSchemas.connectionSchema(orderDescriptor, plan);
connections.registerSchema("protomoltorders", rendered.schema(), Duration.ofMinutes(10));

connections.putItem("protomoltorders", order.getId(), properties,
        fullText, GraphConnections.everyoneAcl());
```

`GraphSchemas` is the fourth engine for the indexing standard: `TEXT` fields
become searchable strings, `KEYWORD` exact-match queryables, sortable or
facetable fields refinable (never together with searchable — Graph forbids
it), repeated fields the collection types Graph offers. Anything Graph's
flat property model cannot represent is returned in `skipped` with a reason,
never silently dropped. Property names are Graph-legal (alphanumeric, 32
chars): dotted paths camel-case, collisions numbered.

Schema registration is asynchronous on Microsoft's side and can take minutes
on a fresh connection; `registerSchema` polls the operation to completion.

## Boundaries stated plainly

- Tokens live in memory. Persisting a refresh token is credential storage —
  the operator's deliberate act, not a library default.
- Uploads use the simple lane (≤ 4 MB); large-file upload sessions are a
  later phase, as are incremental crawl checkpoints and non-everyone ACLs
  mapped from schema metadata.
- Displaying connector results in Microsoft 365 Search/Copilot requires the
  tenant to have the relevant Microsoft 365/Copilot licensing; the ingestion
  API itself works on any work/school tenant with admin consent.
