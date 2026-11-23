# Field mapping

The mapper modules reshape protobuf messages at runtime by copying or moving
fields, appending repeated fields, and clearing scratch data. CEL adds
conditions and computed values. Mapping
is deliberately separate from [validation](validation.md): reshaping a
message and judging it are different concerns.

Two layers are involved. `protomolt-mapper` provides path-based field
access and a small text rule language; `protomolt-mapper-cel` wraps it with
CEL filters and selectors.

## Text rules

`ProtoFieldMapper` (implemented by `ProtoFieldMapperImpl`) applies rules to a
message builder:

```java
var registry = DescriptorRegistry.create();
var mapper = new ProtoFieldMapperImpl(registry);

mapper.mapInPlace(builder, List.of(
    "title = body",
    "tags += \"proto\"",
    "-scratch"
));
```

| Rule | Form | Effect |
|---|---|---|
| Assign | `target = source` | Copy the value at `source` to `target` |
| Append | `target += source` | Append to a repeated `target` |
| Clear | `-field` | Clear the field |

Paths are dotted field paths; sources may be paths or literals. Assigning a
list to a repeated field replaces its elements; appending a list adds each
element. The mapper
also exposes the underlying path operations directly (`getValue`,
`setValue`, `appendValue`, `clearField`) for programmatic use.

## CEL mapping

`CelProtoMapper` evaluates [CEL](https://cel.dev) expressions against the
message, bound by default as `input`. Each `CelMappingRule` combines an
optional boolean filter (should this rule fire?), an optional selector (what
value to write?), a target path, and an optional list of text rules to fall
back on when no selector is given.

```java
var cel = new CelEvaluator(CelEnvironmentFactory.builder()
    .addMessageType(builder.getDescriptorForType())
    .addVar("input")
    .build());

new CelProtoMapper(mapper, cel).map(builder, List.of(
    new CelMappingRule(
        "input.lang == 'en'",   // filter
        "input.title",          // selector
        "search_title",         // target path
        List.of())              // text-rule fallback
));
```

`CelEnvironmentFactory` builds the CEL environment: message types, extra
variables, and custom function bindings. `CelEvaluator` compiles and caches
programs: successful compilations and compile failures alike, so a bad
expression fails fast on every use instead of recompiling. Compilation and
evaluation problems surface as `CelCompilationException` and
`CelEvaluationException`.

Beyond `map`, the mapper supports progressive builder state (later rules see
earlier writes), extra per-call bindings, and candidate fallback via
`tryMap` / `mapFirstCandidate`: try a list of rules and keep the first that
applies. A false filter skips that candidate without evaluating its selector.
Expression compilation errors, missing bindings, non-boolean filters and selector
failures throw instead of selecting a fallback. For optional data, use an explicit
filter such as `has(input.body)` before reading the field. Earlier successful
writes remain on the builder when a later rule fails.

The shared evaluator lives in `protomolt-cel` at `ai.protomolt.proto.cel`.
The mapping classes `CelProtoMapper` and `CelMappingRule` live in
`protomolt-mapper-cel` at `ai.protomolt.proto.mapper.cel`; callers migrating from
their previous package must update imports and recompile.

Any message type works, including `google.protobuf.Struct` for
schema-loose payloads. The `:samples` module contains a runnable example
(`CelMappingSample`).

From a source checkout with JDK 25, run:

```shell
./gradlew :samples:runCelMapping
```

The result contains `name`, `copiedName`, and `selectedName`, each with the
string value `Ada`, and `enabled` with the boolean value `true`. Text mapping
populates `copiedName`; the CEL filter and selector populate `selectedName`.
CEL adapts `Struct` to a map and unwraps its `Value` entries, so the expressions
are `input.enabled` and `input.name`, not protobuf wrapper-field accesses.
This example starts no services. The repository's samples module has broader
dependencies than the mapper itself; a library consumer needs only the mapper
modules and their dependencies.

A selector can build messages and lists of them, so one rule can fill a
repeated message field. The Confluence ACL mapping
(`transform/pipeline/src/test/resources/acl/confluence-document-security.textproto`)
turns the access records grpc-confluence Sync emits into a
`DocumentSecurity` this way: space read grants become `permissions` and each
view-restricted owner becomes one `required_rule_sets` entry. A second
config (`confluence-identity-links.textproto`) turns Sync's users and group
memberships into `IdentityLink`s from an `email` identity, and a third
(`entra-identity-links.textproto`) produces `IdentityLinks` linking each
Entra user principal name to every address of the user: its mail (or the
user principal name when it has no mail), its other mails and its SMTP proxy
addresses.
`CallerIdentityExpansion` follows the links to give an Entra caller the
Atlassian account and groups that share its address. The repository service
reads links from the file named by `PROTOMOLT_IDENTITY_LINKS`. The test runs
the mappings on records from a live Sync run.

For SharePoint, `sharepoint-document-security.textproto` maps the `SiteAcl`
an item's `acl_owner_id` names into `permissions`: each role assignment whose
permission levels include ViewListItems becomes a read rule on the SharePoint
group (`sharepoint-group:<site id>/<group id>`), user (`user-principal-name`,
or `email`), Entra group (`entra-group`) or other claim (`sharepoint-claim`).
Every complete ACL also lets `sharepoint-site-admin:<site id>` read, and the
owners claim of a Microsoft 365 group maps to `entra-group-owner`.
`sharepoint-identity-links.textproto` links the members of each site group to
that group, and members SharePoint marks as site collection administrators to
`sharepoint-site-admin:<site id>`. `entra-group-links.textproto` links the
members of each group in a full DeltaGroups pass (users by `entra-user` object
id, nested groups by `entra-group`) to the group; the Entra user mapping links
each user principal name to its `entra-user` object id.

## Where mapping sits

The REST gateway and the indexing pipeline both accept mapped messages,
a common arrangement is shape/filter with the mapper, then
[validate](validation.md), then [index](../search/indexing.md). For extracting a
metadata bag from messages (rather than reshaping them), see
[Schema metadata](../schema/metadata.md).
