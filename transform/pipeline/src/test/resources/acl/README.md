# ACL mapping test data

Test-only proof-of-concept data for the ACL mapping configs in this directory.
No production pipeline reads these configs yet.

## Descriptor sets

The descriptor sets hold the connector types the configs map from, without
source info. Both are content-equal to these builds:

| File | Source | Built from |
|---|---|---|
| `confluence-v1.binpb` | grpc-confluence `main` a15169f, grpc-integration `main` 097de02 | `ai/pipestream/confluence/v1/confluence_service.proto`, `ai/pipestream/sync/v1/acl.proto` |
| `microsoft-v1.binpb` | grpc-microsoft `main` 090dec2, grpc-integration `main` 097de02 | `ai/pipestream/microsoft/v1/site_security.proto`, `events.proto`, `directory.proto`, `ai/pipestream/sync/v1/acl.proto` |

To regenerate, put each repository's `src/main/proto` tree in a buf v2
workspace (`cf`, `ms`, `sync` modules) and run:

```shell
buf build --exclude-source-info \
  --path cf/ai/pipestream/confluence/v1/confluence_service.proto \
  --path sync/ai/pipestream/sync/v1/acl.proto -o confluence-v1.binpb
buf build --exclude-source-info \
  --path ms/ai/pipestream/microsoft/v1/site_security.proto \
  --path ms/ai/pipestream/microsoft/v1/events.proto \
  --path ms/ai/pipestream/microsoft/v1/directory.proto -o microsoft-v1.binpb
```

## Recorded records

The other `.binpb` files are length-delimited records from live runs against
the test sites, taken from the connectors' replay output:

| File | Message | Live run |
|---|---|---|
| `confluence-sync-records.binpb`, `confluence-private-sync-records.binpb` | `ConfluenceEntity` | grpc-confluence 45185699414 |
| `confluence-permission-checks.binpb` | `CheckContentPermissionRequest` / `Response` pairs | grpc-confluence 45185699414 |
| `sharepoint-sync-records.binpb` | `MicrosoftEntity` | grpc-microsoft 45100103838 |
| `entra-users.binpb` | `GraphUser` | grpc-microsoft 45100103838 |
| `entra-group-changes.binpb` | `GroupChange` | grpc-microsoft 45100103838 |
| `entra-group-owners.binpb` | one `GraphGroup`, then its `DirectoryMember` owners | grpc-microsoft 45100103838 |

The recorders replace email addresses, user principal names and Atlassian
account ids with placeholders (`account-N`, `email-N@example.invalid`,
`join-proof@example.invalid`). Test-site ids are kept as recorded: space,
page, site, list, item and group ids, group names, and the names of lists a
live write run created.
