# ACL mapping test data

Test-only proof-of-concept data for the ACL mapping configs in this directory.
No production pipeline reads these configs yet.

## Descriptor sets

The descriptor sets hold the connector types the configs map from, without
source info. Both are content-equal to these builds:

| File | Source | Built from |
|---|---|---|
| `confluence-v1.binpb` | grpc-confluence `main` a15169f, grpc-integration `main` 097de02 | `ai/pipestream/confluence/v1/confluence_service.proto`, `ai/pipestream/sync/v1/acl.proto` |
| `microsoft-v1.binpb` | grpc-microsoft `main` acfc858, grpc-integration `main` 2f24924 | `ai/pipestream/microsoft/v1/site_security.proto`, `events.proto`, `directory.proto`, `ai/pipestream/sync/v1/acl.proto` |

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
the test sites, taken from the connectors' replay output. grpc-microsoft
writes its four files when its write replay runs with a target directory:

```shell
./gradlew :grpc-microsoft-service:test --tests '*MicrosoftWriteReplayTest' -PreplayRecordsDir=/absolute/dir
```

| File | Message | Live run |
|---|---|---|
| `confluence-sync-records.binpb`, `confluence-private-sync-records.binpb` | `ConfluenceEntity` | grpc-confluence 45185699414 |
| `confluence-permission-checks.binpb` | `CheckContentPermissionRequest` / `Response` pairs | grpc-confluence 45185699414 |
| `sharepoint-sync-records.binpb` | `MicrosoftEntity` | grpc-microsoft 46054394305 |
| `entra-users.binpb` | `GraphUser` | grpc-microsoft 46054394305 |
| `entra-group-changes.binpb` | `GroupChange` | grpc-microsoft 46054394305 |
| `entra-group-owners.binpb` | one `GraphGroup`, then its `DirectoryMember` owners | grpc-microsoft 46054394305 |

The recorders replace email addresses, user principal names and Atlassian
account ids with placeholders (`account-N`, `email-N@example.invalid`,
`join-proof@example.invalid`). The Confluence records keep test-site ids as
recorded: space and page ids, group names. The Microsoft recorder replaces
tenant, site, list, drive, item and directory object ids with placeholders
too, and keeps SharePoint's numeric group and list item ids, the names of
sharing link and limited access groups (a kind and placeholder ids) and the
names of lists a live write run created.

The SharePoint records of run 46054394305 hold one file shared in that run
(`shared.txt`, under role assignments of its own): the group of an
organization view link and the group of a link for named people each hold
Read on it, the one directory user is the member of the latter, and an
invited directory group holds Contribute on the file and Limited Access on
the site. The site also holds groups of organization links for files that no
longer exist.
