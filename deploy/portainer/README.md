# Always-on coordinator in Portainer

`compose.yml` runs the persistent ProtoMolt coordinator on the NAS Portainer
endpoint. Portainer is the sole lifecycle owner. The existing DJL stack remains
on the GPU host and is not duplicated here.

The server image is pinned by digest. Updating the coordinator is an explicit
compose change after a new image has been built and verified from `main`.

The stack provides:

| Service | LAN endpoint | Purpose |
|---|---|---|
| ProtoMolt MCP/HTTP | `http://nas:19902/mcp` | MCP, REST, OpenAPI, Swagger, and console |
| ProtoMolt gRPC | `nas:19903` | ProtoMolt API and reflection |
| ProtoMolt registry | `http://nas:19904` | Git-backed Confluent-compatible schema registry |
| RustFS | `http://nas:31900` | Persistent S3-compatible artifact storage |
| Keycloak | `http://nas:19901` | Local OIDC identity provider |

Traefik terminates TLS with the NAS wildcard certificate for all externally
named surfaces:

| Surface | TLS endpoint |
|---|---|
| MCP, REST, Swagger, console | `https://protomolt.rokkon.com` |
| gRPC and reflection | `protomolt-grpc.rokkon.com:443` |
| Schema registry | `https://protomolt-registry.rokkon.com` |
| Keycloak | `https://keycloak.rokkon.com` |

All four names are single labels under `rokkon.com`, so a `*.rokkon.com`
certificate covers them. They need DNS records pointing at the NAS.

## Portainer configuration

Create a standalone Compose stack on the NAS endpoint with this file and set
these stack environment variables. Values marked secret must be generated and
stored in Portainer, not committed.

| Variable | Required | Notes |
|---|---|---|
| `PROTOMOLT_API_TOKEN` | yes | Shared bearer token for operational ProtoMolt surfaces |
| `PROTOMOLT_RUSTFS_SECRET_KEY` | yes | RustFS secret; access key defaults to `protomolt` |
| `PROTOMOLT_KEYCLOAK_ADMIN_PASSWORD` | yes | Keycloak bootstrap admin password |
| `PROTOMOLT_KEYCLOAK_CLIENT_SECRET` | yes | Secret for the `protomolt-coordinator` service client |

The coordinator and Keycloak defaults reserve NAS ports `19901` through
`19904`. RustFS uses `31900`, a separate host range that avoids the shared
development-services ports. Every binding and hostname can be overridden with
the corresponding variable in `compose.yml`.

The server starts with a durable Git registry, durable service-profile and
recipe workspaces, and the demo seed. An MCP client can therefore connect and
immediately discover useful resources and verbs. Registered gRPC targets need
only be routable from the NAS; they do not have to share this Compose network.
Unauthenticated reflection targets can be registered now. Authenticated
service-profile reflection remains a bounded delegatable work item in
`docs/design/grpc-recipe-workbench.md`; the coordinator keeps its API token
enabled until that host credential boundary lands.

Keycloak uses its embedded development database on a persistent volume and is
intentionally started with `start-dev`. It is suitable for this private
development coordinator, not a public production identity service. Startup
imports the `protomolt` realm only when it does not already exist.

Back up the `protomolt-data`, `rustfs-data`, and `keycloak-data` volumes before
upgrading or replacing the stack. The first holds the schema Git history,
service registrations, recipes, and evidence.
