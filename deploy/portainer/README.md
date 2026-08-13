# Always-on coordinator in Portainer

`compose.yml` runs the persistent ProtoMolt coordinator on the NAS Portainer
endpoint. Portainer is the sole lifecycle owner. The existing DJL stack remains
on the GPU host and is not duplicated here.

The serve and repo-service images default to the `edge` tag published by the
Docker Publish workflow. Updating the coordinator is an explicit pin: after a
new image has been built from `main` and verified, set
`PROTOMOLT_SERVE_IMAGE` (or `PROTOMOLT_REPO_SERVICE_IMAGE`) to
`ghcr.io/ai-pipestream/protomolt-serve:edge@sha256:<digest>` in the Portainer
stack variables and redeploy. To refresh `edge` itself, dispatch the Docker
Publish workflow against `main`.

The stack provides:

| Service | LAN endpoint | Purpose |
|---|---|---|
| ProtoMolt MCP/HTTP | `http://nas:19902/mcp` | MCP, REST, OpenAPI, Swagger, and console |
| ProtoMolt gRPC | `nas:19903` | ProtoMolt API and reflection |
| ProtoMolt registry | `http://nas:19904` | Git-backed Confluent-compatible schema registry |
| RustFS | `http://nas:31900` | Persistent S3-compatible artifact storage |
| Keycloak | `http://nas:19901` | Local OIDC identity provider |
| repo-service | internal only | Claim-check document store backing the delegation transcript |
| repo-postgres | internal only | repo-service ledger database |

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
| `PROTOMOLT_TASK_CONSOLE_TOKEN` | yes | Separate browser login for the bounded task console; generate with `openssl rand -base64 32` |
| `PROTOMOLT_RUSTFS_SECRET_KEY` | yes | RustFS secret; access key defaults to `protomolt` |
| `PROTOMOLT_KEYCLOAK_ADMIN_PASSWORD` | yes | Keycloak bootstrap admin password |
| `PROTOMOLT_KEYCLOAK_CLIENT_SECRET` | yes | Secret for the `protomolt-coordinator` service client |
| `PROTOMOLT_TRANSCRIPT_KEY` | yes | Base64-encoded 32-byte AES key encrypting the delegation transcript; generate with `openssl rand -base64 32` |
| `PROTOMOLT_REPO_DB_PASSWORD` | yes | Password for the repo-service ledger database; user and database default to `documents` |
| `PROTOMOLT_SERVE_IMAGE` | no | Serve image override; pin with `ghcr.io/ai-pipestream/protomolt-serve:edge@sha256:<digest>` |
| `PROTOMOLT_REPO_SERVICE_IMAGE` | no | repo-service image override, same pinning form as `PROTOMOLT_SERVE_IMAGE` |
| `PROTOMOLT_DELEGATION_REPO_DRIVE` | no | Repository drive of the transcript blob; defaults to `protomolt` and must match the drive repo-init creates |
| `PROTOMOLT_REPO_DB_USER` / `PROTOMOLT_REPO_DB_NAME` | no | Ledger database user and name; both default to `documents` |
| `PROTOMOLT_TASK_CONSOLE_SESSION_SECONDS` | no | Browser session lifetime in seconds; defaults to 43200 (12 hours), maximum 604800 |

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
`docs/transform/recipes.md`; the coordinator keeps its API token
enabled until that host credential boundary lands.

The coordinator runs with durable delegation transcripts
(`--delegation-repo-endpoint repo-service:9090`). The serve process validates
and encrypts each transcript snapshot locally with AES-256-GCM under the key
that `PROTOMOLT_TRANSCRIPT_KEY` holds, then stores the ciphertext as one blob
through the repo-service (`PutBlob`/`GetBlob`) on the drive named by
`PROTOMOLT_DELEGATION_REPO_DRIVE` (default `protomolt`, created by the
repo-init container against the existing `protomolt` RustFS bucket). The
repo-service, its PostgreSQL ledger, and RustFS only ever see ciphertext. A
coordinator restart restores every task, event cursor, and worker sequence
scope, so a re-registering agent host resumes where the record left off.
Losing `PROTOMOLT_TRANSCRIPT_KEY` makes the stored transcript unreadable;
rotate it only by retiring the old transcript object.

The task console is available at
`https://protomolt.rokkon.com/console/tasks`. Its login token is deliberately
different from `PROTOMOLT_API_TOKEN`. A successful login creates an
HttpOnly, Secure, SameSite=Strict cookie held in the serve process. Browser
JavaScript can reach only the task, worker, transcript, and task-message API.
The general registry and serve proxies remain disabled while the coordinator
uses its process API token. Restarting serve invalidates browser sessions but
does not affect the repository-backed delegation transcript.

Keycloak uses its embedded development database on a persistent volume and is
intentionally started with `start-dev`. It is suitable for this private
development coordinator, not a public production identity service. Startup
imports the `protomolt` realm only when it does not already exist.

Back up the `protomolt-data`, `rustfs-data`, `keycloak-data`, and
`repo-postgres-data` volumes before upgrading or replacing the stack. The first
holds the schema Git history, service registrations, recipes, and evidence.
`rustfs-data` and `repo-postgres-data` together hold the encrypted delegation
transcript; both are useless without `PROTOMOLT_TRANSCRIPT_KEY`, so back the
key up with the same care as the volumes.
