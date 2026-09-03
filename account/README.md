# account/ — the tenant root: account CRUD + activation + drive provisioning

The platform's account service: accounts as first-class tenants, their
lifecycle (create → activate ↔ deactivate), provisioning of every account's
two platform drives on repo-service, and an `account-events` Kafka feed — no
framework, blocking style, virtual threads. A peer process of repo-service:
separate database, separate deploy/scale story, coupled only through
repo-service's wire contract.

## What it is

An account is the tenancy key the whole platform hangs off: drives,
documents, and ledger rows are account-scoped, and `account_id` is required
on every repo-service request. This service owns the account itself — one
row per account in its own Postgres database — and the two moments that
make an account real:

1. **Creation.** `CreateAccount` provisions the account's `intake`
   (`INTAKE`) and `pipeline` (`PIPELINE`) drives by calling repo-service's
   `DriveService.CreateDrive`, then commits the account row. Provisioning
   rides creation (not activation) because the account is where tenancy
   begins: the drives are part of what it means for an account to exist.
2. **Activation.** `ActivateAccount` returns a SUSPENDED/DEACTIVATED account
   to ACTIVE and re-ensures the drives — repo's CreateDrive is idempotent by
   deterministic drive id, so this converges an account created while
   repo-service was unreachable instead of duplicating anything.

Ordering at every commit point: repo-service RPCs happen **first**, outside
any database transaction (never hold a transaction open across an RPC); if
repo is unreachable the RPC fails UNAVAILABLE and nothing commits. The
account mutation and its outbox event then commit **in one transaction**.
No-op transitions (activating an ACTIVE account, deactivating a DEACTIVATED
one) write nothing, fire nothing, and skip the drive calls entirely — an
idempotent re-activation provokes no repo traffic.

## Tenets

1. **No framework.** No Quarkus, no Spring. Pure Java 21+, constructor
   wiring, a small `main`. `AccountServices` is the SPI.
2. **Virtual threads everywhere.** Blocking style, one virtual thread per
   unit of work. No reactive types in APIs.
3. **`account_id` is explicit and caller-minted.** The string id IS the
   primary key — it is baked into identity hashes and storage prefixes
   platform-wide, so it is never aliased behind a surrogate id and never
   defaulted.
4. **Peer, not module.** Account data never shares repo's schema, Flyway
   history, or process. The only coupling is the `protomolt-repo-proto`
   stub for drive provisioning.
5. **Plain JDBC where JPA buys nothing.** The account schema is two flat
   tables; the repo ledger's shape (HikariCP pool sized to the database,
   Flyway owning the schema, fail-fast boot) is kept, the mapping layer is
   not.

## Modules

| Module | Gradle project | Role |
|--------|----------------|------|
| `account/proto` | `:protomolt-account-proto` | The wire contract: `Account`, `AccountService`, and the `account-events` outbox payloads |
| `account/service` | `:protomolt-account-service` | The service set: `AccountStore`/`IdentityResolver` SPIs + Postgres default, gRPC impl, drive provisioning, the outbox relay, `AccountServices` wiring |

## SPI seams

- **`AccountStore`** (`store` package) — CRUD + paged list + status
  transitions. Every method rides the caller's `Connection`: an account
  mutation and its outbox event must commit atomically, so the store never
  opens its own connections. `JdbcAccountStore` is the Postgres default
  (Flyway migrations in `account/service/src/main/resources/db/migration/account`:
  V1 `accounts`, V2 `account_events_outbox`).
- **`IdentityResolver`** (`identity` package) — the seam for external
  identity systems (Salesforce/AD/OAuth adapters later): resolve an external
  principal (`identity` + `identity_type`) to an `account_id`. The default
  is `DirectAccountIdentityResolver`, a pass-through for direct account ids
  (blank or `"account-id"` type). No RPC consumes the resolver yet — typed
  principals arrive with the ACL proto; the seam is designed and tested
  first.

## Kafka eventing (transactional outbox)

Same pattern as repo's document events. Every account commit point (create,
activate, deactivate) inserts one `AccountEvent` protobuf row into
`account_events_outbox` **in the same transaction** as the account mutation;
a single virtual-thread relay loop drains PENDING rows to the
`account-events` topic, keyed by `account_id` (partition-ordered per
account), through the protomolt serde pinned to the `AccountEvent` wrapper
type — no schema registry required on either side. Delivery is
at-least-once: publish precedes the PUBLISHED transition, so a crash
republishes on restart; consumers dedupe on `event_id` (the outbox row id).
Attempts are capped (10) and land the row FAILED — the DLQ is the row
itself.

Unset `DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS` = eventing off: no
outbox writes, no relay, no producer, zero overhead.

## Running

`AccountServiceMain` boots from the environment and serves gRPC (Netty,
health + reflection) plus the background relay loop. To embed in-JVM
instead, use `AccountServices.build(config)` + `startInProcess(name)`.

| Env var | Default | Meaning |
|---------|---------|---------|
| `DOCUMENT_PLATFORM_ACCOUNT_GRPC_PORT` | `9091` | gRPC listen port |
| `DOCUMENT_PLATFORM_ACCOUNT_JDBC_URL` / `_USERNAME` / `_PASSWORD` / `_POOL_SIZE` | local Postgres `accounts` db | Account store database (see `AccountStoreConfig`) |
| `DOCUMENT_PLATFORM_REPO_GRPC_TARGET` | `localhost:9090` | repo-service's gRPC target (plaintext `host:port`) for drive provisioning; the `inprocess:<name>` prefix selects the in-process transport instead (same-JVM embedding, tests) |
| `DOCUMENT_PLATFORM_ACCOUNT_KAFKA_BOOTSTRAP_SERVERS` | _(none)_ | Kafka bootstrap servers; unset = eventing off (no outbox writes, no relay, no producer) |
| `DOCUMENT_PLATFORM_ACCOUNT_KAFKA_TOPIC` | `account-events` | The account-events topic the relay publishes to |
| `DOCUMENT_PLATFORM_ACCOUNT_SCHEMA_REGISTRY_URL` | _(none)_ | Confluent-compatible schema registry for the relay's serde; unset = registry-free (frames stamp schema id 0, which only protomolt consumers resolve) |
| `DOCUMENT_PLATFORM_ACCOUNT_LIFECYCLE_ENABLED` | `true` | Run the background relay loop when `startLifecycle()` is called |
| `DOCUMENT_PLATFORM_ACCOUNT_RELAY_INTERVAL_MS` | `5000` | Relay-drain idle backoff; a non-empty drain loops again immediately |

## API surface

`AccountService` (`account/proto`, package `ai.protomolt.proto.account.v1`),
unary-only and grpc-web friendly:

- `CreateAccount` — provisions `intake` + `pipeline` drives, then commits
  the ACTIVE row + `AccountCreated` event. Duplicate id → ALREADY_EXISTS.
- `GetAccount` — by id; unknown → NOT_FOUND.
- `ListAccounts` — paged (`limit` + offset-style `continuation_token`,
  `total_count`), optional `status_filter`.
- `ActivateAccount` — SUSPENDED/DEACTIVATED → ACTIVE; re-ensures the drives,
  fires `AccountActivated`. No-op on an ACTIVE account.
- `DeactivateAccount` — → DEACTIVATED, fires `AccountDeactivated`. No-op on
  a DEACTIVATED account. Drives and data are retained.

## Building and testing

```
./gradlew :protomolt-account-proto:build :protomolt-account-service:build
```

The suites run against testcontainers PostgreSQL 17 (Docker required):
`JdbcAccountStoreIT` (CRUD, transitions, paging), `AccountServiceIT` (the
full stack in-process against a fake recording `DriveService` — drive
provisioning calls, idempotent re-activation, outbox co-commit, repo-outage
semantics), and `AccountEventRelayIT` (relay drains with a mock producer —
no Kafka container). `AccountServiceConfigTest` covers the env contract.
