# ProtoMolt starter

Download and unzip `protomolt-starter-<version>.zip` from the matching ProtoMolt
release. The bundle's `.env` pins all three application images by digest.
Only Docker Engine and the Docker Compose plugin are needed on the host. From
the extracted directory, run:

```sh
docker compose up -d
docker compose ps
```

The first start creates separate random operator, browser-console, and correction
tokens in separate persistent volumes. The correction container can read only
its own token, and the ACP container can read only the operator token. It also
creates persistent `serve-data` (registry, service and workflow definitions,
and run artifacts) and
`correction-data` (signing identity, trust, correction runs and receipts) volumes.
Restarting or upgrading with `docker compose up -d` keeps those volumes. Do not
run `docker compose down -v` if you want to keep the installation.
For a second independent installation, set a different `COMPOSE_PROJECT_NAME`
in its `.env` and retain that name through upgrades.

The guided console starts at <http://127.0.0.1:8080/console/start>.
Retrieve its login token locally:

```sh
docker compose exec -T serve cat /run/console-secret/token
```

This token is for the browser console only. The operator API token is different.
For a local gRPC or MCP client, retrieve the operator token with
`docker compose exec -T serve cat /run/operator-secret/token`, then supply it
as `Authorization: Bearer <token>`. Do not paste either token into logs or issues.
The loopback endpoints are HTTP/MCP `http://127.0.0.1:8080/mcp`, gRPC
`127.0.0.1:9090`, health `http://127.0.0.1:8080/health`, OpenAPI
`http://127.0.0.1:8080/openapi.json`, and Swagger UI
`http://127.0.0.1:8080/docs`. Only the HTTP and gRPC ports are published,
and only to the local machine. Set `PROTOMOLT_HTTP_PORT` and
`PROTOMOLT_GRPC_PORT` in the bundle's `.env` to choose other local ports.

The default correction sample uses a deterministic fixture provider. It needs
no account or model credential. In the console, follow the contact-correction
guide, save the outcome and receipt, then restart the stack and open the same
run again. To export and check a completed run outside the platform, run the
helper using the run ID shown in the console:

```sh
./verify-receipt.sh <run-id>
```

The helper exports the signed records, public trust snapshot, run evidence,
outcome files and only the content-addressed artifacts referenced by the selected
signed records. It restarts the services,
checks that the signed bytes remain, and runs the bundled independent verifier
against the records and artifact bytes with Docker networking disabled. It
reuses the already-pulled correction image only for its JRE; the verifier uses
its own standalone JAR and no platform service. The exported files stay under
`evidence/<run-id>` for separate inspection. Artifact and run bytes may contain
submitted contact data, so keep that directory private. Docker image
publication and the browser guide must both be available in the release before
this bundle is advertised as ready.

ACP uses stdio and has no network listener. Configure an ACP-capable client to
launch `docker compose run --rm -T -i acp` from this directory. The container uses
the same persistent operator identity to connect to `serve:9090` internally.
The client must keep stdin and stdout attached; do not allocate a TTY.

For a private remote inference bridge, copy your bridge API token to a local
file named `inference-token`, run `chmod 600 inference-token`
followed by `sudo chown 10001:10001 inference-token`, and set
`PROTOMOLT_INFERENCE_TARGET` in `.env` to the bridge's private `host:port`.
Then use `docker compose -f compose.yml -f compose.live.yml up -d`. This is
separate from the fixture starter and requires a reachable, authenticated
inference service. The live override uses a separate correction volume because
its reviewed workflow differs from the fixture. Keep the token file out of the
bundle and version control.
