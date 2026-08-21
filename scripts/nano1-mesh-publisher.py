#!/usr/bin/env python3
"""Health-gated Nano1 mesh advertisements over ProtoMolt MCP."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.request


GRPCURL_IMAGE = (
    "fullstorydev/grpcurl@sha256:"
    "3baecd2e73cd4c7e9c01e75af8f08d14c0c13a5767dc86db4eeffc24fae593d6"
)
MODEL_ID = "BAAI/bge-small-en-v1.5"
MODEL_SHA = "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def timestamp(value: dt.datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def run(*command: str, timeout: int = 30) -> str:
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.stdout


def tailnet_ip() -> str:
    configured = os.environ.get("NANO1_TAILSCALE_IP")
    if configured:
        return configured
    return run("tailscale", "ip", "-4", timeout=10).strip().splitlines()[0]


def probe_host(repo_root: pathlib.Path) -> None:
    run(str(repo_root / "deploy/nano1/check-host.sh"), timeout=45)


def probe_tei(target: str) -> dict:
    inspect = json.loads(run(
        "docker", "inspect", "protomolt-tei-jetson", timeout=10
    ))[0]
    if not inspect["State"]["Running"] or inspect["HostConfig"]["Runtime"] != "nvidia":
        raise RuntimeError("TEI is not running under the NVIDIA runtime")
    info = json.loads(run(
        "docker", "run", "--rm", "--network", "host", GRPCURL_IMAGE,
        "-plaintext", "-d", "{}", target, "tei.v1.Info/Info", timeout=30,
    ))
    if info.get("modelId") != MODEL_ID or info.get("modelSha") != MODEL_SHA:
        raise RuntimeError("TEI model identity does not match the pinned deployment")
    embedding = json.loads(run(
        "docker", "run", "--rm", "--network", "host", GRPCURL_IMAGE,
        "-plaintext", "-d", json.dumps({
            "inputs": "Nano1 health-gated mesh lease", "normalize": True
        }), target, "tei.v1.Embed/Embed", timeout=30,
    ))
    vector = embedding.get("embeddings", [])
    if len(vector) != 384:
        raise RuntimeError("TEI did not return a 384-dimensional embedding")
    norm = sum(float(value) * float(value) for value in vector) ** 0.5
    if abs(norm - 1.0) > 0.01:
        raise RuntimeError("TEI embedding is not normalized")
    return info


class McpClient:
    def __init__(self, endpoint: str, token: str) -> None:
        self.endpoint = endpoint
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": "Bearer " + token,
        }
        headers, _ = self._post({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "nano1-mesh-publisher", "version": "1"},
            },
        })
        session = headers.get("Mcp-Session-Id") or headers.get("mcp-session-id")
        if not session:
            raise RuntimeError("ProtoMolt returned no MCP session id")
        self.headers["Mcp-Session-Id"] = session
        self.headers["MCP-Protocol-Version"] = "2025-06-18"
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _post(self, payload: dict) -> tuple[dict, str]:
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps(payload).encode(),
            headers=self.headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=45) as response:
            return dict(response.headers), response.read().decode()

    @staticmethod
    def _envelope(raw: str) -> dict:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            events = [line[5:].strip() for line in raw.splitlines()
                      if line.startswith("data:")]
            if not events:
                raise RuntimeError("ProtoMolt returned neither JSON nor an MCP event")
            return json.loads(events[-1])

    def call(self, name: str, arguments: dict) -> dict:
        _, raw = self._post({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        })
        envelope = self._envelope(raw)
        if "error" in envelope:
            raise RuntimeError(envelope["error"].get("message", "MCP call failed"))
        result = envelope.get("result", {})
        if result.get("isError"):
            content = result.get("content", [])
            detail = content[0].get("text", "tool failed") if content else "tool failed"
            raise RuntimeError(f"{name}: {detail}")
        return result.get("structuredContent", {})


class PublisherState:
    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        try:
            self.values = json.loads(path.read_text())
        except FileNotFoundError:
            self.values = {}
        previous = int(self.values.get("epoch", 0))
        self.values["epoch"] = max(previous + 1, int(time.time()))
        self.values.setdefault("nodeSeq", 0)
        self.values.setdefault("heartbeatSeq", 0)
        self.values.setdefault("processors", {})
        self.values.setdefault("capacities", {})

    @property
    def epoch(self) -> int:
        return int(self.values["epoch"])

    def next(self, group: str, name: str | None = None) -> int:
        if name is None:
            self.values[group] = int(self.values.get(group, 0)) + 1
            return int(self.values[group])
        values = self.values[group]
        values[name] = int(values.get(name, 0)) + 1
        return int(values[name])

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.values, sort_keys=True) + "\n")
        temporary.replace(self.path)


def processor(processor_id: str, kind: str, capabilities: list[str], now: dt.datetime,
              expiry: dt.datetime, epoch: int, seq: int, provider: str = "",
              model: str = "", model_version: str = "",
              schema_fingerprint: str = "") -> dict:
    advertisement = {
        "processorId": processor_id,
        "nodeId": "nano1",
        "kind": kind,
        "capabilities": capabilities,
        "leaseEpoch": str(epoch),
        "leaseExpiresAt": timestamp(expiry),
        "advertisedAt": timestamp(now),
        "seq": str(seq),
        "nodeEpoch": str(epoch),
        "capabilityDetails": [
            {"name": name, "description": description}
            for name, description in {
                "embedding": "Normalized dense text embeddings over reflected gRPC.",
                "cuda": "GPU execution is required by the local readiness gate.",
                "cuda-sm87": "NVIDIA compute capability 8.7 is required by the gate.",
                "dimensions-384": "Dense embedding vectors contain 384 float values.",
                "arm64-build-capacity": "Trusted native linux/arm64 build host capacity.",
            }.items() if name in capabilities
        ],
    }
    if provider:
        advertisement["provider"] = provider
    if model:
        advertisement["model"] = model
    if model_version:
        advertisement["modelVersion"] = model_version
    if processor_id == "nano1-tei":
        advertisement["acceptedSchemas"] = [{
            "typeName": "tei.v1.EmbedRequest",
            "descriptorFingerprint": schema_fingerprint,
        }]
    return advertisement


def publish_once(client: McpClient, state: PublisherState,
                 repo_root: pathlib.Path) -> dict:
    now = utc_now()
    ttl_seconds = int(os.environ.get("PROTOMOLT_NANO1_MESH_TTL_SECONDS", "90"))
    expiry = now + dt.timedelta(seconds=ttl_seconds)
    ip = tailnet_ip()
    tei_target = os.environ.get("NANO1_TEI_TARGET", f"{ip}:8083")

    probe_host(repo_root)
    healthy: dict[str, object] = {}
    failures: dict[str, str] = {}
    for name, probe in {
        "nano1-tei": lambda: probe_tei(tei_target),
        "nano1-arm64-builder": lambda: {"architecture": "linux/arm64"},
    }.items():
        try:
            healthy[name] = probe()
        except Exception as error:  # one failed processor must not suppress the others
            failures[name] = str(error)

    tei_schema_fingerprint = ""
    if "nano1-tei" in healthy:
        try:
            inspection = client.call("service-inspect", {"name": "nano1-tei"})
            tei_schema_fingerprint = inspection["profile"]["schemaSource"][
                "descriptorFingerprint"
            ]
            if len(tei_schema_fingerprint) != 64:
                raise RuntimeError("registered TEI schema fingerprint is invalid")
        except Exception as error:
            healthy.pop("nano1-tei")
            failures["nano1-tei"] = str(error)

    node_seq = state.next("nodeSeq")
    heartbeat_seq = state.next("heartbeatSeq")
    processor_sequences = {
        processor_id: state.next("processors", processor_id)
        for processor_id in healthy
    }
    capacity_sequences = {
        processor_id: state.next("capacities", processor_id)
        for processor_id in healthy
    }
    # Sequence positions are durable locally before any remote mutation becomes
    # visible. A crash may skip a number, which is allowed; it must never reuse a
    # number with changed timestamps after an ambiguous network failure.
    state.save()
    endpoints = []
    node_capabilities = ["arm64", "cuda"]
    if "nano1-tei" in healthy:
        endpoints.append({
            "endpointId": "tei-grpc",
            "address": tei_target,
            "tlsMode": "TLS_MODE_DISABLED",
            "direct": True,
        })
        node_capabilities.append("grpc-reflection")
    node = {
        "nodeId": "nano1",
        "clusterId": os.environ.get("PROTOMOLT_MESH_CLUSTER_ID", "protomolt"),
        "capabilities": node_capabilities,
        "endpoints": endpoints,
        "advertisedAt": timestamp(now),
        "ttl": f"{ttl_seconds}s",
        "epoch": str(state.epoch),
        "seq": str(node_seq),
    }
    client.call("mesh-node-register", {"advertisement": node})
    client.call("mesh-node-heartbeat", {"presence": {
        "nodeId": "nano1",
        "clusterId": node["clusterId"],
        "state": "PRESENCE_STATE_ACTIVE",
        "lastHeartbeatAt": timestamp(now),
        "heartbeatSeq": str(heartbeat_seq),
        "ttl": f"{ttl_seconds}s",
        "expiresAt": timestamp(expiry),
        "nodeEpoch": str(state.epoch),
    }})

    definitions = {
        "nano1-tei": (
            "PROCESSOR_KIND_GRPC_SERVICE",
            ["embedding", "dimensions-384", "cuda", "cuda-sm87", "grpc-reflection"],
            "tei", MODEL_ID, MODEL_SHA,
            int(healthy.get("nano1-tei", {}).get("maxConcurrentRequests", 4)),
        ),
        "nano1-arm64-builder": (
            "PROCESSOR_KIND_DETERMINISTIC", ["arm64-build-capacity"],
            "", "", "", 1,
        ),
    }
    renewed: list[str] = []
    for processor_id in healthy:
        kind, capabilities, provider, model, version, limit = definitions[processor_id]
        client.call("mesh-processor-register", {"advertisement": processor(
            processor_id, kind, capabilities, now, expiry, state.epoch,
            processor_sequences[processor_id], provider, model, version,
            tei_schema_fingerprint if processor_id == "nano1-tei" else "",
        )})
        client.call("mesh-capacity-update", {"capacity": {
            "nodeId": "nano1",
            "processorId": processor_id,
            "maxInFlight": limit,
            "inFlight": 0,
            "observedAt": timestamp(now),
            "seq": str(capacity_sequences[processor_id]),
            "sourceEpoch": str(state.epoch),
        }})
        renewed.append(processor_id)

    client.call("mesh-sweep", {})
    snapshot = client.call("mesh-snapshot", {})
    return {"renewed": renewed, "notRenewed": failures,
            "snapshot": snapshot.get("snapshot", {})}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="publish one health-gated lease set and exit")
    args = parser.parse_args()
    if os.environ.get("PROTOMOLT_NANO1_MESH_LIVE") != "1":
        print("SKIP: set PROTOMOLT_NANO1_MESH_LIVE=1 on Nano1")
        return 0
    token = os.environ.get("PROTOMOLT_MCP_TOKEN")
    if not token:
        raise SystemExit("PROTOMOLT_MCP_TOKEN is required")
    endpoint = os.environ.get(
        "PROTOMOLT_MCP_ENDPOINT", "https://protomolt.rokkon.com/mcp"
    )
    state_path = pathlib.Path(os.environ.get(
        "PROTOMOLT_NANO1_MESH_STATE",
        "/var/lib/protomolt-runner/mesh/nano1.json",
    ))
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    state = PublisherState(state_path)
    interval = int(os.environ.get("PROTOMOLT_NANO1_MESH_INTERVAL_SECONDS", "30"))
    while True:
        try:
            result = publish_once(McpClient(endpoint, token), state, repo_root)
            print(json.dumps({
                "renewed": result["renewed"],
                "notRenewed": result["notRenewed"],
                "snapshotSeq": result["snapshot"].get("snapshotSeq"),
                "snapshotFingerprint": result["snapshot"].get("fingerprint"),
            }, sort_keys=True), flush=True)
        except Exception as error:
            print(f"Nano1 mesh publication failed: {error}", file=sys.stderr, flush=True)
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
