#!/usr/bin/env python3
"""In-process contract tests for the Nano1 mesh publisher."""

import importlib.util
import os
import pathlib
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("nano1-mesh-publisher.py")
SPEC = importlib.util.spec_from_file_location("nano1_mesh_publisher", SCRIPT)
PUBLISHER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLISHER)


class FakeClient:
    def __init__(self, profile_registered=True):
        self.calls = []
        self.profile_registered = profile_registered

    def call(self, name, arguments):
        self.calls.append((name, arguments))
        if name == "service-inspect":
            if not self.profile_registered:
                raise RuntimeError(
                    "service-inspect: {\"error\":\"not-found\",\"message\":"
                    "\"service profile 'nano1-tei' was not found\"}")
            return {"profile": {"schemaSource": {
                "descriptorFingerprint": "a" * 64,
            }}}
        if name == "mesh-snapshot":
            return {"snapshot": {"snapshotSeq": "8", "fingerprint": "b" * 64}}
        return {"ok": True}

    def arguments(self, name):
        return [arguments for called, arguments in self.calls if called == name]


class Nano1MeshPublisherTest(unittest.TestCase):
    def test_publishes_canonical_durations_and_only_reachable_endpoints(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.dict(os.environ, {
                    "PROTOMOLT_NANO1_MESH_TTL_SECONDS": "90",
                    "PROTOMOLT_MESH_CLUSTER_ID": "protomolt",
                }, clear=True), \
                mock.patch.object(PUBLISHER, "tailnet_ip", return_value="100.64.3.106"), \
                mock.patch.object(PUBLISHER, "probe_host"), \
                mock.patch.object(PUBLISHER, "probe_tei", return_value={
                    "maxConcurrentRequests": 4,
                }):
            state = PUBLISHER.PublisherState(
                pathlib.Path(temporary) / "publisher-state.json")
            result = PUBLISHER.publish_once(client, state, pathlib.Path(temporary))

        node = client.arguments("mesh-node-register")[0]["advertisement"]
        heartbeat = client.arguments("mesh-node-heartbeat")[0]["presence"]
        self.assertEqual("90s", node["ttl"])
        self.assertEqual("90s", heartbeat["ttl"])
        self.assertEqual(["tei-grpc"], [entry["endpointId"]
                                       for entry in node["endpoints"]])
        self.assertCountEqual(
            ["nano1-tei", "nano1-arm64-builder"],
            result["renewed"],
        )
        self.assertEqual({}, result["misconfigured"])

    def test_a_missing_service_profile_is_reported_as_misconfiguration(self):
        # This is the failure that actually happened. The TEI gate passed, the GPU was
        # healthy the whole time, and the profile was gone because the coordinator's
        # volumes had been recreated. Reported beside a failed hardware gate it reads
        # as transient, so the GPU sat outside the mesh while the node looked fine.
        client = FakeClient(profile_registered=False)
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.dict(os.environ, {
                    "PROTOMOLT_NANO1_MESH_TTL_SECONDS": "90",
                    "PROTOMOLT_MESH_CLUSTER_ID": "protomolt",
                }, clear=True), \
                mock.patch.object(PUBLISHER, "tailnet_ip", return_value="100.64.3.106"), \
                mock.patch.object(PUBLISHER, "probe_host"), \
                mock.patch.object(PUBLISHER, "probe_tei", return_value={
                    "maxConcurrentRequests": 4,
                }):
            state = PUBLISHER.PublisherState(
                pathlib.Path(temporary) / "publisher-state.json")
            result = PUBLISHER.publish_once(client, state, pathlib.Path(temporary))

        # The builder still renews: one withheld processor must not suppress the other.
        self.assertEqual(["nano1-arm64-builder"], result["renewed"])
        # And the reason is filed where an operator can act on it, not among the
        # health-gate failures that fix themselves.
        self.assertEqual({}, result["notRenewed"])
        self.assertIn("nano1-tei", result["misconfigured"])
        self.assertIn("register", result["misconfigured"]["nano1-tei"])


if __name__ == "__main__":
    unittest.main()
