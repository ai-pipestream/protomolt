# Authoring policy seed

Build the sample launcher with `./gradlew :samples:installAuthoringPolicySeed`. Run it before starting the authoring coordinator:

```text
authoring-policy-seed /data/serve/workflows /data/serve/workflows/artifacts fixture:9778 /data/serve/workflows/policy.sha256
```

The sample fixture target is fixed to the supplied `host:port`. The seed uses operation UUID `00000000-0000-4000-8000-000000000442` for the independent reviewer fixture, creates the complete descriptor/import closure, and stores the pinned request, expected response, and policy as unredacted protobuf artifacts. Serve uses `<workspace>/artifacts` for its artifact repository.

The workspace holds `.authoring-policy-seed-v1.json`, an intent manifest binding the target, paths, and four artifact hashes. The seed publishes and syncs this intent before writing artifacts. It writes `policy.sha256` atomically only after verifying every stored reference and byte string. The SHA file contains lowercase hex and a trailing newline; pass its contents to Serve's `--workflow-authoring-policy-sha256` option. An interrupted setup with a matching intent and no ready SHA can resume. A changed target or path, corrupt manifest, mismatched existing artifact, or missing artifact after readiness fails without replacing the pinned policy.
