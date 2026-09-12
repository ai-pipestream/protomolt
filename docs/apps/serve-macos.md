# Running on a Mac (.dmg)

Each release attaches `protomolt-serve-<version>-macos-aarch64.dmg`: the same
one-process server the [Docker image](docker.md) runs, packaged for Apple
silicon with an embedded Java runtime, so nothing needs to be installed first.
It is built by `.github/workflows/macos-serve-dmg.yml` on a GitHub-hosted
`macos-15` runner with jpackage over the `:protomolt-serve:installDist`
distribution (console assets included, exactly like the Docker and release-zip
builds). The release workflow calls it with the released version; it can also
be dispatched manually from any ref, in which case the DMG is a workflow
artifact named from `git describe` instead of a release asset.

## Install and run

Open the DMG and drag `protomolt-serve.app` to Applications (or anywhere).
The server is a terminal program, not a windowed app, so run its launcher
directly:

```shell
/Applications/protomolt-serve.app/Contents/MacOS/protomolt-serve --demo
```

`--demo` seeds a throwaway registry, schema, and workflow; the console answers
on `http://localhost:8080/console` and gRPC on `localhost:9090`, the same
surfaces the [Docker guide](docker.md) tables. `--help` lists every flag.

If the build is unsigned (no Developer ID in the release notes), Gatekeeper
quarantines the downloaded app; clear it once after copying:

```shell
xattr -dr com.apple.quarantine /Applications/protomolt-serve.app
```

## Why jpackage

`protomolt-serve` is a JVM application: the Docker image is a thin JRE layer
over `installDist`, and the release zip requires a JRE 25+ on the host. For a
native Mac artifact, jpackage bundles those same jars with a jlink-built
runtime (`ALL-DEFAULT` modules, matching the full JRE the container uses) into
one self-contained app, and — unlike hand-rolled DMG assembly — signs the
whole bundle (launcher, jars, native libraries) with the hardened runtime and
JVM-suited entitlements in one pass when signing is enabled. GraalVM
native-image is not attempted here: serve's surface (gRPC, HTTP hosts, MCP,
OpenVINO/OpenAI inference providers, JDBC) has no maintained reachability
metadata, unlike the CLI.

## Signing and notarization

Optional, and on automatically when the repository secrets exist; none are
required to produce a working DMG.

| Secret | Holds |
|---|---|
| `MACOS_SIGNING_CERT_P12` | base64 of a Developer ID Application certificate exported as .p12 |
| `MACOS_SIGNING_CERT_PASSWORD` | the .p12 password |
| `MACOS_SIGNING_IDENTITY` | optional identity string; defaults to the first Developer ID Application in the imported certificate |
| `APPLE_NOTARY_KEY_ID` | App Store Connect API key id |
| `APPLE_NOTARY_ISSUER_ID` | App Store Connect issuer id |
| `APPLE_NOTARY_KEY_P8` | base64 of the App Store Connect API private key (.p8) |

With the first two set, jpackage signs the app bundle with the hardened
runtime; with all six, the DMG is also notarized with `notarytool` and
stapled, so Gatekeeper accepts it with no prompts. Without them the DMG is
unsigned and the quarantine clear above applies.
