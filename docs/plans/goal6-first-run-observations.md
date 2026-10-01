# Goal 6 first-run observations

## Automated rehearsal, 2026-10-01

The published `authoring-starter-d209f0551566` bundle was downloaded anonymously
into a new temporary directory and checked against its published SHA-256 file.
Compose started under a new project, `protomolt-goal6-first-run`, with fresh
volumes and loopback ports 18540 and 19540. The host already had Docker,
Compose 5.5.1, Node, Chromium, and an image cache. This is not a cold-machine
or unfamiliar-human setup measurement.

- Download started at 20:02:48 UTC.
- Health was observed UP at 20:03:44 UTC.
- Browser completion was observed at 20:03:53 UTC.
- Total observed elapsed time was 65 seconds, including gaps between commands.
  This is not isolated startup time, workflow latency, or a user completion-time
  benchmark. Those need separately instrumented measurements.
- The release's browser script returned `authoring: accepted`, `job: completed`,
  `provider: scripted`, and `liveModel: false`.
- Operation: `94181e0a-a892-4d68-96db-5a8701d7a72b`.
- Job: `1e9a71a1-3393-48bf-bb39-40a1fac899bd`.
- Visual inspection confirmed accepted state, completed execution, authorization
  and input fingerprints, and three successful contract checks.
- No paid model provider was used. Hosting cost and cold-download bytes were
  not measured. This result cannot support a live-provider cost claim.

Local raw evidence is in `/tmp/protomolt-goal6-first-run.VE8zdc`: checksum,
pull and startup logs, service status, timestamps, browser result, and screenshot
inside the extracted bundle directory. These temporary artifacts are not durable
public evidence. The immutable release and its prior architecture qualification
are linked from [the release report](goal5-release-qualification.md).

## Friction found and current changes

- The README did not direct new users to the qualified bundle. A first-workflow
  tutorial now links the exact assets, checksum, browser steps, expected service
  states, and failure diagnosis. It distinguishes the scripted demonstration from
  arbitrary workflow authoring. The tutorial still needs an unfamiliar reader.
- Two token types exist; only the browser launch token can launch this example.
  The walkthrough now names the correct token and explains the distinction.
- The console exposes a long typed deliverable before the launch controls. This
  is useful inspection data but may make the next action harder to find. No UI
  redesign has been made; evaluate this with the unfamiliar reader.
- Running `./gradlew :samples:runCelMapping` failed with
  `key 'fields' is not present in map`. The example accessed protobuf wrapper
  fields even though CEL adapts Struct/Value to maps/scalars. Changing the sample
  expressions to `input.enabled` and `input.name` made the command succeed and
  produce both copied and selected `Ada` values. Production CEL behavior was
  unchanged. The mapping guide now gives the command and expected result.

## Remaining evidence

Cold setup timings; independent human walkthrough; documented-input rehearsal;
restart observations for this new project; external-service MCP/ACP tutorial;
standalone consumer dependency resolution; remaining library examples;
correction/coordination/integration latency and outcome measurements; optional
Kafka/connector walkthroughs; durable publication of tutorials and evidence.
Goal 6 is not complete.
