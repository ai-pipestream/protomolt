# Red reproducer — publication replay before server drainage

Evidence for the intermittent failure repaired on branch
`agent/publication-cancellation-repair` (Forgejo PR 309; failing reference:
GitHub run 38121396523, job 114416690422).

- `ProbeAssumptionDemonstrationTest.java` — the exact scratch test source. It
  encodes the old probe assumption: issue the exact receipt replay immediately
  after the committed response is observed, without waiting for the documented
  server drainage event. The controlled observer withholds the server terminal
  callback — a legal gRPC ordering, since that callback is queued on the server
  call executor after the response is on the wire — so the replay lands while the
  one-slot transport capacity is still legitimately held.
- `red-run.log` — the run: command, exit code 1, and the failure,
  `RESOURCE_EXHAUSTED: Publication transport is full`, the exact CI signature.

The file is kept here, not under `src/test`, because it is red by design: it
documents an invalid assumption. The committed green contract suite is
`repo/service/src/test/java/ai/protomolt/proto/repo/service/DocumentPublicationGrpcServiceTest.java`,
which asserts the same forced ordering as the transport's actual capacity
contract. To reproduce the red run, copy the `.java` back into that directory and
run the command recorded in `red-run.log`.
