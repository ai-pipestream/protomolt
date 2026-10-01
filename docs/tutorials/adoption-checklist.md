# ProtoMolt adoption walkthrough checklist

Use this with someone who has not used ProtoMolt before. The tester follows the
linked guides without coaching. Record questions and assistance rather than
quietly correcting the instructions. Stop a path when its instructions are
insufficient; a useful finding is not a failed tester.

The first workflow uses a scripted author, not a paid model. The library example
uses mutable snapshot dependencies. The service walkthrough uses a separate
example gRPC process first and then asks the tester to connect their own service.
Never include credentials or private input data in the report.

## Record the environment

- Date and guide commit:
- OS, CPU architecture, and Docker/Compose versions:
- JDK/Gradle versions if testing libraries or building ACP:
- Previously installed tools and cached images/dependencies:
- Local Docker host or remote host:
- Which parts the tester attempted:

## First workflow

Follow [Run your first workflow](first-workflow.md).

- Start time; time to download; time to a healthy stack; time to completed job:
- Number of commands, UI actions, and questions requiring help:
- Did the checksum verification work?
- Was it clear which containers should exit and which should remain running?
- Could you find the correct token, connect, and recognize the author as scripted?
- Could you start authoring, recognize acceptance, and launch the workflow?
- Could you distinguish a submitted job from a completed job?
- Try an invalid operation ID. Was the refusal understandable and recoverable?
- Stop and start the stack using the guide. Can you find the prior task and job?
- Download the signed record. Was it clear what that record does and does not prove?
- Note any copied text, setup decision, or error that the guide did not explain.

## Your gRPC service

Follow [Connect a gRPC service](connect-grpc-service.md), first with the included
fixture, then with your own reflected endpoint if one is available.

- Time to connect an MCP client and invoke a method:
- Could you tell which machine resolves the endpoint hostname?
- Did inspection provide enough information to construct a request?
- Was invalid input distinct from an unreachable service?
- Could you use the same profile through remote ACP?
- If you could not connect your own service, record its transport/reflection
  requirements and the exact point of failure. Do not count the included fixture
  alone as proof that bringing your own service was easy.
- Were the outbound policy and credential requirements understandable?

## Libraries without the platform

Follow [Use the protobuf toolkit](../../examples/protobuf-toolkit/README.md).
This path does not need the Compose stack running.

- Time to the expected output, dependency refresh required, and assistance:
- Did the published dependencies resolve without a Forgejo login?
- Can you identify the mapping, selector, projection, and validation in the source?
- Can you change a validation bound and see the program reject an invalid value?
- Can you find the emitted OpenAPI document and the runtime-only CEL constraint?
- Was the embedded registry's lifetime and compatibility behavior clear?
- Could you identify which dependency your own application would need?

## Return the findings

For each confusing step, record the guide section, what you expected, what
happened, the error text with secrets removed, and whether outside help was
needed. Report incomplete paths as incomplete. Include elapsed times and what
was already cached so we do not confuse download speed with product usability.
We will fix the instructions or behavior and rerun affected steps before closing
Goal 6.
