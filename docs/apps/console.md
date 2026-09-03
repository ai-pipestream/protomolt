# The console

The console is ProtoMolt's browser face: a Vue 3 and Vuetify single-page
application whose sections are the platform's surfaces. It browses and
compatibility-checks registry subjects, edits and runs stored workflows,
registers gRPC services and calls their methods through generated forms,
queries a search node and a metrics node, verifies signed work records, and
follows durable agent work. `protomolt-serve` bundles the built application
and serves it at `/console` on its HTTP port.

The design stance behind every page is the same: the console is a thin client
over verbs any caller can invoke. There is no console-only API. Each panel
posts to one of two same-origin bridges the server publishes — `/api/protomolt`
onto the in-process schema registry, and `/api/serve` back onto the verb
catalog at `/api/serve/grpc-json/ProtoMoltService/{Verb}` — so a page's whole
contract with the platform is the verbs it names. `ServiceList`,
`CheckWorkflow`, `RunWorkflow`, `ServiceInvoke`, `VerifyWorkRecord`: what the
console does with a button, a shell script does with `curl` and an agent does
over MCP. That constraint keeps the browser honest. A capability that cannot
be expressed as a verb cannot appear in the console, and a capability that can
is available to every other caller the moment it appears.

> **Run it in a container.** The platform ships as Compose services.
> `./gradlew :protomolt-serve:installDist :protomolt-acp-agent:installDist && docker compose build && docker compose up`
> builds and starts it, with the serve container running `--demo` so the
> registry, a sample schema, and a sample workflow are already there and every
> surface has something to answer. Full detail lives in
> [Running in Docker](docker.md).

> **The ports.** `8080` carries HTTP: the console at `/console`, Swagger UI at
> `/docs`, the OpenAPI document at `/openapi.json`, the REST bridge at
> `/grpc-json/ProtoMoltService/{Method}`, and MCP at `/mcp`. `9090` carries
> gRPC with server reflection on. `8081` carries the schema registry, speaking
> the Confluent protocol. Override them with `PROTOMOLT_HTTP_PORT`,
> `PROTOMOLT_GRPC_PORT`, and `PROTOMOLT_REGISTRY_PORT`.

## The shape of the application

A rail on the left lists the sections — Tasks, Schemas, Workflows, Services,
Search, Metrics, Receipts — collapsed to icons and expanding on hover to show
each section's name and a one-line hint. The application bar carries the
wordmark, a light and dark theme toggle whose choice persists in browser
storage, and, outside the tasks section, a chip reporting the registry bridge:
labelled `/api/protomolt` when a startup probe reaches the registry, and
`registry unreachable` when it does not. The chip is a single startup probe and
nothing more; every page surfaces its own request failures inline, in an alert
that names what could not be loaded and offers a retry.

The root path redirects to `/tasks`. Views that answer a question worth sharing
put their state in the URL: a subject's active tab and version, a workflow's
name and input, a service profile to select or a target to prefill. A link to a
console page is a link to a reproducible view of it.

## Schemas

The Schemas section is the registry browser. Its landing page lists every
subject with the number of versions it holds and which version is latest.
Version counts fan out concurrently once the subject list arrives, and a
subject whose versions cannot be read keeps its placeholder rather than
failing the whole table. A filter box narrows the list by substring — `/`
focuses it from anywhere on the page, Escape clears it — and the chip beside
the heading reports how many of how many subjects match. An empty registry
says so, and says how a subject gets there.

Opening a subject gives a version timeline on the left, latest marked, and five
tabs on the right:

- **Schema** renders the selected version's source with its version, global id,
  and schema type. References are chips that link to the exact subject and
  version they point at, so a chain of imports is navigable rather than merely
  named.
- **Diff** compares any two versions, with a swap control and added and removed
  line counts.
- **Types** compiles the latest version and its transitive references into a
  `FileDescriptorSet` server-side, then explores it file by file.
- **Try it** composes a message of any type in that descriptor set through a
  generated form.
- **Compatibility** takes a candidate `.proto` and its references and checks it
  against the registry under the subject's effective mode.

Compatibility mode is shown as a badge that says whether the subject sets its
own mode or inherits the global one, and editing it in place changes the mode
the next registration is judged against. The Merge workbench, reached from the
registry header bar, takes two message types from two subjects, computes their
clash report from the descriptors alone, and — once each clash has a decision —
emits the merged schema together with its join and union mappings, registrable
like any hand-written proto. The registry itself, its storage and its gates,
is documented in [Registry](../schema/registry.md).

## Workflows

A workflow is a checked serial composition of gRPC calls: each step's request
is mapped from the workflow input and from every prior step's response. The
Workflows page is three columns — the stored workflows, the definition, and the
run — and it exists because that definition is worth editing against immediate
feedback.

The editor holds the definition as JSON and summarises it above the text as
`inputType → step → step`. **Check** verifies the definition without storing
it. **Save** stores it, and is gated server-side by the same check, so a
definition that does not verify cannot become the thing a later run executes.
Running is deliberately unavailable while the editor is dirty: a run executes
the stored workflow, and offering to run edited text would be a lie about what
just happened.

When verification fails it produces findings, and the findings panel is where
the console earns its place over a raw error string. A finding carries the step
it is attributed to and a kind. The panel groups findings by step, puts the
findings about the workflow itself first under the heading *The workflow
itself*, and labels each kind the way a person reads the definition rather than
the way the verifier spells it: `method` reads as *step method*, `when` as
*run condition*, `rule` as *mapping rule*, `celRule` as *CEL rule*, `output` as
*output mapping*, `workflow` as *workflow shape*, `contract` as *declared
contract*. The headline sentence distinguishes the two ways findings arrive —
a check that has not verified yet, and a save the gate refused — because those
call for different next moves.

A run reports each step as a chip, distinguishing steps that executed from
steps their run condition skipped. A failure names the step that failed
alongside its error. A success shows the output type and the output itself,
either as JSON or through the typed message viewer, which needs the output
type's descriptor: taken from the workflow schema's descriptor set when it
carries one, and otherwise obtained by compiling the schema's inline sources
through the `Compile` verb. See
[Workflow manager](../transform/workflow-manager.md) for the execution model
and [Workflows and run evidence](../transform/workflows.md) for what a run
records.

## Services

The Services workbench is where an outside gRPC service becomes part of the
platform. Registering one asks for three things: a profile name, a `host:port`
target, and whether the endpoint is TLS. The port is required and checked in
the browser, because the port is what picks out the listener to reflect, and
the transport is stated explicitly because an unspecified transport is refused
rather than guessed. Registration reflects the target, reads its contract
straight from the running service, and stores the descriptors.

What follows is the point of the section. Every method of a registered profile
that one request can drive is bound as a catalog verb, named for the profile
and the method in kebab case — `ListOrders` on the `billing` profile answers to
`billing-list-orders`. When two of a profile's services declare the same method
name, both take the service name as well, and only the ambiguous ones do,
because a name a caller has to type is worth keeping short. A verb bound this
way is a verb like any other: an RPC on the gRPC surface, a method on the REST
mount, and an MCP tool whose input schema is derived from the request message.
No code is generated and nothing restarts. The console mirrors the server's
derivation exactly, so the verb name shown beside a method is the name agents
and workflows reach it by.

Client-streaming methods are the exception, and the workbench says so in place
rather than hiding them: a verb takes one request, a method that expects a
stream of them cannot be driven by one, so those methods get no verb and the
panel points at a gRPC client with generated stubs instead.

Expanding a method opens the invoke panel. It offers two ways to state a
request. The form has one widget per input field, chosen by the field's shape —
a switch for a singular `bool`, a text field for other singular scalars, a
textarea for messages, repeated fields, and maps — each hinted with its
cardinality, protobuf type, and fully qualified type name. The JSON view holds
a request skeleton with one key per input field at its zero value, and
switching from form to JSON carries the form's current request across. The form
is careful about proto3 JSON: 64-bit integers stay strings so they stay exact,
blank fields are omitted rather than sent as defaults, and a field that expects
a number refuses text that is not one, naming the field. The reply shows the
gRPC status, the output type, and each response message; a server-streaming
method reports how many messages it streamed.

The Connection directions page takes the same idea one step further out. It
acquires a service's schema by reflection, from a git repository, or from
pasted `.proto` source, then hands back working directions: the `claude mcp add`
command that makes the service agent-operable, a prompt to hand the agent, and
a Kafka Connect sink configuration for a chosen method. Persistent profiles and
their descriptor storage are covered in
[Service workspaces](../surface/service-workspace.md).

> **The console's server is an MCP server.** The same process the console talks
> to speaks the Model Context Protocol over stateless streamable HTTP at
> `/mcp`. Connect Claude Code with
> `claude mcp add --transport http protomolt http://localhost:8080/mcp`.
> Everything the console does through verbs, an agent can do through MCP —
> including the verbs a service registration just created.

## Search

Search is served by a document-platform node rather than by `protomolt-serve`,
so the console reaches it the way it reaches any remote gRPC service: through a
registered service profile and the `ServiceInvoke` verb. The page finds that
profile by contract rather than by name, looking for the registered profile
whose reflected services include `ai.protomolt.proto.search.v1.SearchService`.
Profiles are inspected concurrently so one slow endpoint cannot stall the page,
the first match in registration order wins, and a profile whose endpoint
refuses inspection is not the profile today. When nothing exposes the contract,
the page says which contract it is looking for and links to the registration
form; the profile's name is immaterial.

A query names a subject, a phrase, a lane — lexical term matching over the
mapping's text fields, vector nearest-chunk search over its vectors, or the two
fused — and how many hits to return. A subject with no vector lane says so
rather than offering a lane that cannot answer. Results are ordered best first
and show the document id, the chunk's ordinal where the hit is a chunk, the
score, and the stored fields the mapping declares, each rendered from whichever
typed arm it carries. A `bytes` field reports its size instead of its base64,
because a byte count is the useful fact. The query surface itself is documented
in [The search service](../search/service.md).

## Metrics

Metrics follows the same pattern against
`ai.protomolt.proto.metric.v1.MetricService`, found by contract and reached
through `ServiceInvoke`. The page starts from a mapping subject: describing it
returns the mapping's members, and the members' roles fill the pickers, so
measures and dimensions are offered because the mapping declares them rather
than because a person remembered them. Describing a subject preselects its
first measure, which puts one answer one click away.

A query asks for chosen measures, optional dimensions to group by, and a row
limit. The result table's columns are frozen to the members that were actually
queried, so editing the pickers afterwards cannot grow columns the engine never
answered. Each measure cell shows its value in tabular figures over a data bar
scaled to the largest magnitude in that column — redundant magnitude behind
text, never a substitute for it. A disclosure beneath the table, *How the
engine ran it*, holds the physical plan the engine reports. Counts render as
integers and ratios keep their precision. What is queryable and how the
compiler refuses what is not is covered in [Metrics](../metric/metrics.md).

## Receipts

A signed work record is a receipt for a run: what ran, over what, in what
order. The Receipts page does two distinct things with one, and keeping them
distinct is the whole point of the layout.

**Verification** asks whether the record holds on its own terms — do its
signatures and digests check out. A record arrives either by exporting it from
a recorded run's id or by pasting its base64. Verification renders every check
as a line with its identifier and detail, and the check list has three states,
not two: passed, failed, and skipped, where skipped means a check that could
not run by design. A skipped check is drawn as neither a pass nor a failure,
because rendering absent evidence as success is exactly the error a receipt
exists to prevent.

Below the checks sits the record's non-claims, under the heading *What this
record does not claim*. These are stated by the verifier rather than implied by
silence: a receipt is honest about its limits, and a reader who has to infer
the boundary of a proof will infer it generously. Records verify anywhere — the
[zero-dependency record verifier](record-verifier.md) ships separately, so a
counterpart does not need ProtoMolt to check a receipt you hand them.

**Evaluation** asks a different question: does this record show that a
particular workflow was actually followed. It takes a stored workflow, compiles
it, and replays the record against that compiled definition and its schema —
the verb requires both, because an evaluation is a claim about a specific
contract and there is no evaluating without one. The result reports whether the
record is accepted, under which policy, and how each recorded step replayed,
with a line of detail for every step that did not. The record format, its
signing, and its trust model are described in
[Signed work records](../design/receipts.md).

## Tasks

The Tasks section follows durable agent delegation: the workers a coordinator
knows, each task's lifecycle state, its cursor-ordered transcript, the contract
of done joined against a completion candidate's evidence, and the review
decisions a console session makes as the external reviewer. It also carries its
own browser login boundary, separate from the operator API token. It has its
own chapter: [Task console](task-console.md).

## Locking it down

By default every surface is open, which suits a laptop or a trusted network.
Setting `PROTOMOLT_API_TOKEN` requires that secret on every operational
surface and closes the browser surfaces with it: `/console` and both
same-origin bridges answer with a message explaining that they are disabled and
pointing at an authenticated protocol client. Configuring a separate task
console login alongside the API token serves the console again, but only the
task routes work behind it — the registry and action bridges the other sections
depend on stay disabled. [Task console](task-console.md) documents that session
boundary.

## Working on the console

The application lives at `apps/console`. `npm run dev` starts a dev server that
proxies `/api/protomolt/*` to a registry, so the application is same-origin in
development; point it at any Confluent-compatible registry with
`PROTOMOLT_REGISTRY_URL`. `npm test` runs the Vitest suites, including jsdom
component and view suites. `npm run build` produces the static bundle in
`dist/`, which the `protomolt-serve` build bundles onto the classpath and
`ConsoleHandler` serves at `/console`. Any reverse proxy that mirrors
`/console`, `/api/protomolt`, and `/api/serve` works equally well.
