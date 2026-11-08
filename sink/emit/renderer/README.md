# Typed rendition renderers

`protomolt-renderer` is optional. It depends on protobuf, descriptor/validation
utilities and gRPC, and does not pull Parquet, Jena or repository-service into
its runtime. `RendererRegistry.load(loader, enabledIds)` discovers trusted
installed `RendererProvider` modules through `ServiceLoader`. Missing enabled
providers and duplicate identities fail startup; installing a module does not
enable it or cause writes.

A capability describes one input profile, exact provider/options identity,
media type, extension, loss statement and positive input/output budgets. This
first contract is deliberately one message to one bounded in-memory artifact.
It does not promise bundles, unbounded streaming, seeking or hot JAR loading.
The budgets apply to the adapter, not to repository document size. A provider
must enforce its own internal allocations before producing output; the registry
also bounds writes and checks cancellation before returning any bytes.

`check` evaluates actual descriptor/options compatibility and caller policy
without rendering. `render` repeats those checks, produces a complete artifact,
and returns binary bytes. Hosts must provide current source/disclosure
authorization. Renderer options cannot disable that check. Unknown options and
same-name/different-descriptor options are refused. A rendered artifact is not
a repository commit; #818 owns publication using existing DocumentService APIs.

`GrpcRendererProvider` binds one exact input descriptor and capability to a
host-configured channel and deadline. Hosts create the channel through their
existing outbound policy/credential machinery and set outbound transport limits.
Neither endpoint nor credentials can come from the rendering request. The
adapter bounds responses, propagates cancellation/refusals and verifies the
terminal capability identity. It forwards the exact binary input and its
closed descriptor set. Because this version carries no per-occurrence Any
admission evidence, it refuses present Any messages; use an explicit projection
under the source revision's retained evidence before remote rendering. It never
opens Any using a live registry lookup.

Providers implement the request contract on their own endpoint; no generic
server or storage API is installed by this module. #819 supplies the first
local Parquet adapter; #820 owns console controls. The renderer closure pin is
independent of the repository Document closure. Both are checked during this
change, and the console generator includes this new contract.
