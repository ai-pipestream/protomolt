# Historical occurrence client

`protomolt-repo-history-grpc` reads one selected historical occurrence using the
existing gRPC contract and decodes it with its exact retained descriptor. It does
not require the original schema registry. The client library excludes repository
server assemblies, SQL drivers, Kafka and storage-provider SDKs; `checkRuntimeBoundaries`
enforces that production dependency boundary.

Supply an authenticated `DocumentHistoryMaterializationServiceFutureStub`, a shared
`PayloadBudget`, explicit decode limits, a timeout and a maximum number of open
calls/results to `HistoricalOccurrenceClient`. The embedding host owns the channel,
credentials and endpoint verification. Request account coordinates never confer
access. Close each returned result with try-with-resources.

Each `read(request, control)` makes a new RPC. The server checks authorization
before delivery; the client verifies request identity, retained schema artifacts
and payload identity before decoding. `Result.view(control)` checks local
cancellation, not current server authorization. An already delivered Java value
cannot be revoked. This API is not an implementation of the repository SPI's
reauthorizing result handle.

The byte budget covers an 8 MiB inbound reservation plus retained response/value
reservations and verifier scratch while ownership transfers. These reservations
overlap, so an 8 MiB budget is insufficient for a successful nonempty result.
They measure serialized bytes, not total parsed heap. Open results retain call
slots; hosts should size both limits for their workloads. Cancellation is checked
at most every 25 ms while waiting for the RPC, and before/after bounded decoding
operations. A single protobuf parse is not interruptible.

The client borrows its channel and never closes it. No schema compilation, fresh
validation verdict, latest-schema fallback or automatic retry is performed.
