# Managed selected historical reads

The selected-read RPC is now explicitly mountable through
`HistoricalReadAccess.withMaterialization(limits)`. Its engine, reader, original
backend binding and shutdown lifetime are shared with ordinary history. Both
services share a response byte budget; their call limits are per service.

Inventory: existing protobuf operation and contracts unchanged; Java host access
configuration extended; `historicalMaterializationRepository()` accessor added.
Default construction leaves the selected endpoint absent. Existing authentication
and retention qualification gates apply to this opt-in.

Local verification:

- `:protomolt-repo-service:test --tests '*ManagedDocumentHostIT'`: real PostgreSQL
  and LocalStack host startup, default-off, in-process/Netty authentication,
  authorized missing selection and reader shutdown. Passed in 14 seconds.
- `:protomolt-repo-container:test --tests '*DocumentPublicationCommitIT.hostRuntimePublishesAndReplaysThenDrainsRealProviderResources'`:
  real typed/opaque publication and host reopening. Typed selected reads match
  the local SPI's Any, retained descriptor, metadata and path; responses match
  across fresh in-process and Netty hosts. Passed in 17 seconds.

The retained XML files contain these focused results. Full remote repository
parity, deployment and RustFS performance are not established by these checks.
