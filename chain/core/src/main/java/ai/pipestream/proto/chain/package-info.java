/**
 * Configured compositions of gRPC calls: one endpoint in, one composed answer out.
 *
 * <p>A chain is described by {@link ChainDefinition} — an input type and an ordered list of
 * unary steps, each step's request mapped from the chain scope (the chain input plus every
 * prior step's response, bound under the step's name). {@link ChainVerifier} type-checks a
 * definition against its descriptors without contacting any service, and {@link ChainRunner}
 * executes one, serially and fail-fast, inside a single deadline budget. The two are exposed
 * as the {@code check-chain} and {@code run-chain} verbs by {@link CheckChainAction} and
 * {@link RunChainAction}.</p>
 *
 * <p>{@link ChainRepository} is the extension point for named chains: any store that can
 * return a chain's JSON definition by name satisfies it, with the registry's Git store as the
 * supplied implementation. {@link StreamJoiner} covers the streaming case, pairing two live
 * server streams by arrival order or by key.</p>
 *
 * <p>Mapping and static checking are borrowed from
 * {@link ai.pipestream.proto.shapes.RuleChecker} and the scoped rule dialect in
 * {@code ai.pipestream.proto.shapes}; the calls themselves are made through
 * {@code ai.pipestream.proto.grpc.invoke}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/design/chain-manager.md">chain
 * manager design note</a> for the model behind these types.</p>
 */
package ai.pipestream.proto.chain;
