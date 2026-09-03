/**
 * Configured compositions of gRPC calls: one endpoint in, one composed answer out.
 *
 * <p>A workflow is described by {@link CompiledWorkflow} — an input type and an ordered list of
 * unary steps, each step's request mapped from the workflow scope (the workflow input plus every
 * prior step's response, bound under the step's name). {@link WorkflowVerifier} type-checks a
 * definition against its descriptors without contacting any service, and {@link WorkflowRunner}
 * executes one, serially and fail-fast, inside a single deadline budget. The two are exposed
 * as the {@code check-workflow} and {@code run-workflow} verbs by {@link CheckWorkflowAction} and
 * {@link RunWorkflowAction}.</p>
 *
 * <p>{@link WorkflowRepository} is the extension point for named workflows: any store that can
 * return a workflow's JSON definition by name satisfies it, with the registry's Git store as the
 * supplied implementation. {@link StreamJoiner} covers the streaming case, pairing two live
 * server streams by arrival order or by key.</p>
 *
 * <p>Mapping and static checking are borrowed from
 * {@link ai.protomolt.proto.shapes.RuleChecker} and the scoped rule dialect in
 * {@code ai.pipestream.proto.shapes}; the calls themselves are made through
 * {@code ai.pipestream.proto.grpc.invoke}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/transform/workflow-manager.md">workflow
 * manager design note</a> for the model behind these types.</p>
 */
package ai.protomolt.proto.workflow;
