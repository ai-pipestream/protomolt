package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRepository;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.workflow.CheckWorkflowAction;
import ai.pipestream.proto.workflow.RunWorkflowAction;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.workflow.RecordSigning;
import ai.pipestream.proto.workflow.WorkflowWorkbenchActions;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.emit.okf.EmitOkfAction;
import ai.pipestream.proto.acquire.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectAction;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowVersionRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workspace.ServiceWorkspaceActions;
import ai.pipestream.proto.inference.service.actions.DescribeModelAction;
import ai.pipestream.proto.inference.service.actions.GenerateAction;
import ai.pipestream.proto.inference.service.actions.ListModelsAction;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.jobs.service.actions.CompleteStepAction;
import ai.pipestream.proto.jobs.service.actions.GetJobAction;
import ai.pipestream.proto.jobs.service.actions.ListJobsAction;
import ai.pipestream.proto.jobs.service.actions.SubmitWorkflowAction;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import ai.pipestream.proto.registry.SchemaRegistryStore;

import java.nio.file.Path;
import java.util.function.Supplier;

/**
 * The full catalog: the built-in actions from
 * {@link ActionCatalog#defaults(ActionContext)} plus the gRPC verbs ({@code reflect},
 * {@code grpc-invoke}), {@code generate-stubs}, {@code gather-git}, the workflow verbs
 * ({@code run-workflow}, {@code check-workflow}), {@code emit-okf}, the workflow-runs verbs
 * ({@code submit-workflow}, {@code get-job}, {@code list-jobs}, {@code complete-step}),
 * the service-workspace verbs ({@code service-register}, {@code service-list},
 * {@code service-inspect}, {@code service-refresh}, {@code service-invoke}), the workflow-workbench verbs
 * ({@code suggest-mappings}, {@code compile-workflow}, {@code record-workflow-run},
 * {@code replay-workflow}, {@code promote-workflow}), and the inference verbs
 * ({@code inference-generate}, {@code inference-list-models},
 * {@code inference-describe-model}) — exactly the RPCs of {@code ProtoMoltService}.
 *
 * <p>The MCP server exposes a host-independent subset, leaving out
 * general workflow execution, jobs, inference, and {@code emit-okf}, which need server-side wiring.
 * It does expose the workflow workbench for inline draft workflows. The {@code /mcp} mount inside
 * {@code protomolt-serve} carries the full catalog. Jobs, service-workspace, and workflow-workbench
 * verbs are always registered; without their stores they answer
 * {@code unavailable} with the operator-facing remedy instead of vanishing from the catalog.
 * The inference verbs behave the same way without an {@link InferenceEngines}.
 */
public final class ProtoMoltCatalog {

    private ProtoMoltCatalog() {
    }

    public static ActionCatalog full(ActionContext context) {
        return full(context, null);
    }

    /**
     * @param gatherCacheRoot where {@code gather-git} keeps its per-repo clone caches; null
     *        for the library default under the process owner's home. Operator configuration —
     *        never taken from a request.
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot) {
        return full(context, gatherCacheRoot, null);
    }

    /**
     * @param workflows where {@code run-workflow} resolves stored workflow names; null leaves the
     *        verb inline-only
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows) {
        return full(context, gatherCacheRoot, workflows, null, 0);
    }

    /**
     * The jobs-aware catalog.
     *
     * @param jobs the workflow-runs store the four jobs verbs read and write; null means
     *        jobs are not configured and the verbs answer {@code unavailable}
     * @param maxAttemptsDefault the retry ceiling stamped on newly submitted jobs;
     *        ignored when {@code jobs} is null
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault) {
        return full(context, gatherCacheRoot, workflows, jobs, maxAttemptsDefault, null);
    }

    /**
     * The jobs-and-inference-aware catalog.
     *
     * @param jobs the workflow-runs store the four jobs verbs read and write; null means
     *        jobs are not configured and the verbs answer {@code unavailable}
     * @param maxAttemptsDefault the retry ceiling stamped on newly submitted jobs;
     *        ignored when {@code jobs} is null
     * @param inference the inference facade the three inference verbs execute through;
     *        null means inference is not configured and the verbs answer
     *        {@code unavailable}
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference) {
        return full(context, gatherCacheRoot, workflows, jobs, maxAttemptsDefault, inference, null);
    }

    /**
     * The complete catalog including a durable service workspace when configured.
     *
     * @param serviceProfiles profile storage; null keeps the service workspace actions
     *        discoverable workspace verbs unavailable until a host configures storage
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles) {
        return full(context, gatherCacheRoot, workflows, jobs, maxAttemptsDefault, inference,
                serviceProfiles, null);
    }

    /**
     * The complete catalog with one host-owned policy shared by all outbound gRPC actions.
     *
     * @param serviceProfiles profile storage; null keeps the service workspace actions
     *        discoverable workspace verbs unavailable until a host configures storage
     * @param outboundPolicy host-owned target, transport, deadline, and channel-budget policy;
     *        null uses {@link OutboundChannelPolicy#defaults()}
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles,
                                     OutboundChannelPolicy outboundPolicy) {
        return full(context, gatherCacheRoot, workflows, jobs, maxAttemptsDefault, inference,
                serviceProfiles, outboundPolicy, null, null, null, null);
    }

    /** The complete catalog with the durable workflow-workbench repositories when configured. */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles,
                                     OutboundChannelPolicy outboundPolicy,
                                     ArtifactRepository artifacts,
                                     RunEvidenceRepository runEvidence,
                                     WorkflowVersionRepository workflowVersions) {
        return full(context, gatherCacheRoot, workflows, jobs, maxAttemptsDefault, inference,
                serviceProfiles, outboundPolicy, artifacts, runEvidence, workflowVersions, null);
    }

    /** The complete catalog with the registry that owns reflected descriptor artifacts. */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles,
                                     OutboundChannelPolicy outboundPolicy,
                                     ArtifactRepository artifacts,
                                     RunEvidenceRepository runEvidence,
                                     WorkflowVersionRepository workflowVersions,
                                     SchemaRegistryStore registry) {
        return full(context, gatherCacheRoot, workflows, jobs, maxAttemptsDefault, inference,
                serviceProfiles, outboundPolicy, artifacts, runEvidence, workflowVersions,
                registry, null);
    }

    /**
     * The complete catalog whose verifying verbs read their trust snapshot from
     * {@code trust} per request rather than from the environment at registration, so a
     * host following the config lane re-scopes trust live.
     *
     * @param trust the server's current trust snapshot; null keeps the environment pin
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     WorkflowRepository workflows, WorkflowRunStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles,
                                     OutboundChannelPolicy outboundPolicy,
                                     ArtifactRepository artifacts,
                                     RunEvidenceRepository runEvidence,
                                     WorkflowVersionRepository workflowVersions,
                                     SchemaRegistryStore registry,
                                     Supplier<TrustSnapshot> trust) {
        OutboundChannelPolicy policy = outboundPolicy == null
                ? OutboundChannelPolicy.defaults() : outboundPolicy;
        ChannelFactory channels = ChannelFactory.standard(policy);
        StructuredGenerator structured = inference == null
                ? null : new StructuredGenerator(inference, context.registry());
        WorkflowRunner runner = new WorkflowRunner(policy, structured);
        ActionCatalog catalog = ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction(channels))
                .register(new ReflectAction(channels))
                .register(new GenerateStubsAction())
                .register(new GatherGitAction(gatherCacheRoot))
                .register(new RunWorkflowAction(runner, workflows))
                .register(new CheckWorkflowAction())
                .register(new EmitOkfAction())
                .register(new SubmitWorkflowAction(jobs, workflows, maxAttemptsDefault))
                .register(new GetJobAction(jobs))
                .register(new ListJobsAction(jobs))
                .register(new CompleteStepAction(jobs))
                .register(new GenerateAction(inference))
                .register(new ListModelsAction(inference))
                .register(new DescribeModelAction(inference));
        ServiceWorkspaceActions.register(catalog, serviceProfiles, registry, channels);
        return trust == null
                ? WorkflowWorkbenchActions.register(catalog, runner, artifacts,
                        runEvidence, workflowVersions)
                : WorkflowWorkbenchActions.register(catalog, runner, artifacts,
                        runEvidence, workflowVersions,
                        RecordSigning.fromEnvironment(), trust);
    }
}
