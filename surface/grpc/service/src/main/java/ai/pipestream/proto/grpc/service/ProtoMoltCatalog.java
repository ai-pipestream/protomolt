package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.chain.CheckChainAction;
import ai.pipestream.proto.chain.RunChainAction;
import ai.pipestream.proto.chain.RecipeWorkbenchActions;
import ai.pipestream.proto.codegen.GenerateStubsAction;
import ai.pipestream.proto.emit.okf.EmitOkfAction;
import ai.pipestream.proto.gather.git.GatherGitAction;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectAction;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.RecipeRepository;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workspace.ServiceWorkspaceActions;
import ai.pipestream.proto.inference.service.actions.DescribeModelAction;
import ai.pipestream.proto.inference.service.actions.GenerateAction;
import ai.pipestream.proto.inference.service.actions.ListModelsAction;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.jobs.service.actions.CompleteStepAction;
import ai.pipestream.proto.jobs.service.actions.GetJobAction;
import ai.pipestream.proto.jobs.service.actions.ListJobsAction;
import ai.pipestream.proto.jobs.service.actions.SubmitChainAction;
import ai.pipestream.proto.jobs.service.store.ChainJobStore;

import java.nio.file.Path;

/**
 * The full catalog: the built-in actions from
 * {@link ActionCatalog#defaults(ActionContext)} plus the gRPC verbs ({@code reflect},
 * {@code grpc-invoke}), {@code generate-stubs}, {@code gather-git}, the chain verbs
 * ({@code run-chain}, {@code check-chain}), {@code emit-okf}, the chain-jobs verbs
 * ({@code submit-chain}, {@code get-job}, {@code list-jobs}, {@code complete-step}),
 * the service-workspace verbs ({@code service-register}, {@code service-list},
 * {@code service-inspect}, {@code service-refresh}), the recipe-workbench verbs
 * ({@code suggest-mappings}, {@code compile-recipe}, {@code record-recipe-run},
 * {@code replay-recipe}, {@code promote-recipe}), and the inference verbs
 * ({@code inference-generate}, {@code inference-list-models},
 * {@code inference-describe-model}) — exactly the RPCs of {@code ProtoMoltService}.
 *
 * <p>The MCP server exposes a host-independent subset, leaving out
 * general chain execution, jobs, inference, and {@code emit-okf}, which need server-side wiring.
 * It does expose the recipe workbench for inline draft chains. The {@code /mcp} mount inside
 * {@code protomolt-serve} carries the full catalog. Jobs, service-workspace, and recipe-workbench
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
     * @param chains where {@code run-chain} resolves stored chain names; null leaves the
     *        verb inline-only
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains) {
        return full(context, gatherCacheRoot, chains, null, 0);
    }

    /**
     * The jobs-aware catalog.
     *
     * @param jobs the chain-jobs store the four jobs verbs read and write; null means
     *        jobs are not configured and the verbs answer {@code unavailable}
     * @param maxAttemptsDefault the retry ceiling stamped on newly submitted jobs;
     *        ignored when {@code jobs} is null
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains, ChainJobStore jobs,
                                     int maxAttemptsDefault) {
        return full(context, gatherCacheRoot, chains, jobs, maxAttemptsDefault, null);
    }

    /**
     * The jobs-and-inference-aware catalog.
     *
     * @param jobs the chain-jobs store the four jobs verbs read and write; null means
     *        jobs are not configured and the verbs answer {@code unavailable}
     * @param maxAttemptsDefault the retry ceiling stamped on newly submitted jobs;
     *        ignored when {@code jobs} is null
     * @param inference the inference facade the three inference verbs execute through;
     *        null means inference is not configured and the verbs answer
     *        {@code unavailable}
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains, ChainJobStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference) {
        return full(context, gatherCacheRoot, chains, jobs, maxAttemptsDefault, inference, null);
    }

    /**
     * The complete catalog including a durable service workspace when configured.
     *
     * @param serviceProfiles profile and descriptor-artifact storage; null keeps the four
     *        discoverable workspace verbs unavailable until a host configures storage
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains, ChainJobStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles) {
        return full(context, gatherCacheRoot, chains, jobs, maxAttemptsDefault, inference,
                serviceProfiles, null);
    }

    /**
     * The complete catalog with one host-owned policy shared by all outbound gRPC actions.
     *
     * @param serviceProfiles profile and descriptor-artifact storage; null keeps the four
     *        discoverable workspace verbs unavailable until a host configures storage
     * @param outboundPolicy host-owned target, transport, deadline, and channel-budget policy;
     *        null uses {@link OutboundChannelPolicy#defaults()}
     */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains, ChainJobStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles,
                                     OutboundChannelPolicy outboundPolicy) {
        return full(context, gatherCacheRoot, chains, jobs, maxAttemptsDefault, inference,
                serviceProfiles, outboundPolicy, null, null, null);
    }

    /** The complete catalog with the durable recipe-workbench repositories when configured. */
    public static ActionCatalog full(ActionContext context, Path gatherCacheRoot,
                                     ChainRepository chains, ChainJobStore jobs,
                                     int maxAttemptsDefault, InferenceEngines inference,
                                     ServiceProfileRepository serviceProfiles,
                                     OutboundChannelPolicy outboundPolicy,
                                     ArtifactRepository artifacts,
                                     RunEvidenceRepository runEvidence,
                                     RecipeRepository recipes) {
        OutboundChannelPolicy policy = outboundPolicy == null
                ? OutboundChannelPolicy.defaults() : outboundPolicy;
        ChannelFactory channels = ChannelFactory.standard(policy);
        StructuredGenerator structured = inference == null
                ? null : new StructuredGenerator(inference, context.registry());
        ChainRunner runner = new ChainRunner(policy, structured);
        ActionCatalog catalog = ActionCatalog.defaults(context)
                .register(new GrpcInvokeAction(channels))
                .register(new ReflectAction(channels))
                .register(new GenerateStubsAction())
                .register(new GatherGitAction(gatherCacheRoot))
                .register(new RunChainAction(runner, chains))
                .register(new CheckChainAction())
                .register(new EmitOkfAction())
                .register(new SubmitChainAction(jobs, chains, maxAttemptsDefault))
                .register(new GetJobAction(jobs))
                .register(new ListJobsAction(jobs))
                .register(new CompleteStepAction(jobs))
                .register(new GenerateAction(inference))
                .register(new ListModelsAction(inference))
                .register(new DescribeModelAction(inference));
        ServiceWorkspaceActions.register(catalog, serviceProfiles, channels);
        return RecipeWorkbenchActions.register(catalog, runner, artifacts,
                runEvidence, recipes);
    }
}
