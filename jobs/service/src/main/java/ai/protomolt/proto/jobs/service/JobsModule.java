package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.composer.Channels;
import ai.protomolt.proto.composer.ComposerException;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.jobs.service.actions.CompleteStepAction;
import ai.protomolt.proto.jobs.service.actions.GetJobAction;
import ai.protomolt.proto.jobs.service.actions.ListJobsAction;
import ai.protomolt.proto.jobs.service.actions.SubmitWorkflowAction;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.google.protobuf.Descriptors;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.List;

/**
 * The durable workflow-runs executor as a mountable role. Wiring opens the
 * store, contributes the jobs verbs ({@code submit-workflow},
 * {@code get-job}, {@code list-jobs}, {@code complete-step}) for the registry's
 * actions route, and contributes its {@link ActionContext} so checkpoint
 * transcoding and the catalog share one type registry. Starting registers every
 * contributed descriptor and begins the worker fleet.
 *
 * <p>Requires a co-mounted registry: workflow definitions come from the
 * registry's contributed {@link WorkflowRepository}.
 */
public final class JobsModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "jobs";

    private final WorkflowRunStoreConfig storeConfig;
    private final WorkflowRunsConfig runsConfig;
    private final OutboundChannelPolicy outboundPolicy;
    private final ServiceProfileRepository serviceProfiles;
    private WorkflowRunDatabase database;
    private WorkflowRunWorker worker;

    /**
     * Creates the module with default outbound channel policy.
     *
     * @param storeConfig the Postgres store configuration
     * @param runsConfig the worker and consumer settings
     */
    public JobsModule(WorkflowRunStoreConfig storeConfig, WorkflowRunsConfig runsConfig) {
        this(storeConfig, runsConfig, null, null);
    }

    /**
     * Creates the module with an explicit outbound channel policy and service profile repository.
     *
     * @param storeConfig the Postgres store configuration
     * @param runsConfig the worker and consumer settings
     * @param outboundPolicy outbound gRPC policy, or null to resolve from {@link NodeContext}
     *        contributions or {@link OutboundChannelPolicy#defaults()}
     * @param serviceProfiles resolves pipeline service dependencies, or null to resolve from
     *        {@link NodeContext} contributions
     */
    public JobsModule(WorkflowRunStoreConfig storeConfig, WorkflowRunsConfig runsConfig,
                      OutboundChannelPolicy outboundPolicy,
                      ServiceProfileRepository serviceProfiles) {
        if (storeConfig == null) {
            throw new IllegalArgumentException("storeConfig must not be null");
        }
        if (runsConfig == null) {
            throw new IllegalArgumentException("runsConfig must not be null");
        }
        this.storeConfig = storeConfig;
        this.runsConfig = runsConfig;
        this.outboundPolicy = outboundPolicy;
        this.serviceProfiles = serviceProfiles;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public java.util.Set<String> requires() {
        return java.util.Set.of("registry");
    }

    @Override
    public ServiceMount wire(NodeContext context) {
        List<WorkflowRepository> repositories =
                context.contributions().all(WorkflowRepository.class);
        if (repositories.isEmpty()) {
            throw new ComposerException(
                    "jobs requires a co-mounted registry contributing a WorkflowRepository");
        }
        WorkflowRepository workflows = repositories.getFirst();
        database = new WorkflowRunDatabase(storeConfig);
        JdbcWorkflowRunStore store = new JdbcWorkflowRunStore(database);
        ActionContext actionContext = ActionContext.create();
        context.contributions().contribute(ActionContext.class, actionContext);
        context.contributions().contribute(ProtoAction.class,
                new SubmitWorkflowAction(store, workflows, runsConfig.maxAttemptsDefault()));
        context.contributions().contribute(ProtoAction.class, new GetJobAction(store));
        context.contributions().contribute(ProtoAction.class, new ListJobsAction(store));
        context.contributions().contribute(ProtoAction.class, new CompleteStepAction(store));
        return new ServiceMount() {
            @Override
            public void start() {
                for (Descriptors.FileDescriptor descriptor
                        : context.contributions().all(Descriptors.FileDescriptor.class)) {
                    actionContext.registry().registerFile(descriptor);
                }
                OutboundChannelPolicy effectivePolicy = outboundPolicy != null
                        ? outboundPolicy
                        : firstOrDefault(context.contributions().all(OutboundChannelPolicy.class),
                                OutboundChannelPolicy.defaults());
                ServiceProfileRepository effectiveProfiles = serviceProfiles != null
                        ? serviceProfiles
                        : firstOrDefault(
                                context.contributions().all(ServiceProfileRepository.class), null);
                StructuredGenerator structured = firstOrDefault(
                        context.contributions().all(StructuredGenerator.class), null);
                ChannelFactory outbound = ChannelFactory.standard(effectivePolicy);
                WorkflowRunner runner = new WorkflowRunner(new WorkflowRunner.ChannelFactory() {
                    @Override
                    public ManagedChannel open(CompiledWorkflow.Step step) {
                        String target = step.target();
                        if (target.startsWith(Channels.IN_PROCESS_PREFIX)) {
                            return InProcessChannelBuilder.forName(
                                            target.substring(Channels.IN_PROCESS_PREFIX.length()))
                                    .build();
                        }
                        return outbound.open(target, step.tls());
                    }

                    @Override
                    public void validateTarget(CompiledWorkflow.Step step) {
                        if (!step.target().startsWith(Channels.IN_PROCESS_PREFIX)) {
                            outbound.validateTarget(step.target(), step.tls());
                        }
                    }

                    @Override
                    public void validateDeadline(long deadlineMillis) {
                        outbound.validateDeadline(deadlineMillis);
                    }
                }, structured);
                worker = new WorkflowRunWorker(store, actionContext, workflows, runner,
                        outbound, effectiveProfiles, structured,
                        WorkflowRunWorker.EndpointCredentials.NONE, runsConfig);
                worker.start();
            }

            @Override
            public void close() {
                if (worker != null) {
                    try {
                        worker.close();
                    } catch (Throwable t) {
                        try {
                            database.close();
                        } catch (Throwable suppressed) {
                            t.addSuppressed(suppressed);
                        }
                        throw t;
                    }
                }
                database.close();
            }
        };
    }

    private static <T> T firstOrDefault(List<T> values, T fallback) {
        return values.isEmpty() ? fallback : values.getFirst();
    }
}
