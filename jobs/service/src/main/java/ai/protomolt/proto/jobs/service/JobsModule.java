package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.composer.Channels;
import ai.protomolt.proto.composer.ComposerException;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.jobs.service.actions.GetJobAction;
import ai.protomolt.proto.jobs.service.actions.ListJobsAction;
import ai.protomolt.proto.jobs.service.actions.SubmitWorkflowAction;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.google.protobuf.Descriptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.List;

/**
 * The durable workflow-runs executor as a mountable role. Wiring opens the
 * store, contributes the jobs verbs ({@code submit-workflow},
 * {@code get-job}, {@code list-jobs}) for the registry's actions route, and
 * contributes its {@link ActionContext} so checkpoint transcoding and the
 * catalog share one type registry. Starting registers every contributed
 * descriptor and begins the worker fleet.
 *
 * <p>Requires a co-mounted registry: workflow definitions come from the
 * registry's contributed {@link WorkflowRepository}.
 */
public final class JobsModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "jobs";

    private final WorkflowRunStoreConfig storeConfig;
    private final WorkflowRunsConfig runsConfig;
    private WorkflowRunDatabase database;
    private WorkflowRunWorker worker;

    /**
     * Creates the module.
     *
     * @param storeConfig the Postgres store configuration
     * @param runsConfig the worker and consumer settings
     */
    public JobsModule(WorkflowRunStoreConfig storeConfig, WorkflowRunsConfig runsConfig) {
        if (storeConfig == null) {
            throw new IllegalArgumentException("storeConfig must not be null");
        }
        if (runsConfig == null) {
            throw new IllegalArgumentException("runsConfig must not be null");
        }
        this.storeConfig = storeConfig;
        this.runsConfig = runsConfig;
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
        return new ServiceMount() {
            @Override
            public void start() {
                for (Descriptors.FileDescriptor descriptor
                        : context.contributions().all(Descriptors.FileDescriptor.class)) {
                    actionContext.registry().registerFile(descriptor);
                }
                WorkflowRunner runner = new WorkflowRunner(step -> {
                    String target = step.target();
                    if (target.startsWith(Channels.IN_PROCESS_PREFIX)) {
                        return InProcessChannelBuilder.forName(
                                        target.substring(Channels.IN_PROCESS_PREFIX.length()))
                                .build();
                    }
                    return NettyChannelBuilder.forTarget(target).usePlaintext().build();
                });
                worker = new WorkflowRunWorker(store, actionContext, workflows, runner, runsConfig);
                worker.start();
            }

            @Override
            public void close() {
                if (worker != null) {
                    worker.close();
                }
                database.close();
            }
        };
    }
}
