package ai.pipestream.proto.platform;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.WorkflowRepository;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.pipestream.proto.intake.service.IntakeServiceConfig;
import ai.pipestream.proto.intake.service.IntakeServices;
import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.actions.GetJobAction;
import ai.pipestream.proto.jobs.service.actions.ListJobsAction;
import ai.pipestream.proto.jobs.service.actions.SubmitWorkflowAction;
import ai.pipestream.proto.jobs.service.store.WorkflowRunDatabase;
import ai.pipestream.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.pipestream.proto.jobs.service.worker.WorkflowRunWorker;
import ai.pipestream.proto.parse.playground.ParsePlaygroundServer;
import ai.pipestream.proto.parse.service.ParseWorkflows;
import ai.pipestream.proto.parse.service.ParseCoordinatorConfig;
import ai.pipestream.proto.parse.service.ParseCoordinatorServices;
import ai.pipestream.proto.parse.service.ParserRegistry;
import ai.pipestream.proto.parse.service.RoutingRules;
import ai.pipestream.proto.parse.text.TextParserService;
import ai.pipestream.proto.parse.v1.RoutingRule;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.server.SchemaRegistryServer;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import ai.pipestream.proto.repo.service.RepoServices;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaPublisher;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The document platform in one JVM: the composition root the container
 * boots. Everything the golden path proved, wired for real:
 *
 * <ul>
 *   <li>repo-service on Postgres + S3 (in-process for the siblings, Netty
 *   for external callers), lifecycle loops running;</li>
 *   <li>the git-backed schema registry, ON by default, serving the fleet
 *   document model as a published artifact from first boot, with the jobs
 *   verbs (submit-workflow, get-job, list-jobs) mounted on its actions
 *   route;</li>
 *   <li>the parsing coordinator with the embedded reference text parser (or
 *   a service-profile fleet when configured), and the {@code parse-document}
 *   workflow registered so a durable parse is one submit-workflow call;</li>
 *   <li>the durable jobs worker claiming from its own Postgres;</li>
 *   <li>the authenticated intake door;</li>
 *   <li>the streaming parser playground.</li>
 * </ul>
 *
 * <p>No DI framework: this class is the wiring, in dependency order, and
 * {@link #close()} unwinds it in reverse.
 */
public final class DocumentPlatform implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentPlatform.class);

    /** The registry subject the fleet document model publishes under. */
    public static final String DOCUMENT_SUBJECT = "ai/pipestream/document/v1/document.proto";

    private final RepoServices repo;
    private final Server repoGrpc;
    private final Server textParser;
    private final ParseCoordinatorServices coordinator;
    private final Server parseInProcess;
    private final Server parseGrpc;
    private final GitSchemaRegistryStore registry;
    private final SchemaRegistryServer registryServer;
    private final int registryHttpPort;
    private final WorkflowRunDatabase jobsDatabase;
    private final WorkflowRunWorker worker;
    private final IntakeServices intake;
    private final Server intakeGrpc;
    private final ParsePlaygroundServer playground;

    private final String repoInProcessName;
    private final String parseInProcessName;

    private DocumentPlatform(DocumentPlatformConfig config, ApiKeyIdentityResolver resolver)
            throws IOException {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        this.repoInProcessName = "platform-repo-" + suffix;
        this.parseInProcessName = "platform-parse-" + suffix;
        String textParserName = "platform-text-parser-" + suffix;

        // 1. The document store, with its lifecycle loops.
        this.repo = RepoServices.build(config.repo());
        repo.startInProcess(repoInProcessName);
        this.repoGrpc = repo.startNetty(config.repo().grpcPort());
        repo.seedAccountDrives();
        repo.startLifecycle();

        // 2. The embedded reference parser (a fleet member like any other,
        // it just happens to share the JVM).
        this.textParser = InProcessServerBuilder.forName(textParserName)
                .addService(new TextParserService())
                .build()
                .start();

        // 3. The parsing coordinator.
        RoutingRules rules = config.rulesJson() != null
                ? RoutingRules.fromJson(config.rulesJson())
                : defaultRules();
        ParserRegistry parsers = config.profilesDir() != null
                ? ParserRegistry.fromProfiles(
                        new FileSystemServiceProfileRepository(java.nio.file.Path.of(config.profilesDir())),
                        config.profileEndpoint())
                : ParserRegistry.of(Map.of(
                        TextParserService.PARSER_NAME,
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + textParserName));
        this.coordinator = ParseCoordinatorServices.build(
                new ParseCoordinatorConfig(
                        0,
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + repoInProcessName,
                        "intake",
                        config.parseDeadlineSeconds()),
                rules,
                parsers);
        // The coordinator tracks one server; the platform owns the extra
        // in-process mount (the workflow's target) and closes it itself.
        this.parseInProcess = coordinator.startInProcess(parseInProcessName);
        this.parseGrpc = coordinator.startNetty(config.parseGrpcPort());

        // 4. The jobs store and worker (its own database, its own Flyway
        // history; the disjoint per-module migration directories are what
        // make this classpath legal).
        this.jobsDatabase = new WorkflowRunDatabase(config.jobs());
        JdbcWorkflowRunStore jobs = new JdbcWorkflowRunStore(jobsDatabase);
        ActionContext context = ActionContext.create();
        // Parse checkpoints carry the parser's docling document as an Any;
        // the checkpoint transcoder resolves it through this registry.
        context.registry().registerFile(ai.pipestream.document.v1.DocumentProto.getDescriptor());

        // 5. The schema registry: ON by default, jobs verbs mounted.
        this.registry = GitSchemaRegistryStore.builder()
                .repositoryDir(config.registryGit())
                .build();
        registry.putWorkflow(
                ParseWorkflows.PARSE_DOCUMENT_WORKFLOW,
                ParseWorkflows.parseDocumentWorkflow(
                                ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + parseInProcessName,
                                config.parseDeadlineSeconds() * 1000)
                        .toString());
        WorkflowRepository workflows = workflowRepository(registry);
        ActionCatalog catalog = ActionCatalog.defaults(context)
                .register(new SubmitWorkflowAction(jobs, workflows, WorkflowRunsConfig.DEFAULT_MAX_ATTEMPTS))
                .register(new GetJobAction(jobs))
                .register(new ListJobsAction(jobs));
        this.registryServer = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withPort(config.registryPort()),
                registry,
                catalog);
        this.registryHttpPort = registryServer.start();
        publishDocumentModel();

        // 6. The worker fleet.
        WorkflowRunner runner = new WorkflowRunner(step -> {
            String target = step.target();
            if (target.startsWith(ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX)) {
                return InProcessChannelBuilder.forName(
                                target.substring(
                                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX.length()))
                        .build();
            }
            return NettyChannelBuilder.forTarget(target).usePlaintext().build();
        });
        this.worker = new WorkflowRunWorker(jobs, context, workflows, runner, new WorkflowRunsConfig(
                "document-platform",
                config.workerCount(),
                WorkflowRunsConfig.DEFAULT_LEASE_DURATION,
                WorkflowRunsConfig.DEFAULT_POLL_INTERVAL,
                WorkflowRunsConfig.DEFAULT_BACKOFF_BASE_SECONDS,
                WorkflowRunsConfig.DEFAULT_MAX_ATTEMPTS,
                WorkflowRunsConfig.DEFAULT_MAX_CONCURRENT_PER_TARGET,
                null,
                WorkflowRunsConfig.DEFAULT_EVENTS_TOPIC,
                null,
                null));
        worker.start();

        // 7. The authenticated front door.
        this.intake = IntakeServices.build(
                new IntakeServiceConfig(
                        config.intakeGrpcPort(),
                        IntakeServiceConfig.INPROCESS_TARGET_PREFIX + repoInProcessName,
                        IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES),
                resolver);
        this.intakeGrpc = intake.startNetty(config.intakeGrpcPort());

        // 8. The streaming parser playground, watching the embedded parser.
        this.playground = new ParsePlaygroundServer(
                config.playgroundPort(),
                ParsePlaygroundServer.INPROCESS_TARGET_PREFIX + textParserName,
                JsonFormat.TypeRegistry.newBuilder()
                        .add(ai.pipestream.document.v1.Document.getDescriptor())
                        .build());

        LOG.info(
                "document platform up: repo gRPC {}, intake gRPC {}, parse gRPC {},"
                        + " registry http {}, playground http {}",
                repoGrpc.getPort(), intakeGrpc.getPort(), parseGrpc.getPort(),
                registryHttpPort, playground.port());
    }

    /**
     * Builds and starts the platform.
     *
     * @param config the platform configuration
     * @param resolver the intake key store
     * @return the running platform
     * @throws IOException when a server fails to bind
     */
    public static DocumentPlatform start(DocumentPlatformConfig config, ApiKeyIdentityResolver resolver)
            throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (resolver == null) {
            throw new IllegalArgumentException("resolver must not be null");
        }
        return new DocumentPlatform(config, resolver);
    }

    /** The bound intake gRPC port. */
    public int intakePort() {
        return intakeGrpc.getPort();
    }

    /** The bound coordinator gRPC port. */
    public int parsePort() {
        return parseGrpc.getPort();
    }

    /** The bound repo gRPC port. */
    public int repoPort() {
        return repoGrpc.getPort();
    }

    /** The bound registry HTTP port. */
    public int registryPort() {
        return registryHttpPort;
    }

    /** The bound playground HTTP port. */
    public int playgroundPort() {
        return playground.port();
    }

    /** The in-process name repo-service answers on inside this JVM. */
    public String repoInProcessName() {
        return repoInProcessName;
    }

    @Override
    public void close() {
        playground.close();
        intake.close();
        worker.close();
        registryServer.close();
        try {
            registry.close();
        } catch (Exception e) {
            LOG.warn("registry close failed", e);
        }
        jobsDatabase.close();
        coordinator.close();
        parseInProcess.shutdownNow();
        textParser.shutdownNow();
        // repo tracks every server it started (in-process and Netty alike).
        repo.close();
    }

    /**
     * Publishes the fleet document model from this build's own classpath
     * (the proto rides the parse-document jar) through the registry's wire
     * API. Idempotent: an unchanged schema re-registers as the same version.
     */
    private void publishDocumentModel() throws IOException {
        String documentProto;
        try (InputStream in = DocumentPlatform.class.getClassLoader()
                .getResourceAsStream(DOCUMENT_SUBJECT)) {
            if (in == null) {
                throw new IllegalStateException(
                        "the fleet document model is not on the classpath at " + DOCUMENT_SUBJECT);
            }
            documentProto = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        ProtoSourceSet sources = ProtoSourceSet.builder()
                .add(DOCUMENT_SUBJECT, documentProto, "platform")
                .build();
        try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(
                URI.create("http://127.0.0.1:" + registryHttpPort))) {
            publisher.publish(sources, PublishOptions.defaults()).throwIfFailed();
        } catch (Exception e) {
            throw new IllegalStateException("publishing the document model failed", e);
        }
        LOG.info("registry serves {} ({} subject(s) total)", DOCUMENT_SUBJECT,
                registry.subjects().size());
    }

    private static RoutingRules defaultRules() {
        return RoutingRules.of(List.of(RoutingRule.newBuilder()
                .setRuleId("default-text")
                .setWhen("mime_type == 'text/plain' || mime_type == 'text/markdown'")
                .setParserName(TextParserService.PARSER_NAME)
                .setPriority(1)
                .build()));
    }

    private static WorkflowRepository workflowRepository(GitSchemaRegistryStore store) {
        ObjectMapper json = new ObjectMapper();
        return name -> {
            Optional<String> text = store.workflow(name);
            return text.map(t -> {
                try {
                    var node = json.readTree(t);
                    return node instanceof ObjectNode workflow ? workflow : null;
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    return null;
                }
            });
        };
    }
}
