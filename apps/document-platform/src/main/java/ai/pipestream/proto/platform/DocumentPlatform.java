package ai.pipestream.proto.platform;

import ai.pipestream.proto.composer.Channels;
import ai.pipestream.proto.composer.Composer;
import ai.pipestream.proto.intake.service.IntakeModule;
import ai.pipestream.proto.intake.service.IntakeServiceConfig;
import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.jobs.service.JobsModule;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.parse.playground.PlaygroundModule;
import ai.pipestream.proto.parse.service.ParseModule;
import ai.pipestream.proto.parse.service.RoutingRules;
import ai.pipestream.proto.parse.text.TextParserModule;
import ai.pipestream.proto.parse.text.TextParserService;
import ai.pipestream.proto.parse.v1.RoutingRule;
import ai.pipestream.proto.registry.server.RegistryModule;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import ai.pipestream.proto.repo.service.RepoServiceModule;
import ai.pipestream.proto.chunk.SentencePackedChunker;
import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.EmbeddingProviders;
import ai.pipestream.proto.embeddings.model2vec.Model2VecEmbeddingProvider;
import ai.pipestream.proto.index.spi.ChunkingPolicy;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaPublisher;
import ai.pipestream.proto.search.console.SearchConsoleModule;
import ai.pipestream.proto.search.door.RepoDocumentMapping;
import ai.pipestream.proto.search.door.SearchDoorModule;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The document platform in one JVM: the shipped one-container PRESET over
 * the {@link Composer}. The wiring that used to live here as hand-ordered
 * construction is now the role set
 * {@code repo, parser-text, registry, parse, jobs, intake, playground}
 * mounted through the ServiceModule SPI; this class only maps the
 * {@code DOCUMENT_PLATFORM_*} configuration onto module configs, boots the
 * node, and publishes the fleet document model. The same modules booted
 * with a different role list are a specialized node, not a different
 * program.
 */
public final class DocumentPlatform implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentPlatform.class);

    /** The registry subject the fleet document model publishes under. */
    public static final String DOCUMENT_SUBJECT = "ai/pipestream/document/v1/document.proto";

    private final Composer.Node node;
    private final RepoServiceModule repo;
    private final ParseModule parse;
    private final RegistryModule registry;
    private final IntakeModule intake;
    private final PlaygroundModule playground;
    private final SearchDoorModule search;
    private final SearchConsoleModule searchConsole;

    private DocumentPlatform(DocumentPlatformConfig config, ApiKeyIdentityResolver resolver)
            throws IOException {
        this.repo = new RepoServiceModule(config.repo());
        this.registry = new RegistryModule(
                config.registryGit(),
                SchemaRegistryServerConfig.defaults().withPort(config.registryPort()));
        RoutingRules rules = config.rulesJson() != null
                ? RoutingRules.fromJson(config.rulesJson())
                : defaultRules();
        this.parse = new ParseModule(new ParseModule.Config(
                rules,
                config.profilesDir() != null
                        ? Map.of()
                        : Map.of(TextParserService.PARSER_NAME, TextParserModule.ROLE),
                config.profilesDir() != null ? Path.of(config.profilesDir()) : null,
                config.profileEndpoint(),
                config.parseDeadlineSeconds(),
                config.parseGrpcPort()));
        JobsModule jobs = new JobsModule(config.jobs(), new WorkflowRunsConfig(
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
        this.intake = new IntakeModule(new IntakeModule.Config(
                config.intakeGrpcPort(),
                -1,
                IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES,
                resolver));
        this.playground = new PlaygroundModule(
                config.playgroundPort(), PlaygroundModule.DEFAULT_PARSER_ROLE);
        this.search = new SearchDoorModule(new SearchDoorModule.Config(
                config.searchGrpcPort(),
                config.searchIndexDir(),
                Map.of(RepoDocumentMapping.SUBJECT,
                        RepoDocumentMapping.served(defaultChunkingPolicy()))));
        this.searchConsole = new SearchConsoleModule(new SearchConsoleModule.Config(
                config.searchConsolePort(),
                () -> "http://127.0.0.1:" + registry.httpPort() + "/protomolt/actions"));

        Composer composer = Composer.emptyBuilder()
                .module(repo)
                .module(new TextParserModule())
                .module(registry)
                .module(parse)
                .module(jobs)
                .module(intake)
                .module(playground)
                .module(search)
                .module(searchConsole)
                .environment(Map.of())
                .remoteOpener(target -> NettyChannelBuilder.forTarget(target).usePlaintext().build())
                .build();
        this.node = composer.boot(List.of(
                RepoServiceModule.ROLE, TextParserModule.ROLE, RegistryModule.ROLE,
                ParseModule.ROLE, JobsModule.ROLE, IntakeModule.ROLE, PlaygroundModule.ROLE,
                SearchDoorModule.ROLE, SearchConsoleModule.ROLE));
        try {
            publishDocumentModel();
        } catch (RuntimeException | IOException e) {
            node.close();
            throw e;
        }

        LOG.info(
                "document platform up: repo gRPC {}, intake gRPC {}, parse gRPC {},"
                        + " search gRPC {}, registry http {}, playground http {},"
                        + " search console http {}",
                repo.grpcPort(), intake.grpcPort(), parse.grpcPort(), search.grpcPort(),
                registry.httpPort(), playground.port(), searchConsole.port());
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
        return intake.grpcPort();
    }

    /** The bound coordinator gRPC port. */
    public int parsePort() {
        return parse.grpcPort();
    }

    /** The bound repo gRPC port. */
    public int repoPort() {
        return repo.grpcPort();
    }

    /** The bound registry HTTP port. */
    public int registryPort() {
        return registry.httpPort();
    }

    /** The bound playground HTTP port. */
    public int playgroundPort() {
        return playground.port();
    }

    /** The bound search door gRPC port. */
    public int searchPort() {
        return search.grpcPort();
    }

    /** The bound search console HTTP port. */
    public int searchConsolePort() {
        return searchConsole.port();
    }

    /** The in-process name repo-service answers on inside this JVM. */
    public String repoInProcessName() {
        return node.context().channels().targetOf(RepoServiceModule.ROLE)
                .substring(Channels.IN_PROCESS_PREFIX.length());
    }

    @Override
    public void close() {
        node.close();
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
                URI.create("http://127.0.0.1:" + registry.httpPort()))) {
            publisher.publish(sources, PublishOptions.defaults()).throwIfFailed();
        } catch (Exception e) {
            throw new IllegalStateException("publishing the document model failed", e);
        }
        LOG.info("registry serves {} ({} subject(s) total)", DOCUMENT_SUBJECT,
                registry.store().subjects().size());
    }

    /**
     * The default chunk lane over the folded body: on when a Model2Vec
     * model is configured ({@code protomolt.embeddings.model2vec.path} or
     * {@code PROTOMOLT_MODEL2VEC_PATH}), off otherwise — lexical search
     * always works, the vector lane activates with the model. The policy's
     * dims come from the loaded model, never assumed, so a different model
     * is a different policy digest and a re-derived corpus.
     */
    private static ChunkingPolicy defaultChunkingPolicy() {
        boolean configured =
                System.getProperty(Model2VecEmbeddingProvider.PATH_PROPERTY) != null
                        || System.getenv(
                                Model2VecEmbeddingProvider.PATH_ENVIRONMENT_VARIABLE) != null;
        if (!configured) {
            LOG.info("search: no Model2Vec model configured; the repo-document subject"
                    + " serves the lexical lane only (set {} to activate vectors)",
                    Model2VecEmbeddingProvider.PATH_ENVIRONMENT_VARIABLE);
            return null;
        }
        EmbeddingProvider provider =
                EmbeddingProviders.byId(Model2VecEmbeddingProvider.PROVIDER_ID);
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        120, 16, 20, 200, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        Model2VecEmbeddingProvider.PROVIDER_ID, provider.dimension(),
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    private static RoutingRules defaultRules() {
        return RoutingRules.of(List.of(RoutingRule.newBuilder()
                .setRuleId("default-text")
                .setWhen("mime_type == 'text/plain' || mime_type == 'text/markdown'")
                .setParserName(TextParserService.PARSER_NAME)
                .setPriority(1)
                .build()));
    }
}
