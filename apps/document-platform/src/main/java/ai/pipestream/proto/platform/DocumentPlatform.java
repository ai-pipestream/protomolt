package ai.pipestream.proto.platform;

import ai.pipestream.proto.acquire.jdbc.JdbcPullModule;
import ai.pipestream.proto.acquire.s3.S3PullModule;
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
import ai.pipestream.proto.registry.service.RegistryModule;
import ai.pipestream.proto.registry.service.SchemaRegistryServerConfig;
import ai.pipestream.proto.repo.service.RepoServiceModule;
import ai.pipestream.proto.search.chunk.SentencePackedChunker;
import ai.pipestream.proto.search.embedding.EmbeddingProvider;
import ai.pipestream.proto.search.embedding.EmbeddingProviders;
import ai.pipestream.proto.search.embedding.model2vec.Model2VecEmbeddingProvider;
import ai.pipestream.proto.search.index.spi.ChunkingPolicy;
import ai.pipestream.proto.search.index.spi.VectorSimilarity;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.iceberg.IcebergMetricExecutor;
import ai.pipestream.proto.metric.iceberg.IcebergRollupSink;
import ai.pipestream.proto.metric.iceberg.IcebergRollupSubjects;
import ai.pipestream.proto.metric.lucene.MetricServiceModule;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.RollupSink;
import ai.pipestream.proto.iceberg.LocalFileIO;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaPublisher;
import ai.pipestream.proto.search.console.SearchConsoleModule;
import ai.pipestream.proto.search.service.IndexSnapshots;
import ai.pipestream.proto.search.service.RepoDocumentMapping;
import ai.pipestream.proto.search.service.SearchServiceModule;
import ai.pipestream.proto.search.snapshot.s3.S3SnapshotStore;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.authz.AccessPolicies;
import ai.pipestream.proto.authz.AccessPolicyCallers;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The document platform in one JVM: role selection over the
 * {@link Composer}. The default is the full one-container preset; setting
 * {@code PROTOMOLT_ROLES} boots the same binary as a specialized node (a
 * repo node, a search node, a connector node), with absent roles reached
 * remotely through {@code PROTOMOLT_<ROLE>_TARGET}. This class only maps
 * the {@code DOCUMENT_PLATFORM_*} configuration onto module configs for
 * the selected roles, boots the node, and (when the registry is local)
 * publishes the fleet document model. One binary, many roles: a different
 * role list is a different node, never a different program.
 */
public final class DocumentPlatform implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentPlatform.class);

    /** The registry subject the fleet document model publishes under. */
    public static final String DOCUMENT_SUBJECT = "ai/pipestream/proto/parse/document/v1/document.proto";

    /** The registry subject the routing contract publishes under. */
    public static final String ROUTING_SUBJECT =
            "ai/pipestream/proto/parse/v1/routing.proto";

    /** The registry subject the validation dialect publishes under. */
    public static final String VALIDATE_SUBJECT =
            "ai/pipestream/proto/validate/v1/validate.proto";

    /** The registry subject the access-policy contract publishes under. */
    public static final String ACCESS_POLICY_SUBJECT =
            "ai/pipestream/proto/authz/v1/access_policy.proto";

    /** The config subject the parse role's routing rules read: {@value}. */
    public static final String PARSE_ROUTING_CONFIG_SUBJECT = "parse-routing";

    private final Composer.Node node;
    private final RepoServiceModule repo;
    private final ParseModule parse;
    private final RegistryModule registry;
    private final IntakeModule intake;
    private final PlaygroundModule playground;
    private final SearchServiceModule search;
    private final MetricServiceModule metrics;
    private final SearchConsoleModule searchConsole;
    private final S3Client snapshotClient;
    private final Catalog metricsCatalog;
    private final LocalLakeRollupSink defaultRollupLake;
    private final ai.pipestream.proto.config.DistributedConfig distributedConfig;
    private final SwappableTaxonomyCatalog taxonomies;
    private final SwappablePostalCodes postalCodes;
    private final SwappableScreening screening;
    private final String screeningMountName;
    private final java.util.concurrent.ScheduledExecutorService configRefresher;
    private final String apiToken;
    private final SwappableCallers callers;

    private DocumentPlatform(DocumentPlatformConfig config, ApiKeyIdentityResolver resolver)
            throws IOException {
        if (config.mounts(IntakeModule.ROLE) && resolver == null) {
            throw new IllegalArgumentException(
                    "resolver is required: this node mounts the intake service");
        }
        this.apiToken = apiTokenFromEnvironment(config.environment());
        java.nio.file.Path accessPolicyFile =
                accessPolicyFromEnvironment(config.environment());
        if (accessPolicyFile != null && apiToken == null) {
            throw new IllegalArgumentException(DocumentPlatformConfig.ENV_ACCESS_POLICY
                    + " is set but the operator token is not: an access policy"
                    + " requires " + DocumentPlatformConfig.ENV_API_TOKEN);
        }
        this.callers = apiToken == null ? null : new SwappableCallers();
        if (accessPolicyFile != null) {
            try {
                callers.swap(new AccessPolicyCallers(
                        AccessPolicies.load(accessPolicyFile)));
                LOG.info("access policy mounted from {}", accessPolicyFile);
            } catch (IOException e) {
                throw new IllegalStateException("failed to read the access policy at "
                        + accessPolicyFile + ": " + e.getMessage(), e);
            }
        }
        Composer.Builder composer = Composer.emptyBuilder()
                .environment(config.environment())
                .remoteOpener(target -> {
                    NettyChannelBuilder builder =
                            NettyChannelBuilder.forTarget(target).usePlaintext();
                    if (apiToken != null) {
                        // A guarded node presents its own authority to its
                        // remote roles; unguarded roles ignore the header.
                        Metadata identity = new Metadata();
                        identity.put(Metadata.Key.of("api_token",
                                Metadata.ASCII_STRING_MARSHALLER), apiToken);
                        builder.intercept(
                                MetadataUtils.newAttachHeadersInterceptor(identity));
                    }
                    return builder.build();
                });

        this.repo = config.mounts(RepoServiceModule.ROLE)
                ? new RepoServiceModule(config.repo()) : null;
        this.registry = config.mounts(RegistryModule.ROLE)
                ? new RegistryModule(
                        config.registryGit(),
                        SchemaRegistryServerConfig.defaults()
                                .withPort(config.registryPort())
                                .withApiToken(apiToken),
                        callers)
                : null;
        if (config.mounts(ParseModule.ROLE)) {
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
        } else {
            this.parse = null;
        }
        JobsModule jobs = config.mounts(JobsModule.ROLE)
                ? new JobsModule(config.jobs(), new WorkflowRunsConfig(
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
                        null))
                : null;
        this.intake = config.mounts(IntakeModule.ROLE)
                ? new IntakeModule(new IntakeModule.Config(
                        config.intakeGrpcPort(),
                        -1,
                        IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES,
                        resolver))
                : null;
        this.playground = config.mounts(PlaygroundModule.ROLE)
                ? new PlaygroundModule(
                        config.playgroundPort(), PlaygroundModule.DEFAULT_PARSER_ROLE)
                : null;
        SearchSnapshotConfig snapshotConfig = config.mounts(SearchServiceModule.ROLE)
                ? SearchSnapshotConfig.fromEnvironment(config.environment())
                : null;
        boolean searchReadOnly = config.mounts(SearchServiceModule.ROLE)
                && searchReadOnly(config.environment());
        long searchRefreshSeconds = config.mounts(SearchServiceModule.ROLE)
                ? searchRefreshSeconds(config.environment())
                : 0L;
        if (searchRefreshSeconds > 0 && !searchReadOnly) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS
                            + " is set but this node is a writer: refresh is the"
                            + " reader's pull, so set "
                            + DocumentPlatformConfig.ENV_SEARCH_READ_ONLY
                            + "=true or unset the interval");
        }
        this.snapshotClient = snapshotConfig == null ? null : snapshotClient(snapshotConfig);
        // The taxonomy follow: names parsed up front, the swappable catalog
        // handed to the service before boot, the mounts swapped in once the
        // config lane exists. Fail-closed until then.
        List<String> taxonomyNames = taxonomiesFromEnvironment(config.environment());
        this.taxonomies = taxonomyNames.isEmpty() ? null : new SwappableTaxonomyCatalog();
        this.screeningMountName = screeningFromEnvironment(config.environment());
        this.screening = screeningMountName == null ? null : new SwappableScreening();
        this.postalCodes = postalCodesFromEnvironment(config.environment())
                ? new SwappablePostalCodes() : null;
        this.search = config.mounts(SearchServiceModule.ROLE)
                ? new SearchServiceModule(new SearchServiceModule.Config(
                        config.searchGrpcPort(),
                        config.searchIndexDir(),
                        Map.of(RepoDocumentMapping.SUBJECT,
                                RepoDocumentMapping.served(defaultChunkingPolicy())),
                        snapshotConfig == null
                                ? null
                                : new IndexSnapshots(new S3SnapshotStore(
                                        snapshotClient,
                                        snapshotConfig.bucket(),
                                        snapshotConfig.prefix()),
                                        searchReadOnly),
                        searchReadOnly,
                        searchRefreshSeconds,
                        taxonomies,
                        screening,
                        postalCodes)
                        .secured(apiToken, callers))
                : null;
        MetricsIcebergConfig lakeConfig = config.mounts(MetricServiceModule.ROLE)
                ? MetricsIcebergConfig.fromEnvironment(config.environment())
                : null;
        this.metricsCatalog = lakeConfig == null ? null : metricsCatalog(lakeConfig);
        // Rollups always have somewhere to land: the configured lake when
        // the family is set, a lazily created local lake otherwise. The
        // default mounts the SINK only, never the Iceberg query backend,
        // so a plain single-engine mount keeps answering unset-backend
        // queries with Lucene.
        this.defaultRollupLake = config.mounts(MetricServiceModule.ROLE) && lakeConfig == null
                ? new LocalLakeRollupSink(Path.of(metricsLakeDir(config.environment())))
                : null;
        RollupSink rollupSink = lakeConfig != null
                ? new IcebergRollupSink(metricsCatalog, lakeConfig.namespace())
                : defaultRollupLake;
        ai.pipestream.proto.metric.spi.MetricSubjectResolver rollupSubjects =
                lakeConfig != null
                        ? new IcebergRollupSubjects(metricsCatalog, lakeConfig.namespace())
                        : defaultRollupLake == null ? null : defaultRollupLake.resolver();
        this.metrics = config.mounts(MetricServiceModule.ROLE)
                ? new MetricServiceModule(new MetricServiceModule.Config(
                        config.metricsGrpcPort(),
                        Map.of(RepoDocumentMapping.SUBJECT, new MetricServiceModule.Subject(
                                RepoDocumentMetrics.mapping(),
                                RepoDocumentMapping.mapping(),
                                lakeConfig == null
                                        ? Map.of()
                                        : Map.of(MetricBackend.METRIC_BACKEND_ICEBERG,
                                                new IcebergMetricExecutor(lakeTables(
                                                        metricsCatalog,
                                                        lakeConfig.namespace()))))),
                        rollupSink,
                        rollupSubjects)
                        .secured(apiToken, callers))
                : null;
        // On a guarded node the console demands browser logins bound to access-policy
        // principals; the swappable resolver means a policy arriving on the config lane
        // starts admitting logins with no restart.
        this.searchConsole = config.mounts(SearchConsoleModule.ROLE)
                ? new SearchConsoleModule(apiToken == null
                        ? new SearchConsoleModule.Config(
                                config.searchConsolePort(), actionsBaseUrl(config))
                        : new SearchConsoleModule.Config(
                                config.searchConsolePort(), actionsBaseUrl(config))
                                .secured(callers))
                : null;

        List<ai.pipestream.proto.composer.ServiceModule> selected = new ArrayList<>();
        for (ai.pipestream.proto.composer.ServiceModule module
                : new ai.pipestream.proto.composer.ServiceModule[] {
                        repo, registry, parse, jobs, intake, playground, search,
                        metrics, searchConsole}) {
            if (module != null) {
                selected.add(module);
            }
        }
        if (config.mounts(TextParserModule.ROLE)) {
            selected.add(new TextParserModule());
        }
        if (config.mounts(S3PullModule.ROLE)) {
            selected.add(new S3PullModule(
                    S3PullModule.Config.fromEnvironment(config.environment())));
        }
        if (config.mounts(JdbcPullModule.ROLE)) {
            selected.add(new JdbcPullModule(
                    JdbcPullModule.Config.fromEnvironment(config.environment())));
        }
        for (ai.pipestream.proto.composer.ServiceModule module : selected) {
            composer.module(module);
        }

        long configRefreshSeconds = configRefreshSeconds(config.environment());
        this.node = composer.build().boot(config.roles());
        try {
            if (registry != null) {
                publishDocumentModel();
            }
            if (taxonomies != null && configRefreshSeconds <= 0) {
                throw new IllegalArgumentException(
                        DocumentPlatformConfig.ENV_TAXONOMIES + " is set but the config"
                                + " lane is off: taxonomy mounts arrive as config"
                                + " documents, so set "
                                + DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                                + " or unset the taxonomies");
            }
            if (screening != null && configRefreshSeconds <= 0) {
                throw new IllegalArgumentException(
                        DocumentPlatformConfig.ENV_SCREENING + " is set but the config"
                                + " lane is off: screening mounts arrive as config"
                                + " documents, so set "
                                + DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                                + " or unset the screening mount");
            }
            if (postalCodes != null && configRefreshSeconds <= 0) {
                throw new IllegalArgumentException(
                        DocumentPlatformConfig.ENV_POSTAL_CODES + " is set but the config"
                                + " lane is off: the postal-code pack arrives as a config"
                                + " document, so set "
                                + DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                                + " or unset it");
            }
            if (configRefreshSeconds > 0) {
                if (parse == null && taxonomies == null && screening == null
                        && postalCodes == null && callers == null) {
                    throw new IllegalArgumentException(
                            DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                                    + " is set but this node mounts no config consumer:"
                                    + " the config lane serves the parse role's routing"
                                    + " rules, the taxonomy and screening mounts, and"
                                    + " the access policy, so mount 'parse', set "
                                    + DocumentPlatformConfig.ENV_TAXONOMIES + ", "
                                    + DocumentPlatformConfig.ENV_SCREENING + ", or "
                                    + DocumentPlatformConfig.ENV_API_TOKEN
                                    + ", or unset the interval");
                }
                ai.pipestream.proto.config.kafka.KafkaConfigSource.Config kafka =
                        kafkaConfigFamily(config.environment());
                ai.pipestream.proto.config.ConfigSource configSource;
                if (kafka != null) {
                    configSource = new ai.pipestream.proto.config.kafka.KafkaConfigSource(
                            kafka);
                } else {
                    String configUrl = config.environment()
                            .getOrDefault(DocumentPlatformConfig.ENV_CONFIG_URL, "").trim();
                    if (configUrl.isEmpty()) {
                        if (registry == null) {
                            throw new IllegalArgumentException(
                                    DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                                            + " is set but this node has no registry to"
                                            + " pull from: mount 'registry', set "
                                            + DocumentPlatformConfig.ENV_CONFIG_URL
                                            + ", or set the "
                                            + DocumentPlatformConfig
                                                    .ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS
                                            + " family");
                        }
                        configUrl = "http://127.0.0.1:" + registry.httpPort() + "/protomolt";
                    }
                    configSource = new ai.pipestream.proto.config.registry
                            .RegistryConfigSource(configUrl, apiToken);
                }
                this.distributedConfig = ai.pipestream.proto.config.DistributedConfig
                        .over(configSource);
                if (parse != null) {
                    ai.pipestream.proto.config.DistributedConfig
                            .Subscription<ai.pipestream.proto.parse.v1.RoutingConfig> routing =
                            distributedConfig.subscribe(PARSE_ROUTING_CONFIG_SUBJECT,
                                    ai.pipestream.proto.parse.v1.RoutingConfig
                                            .getDefaultInstance());
                    routing.onChange((document, version) -> {
                        parse.swapRules(RoutingRules.of(document.getRulesList()));
                        LOG.info("parse routing rules applied from config '{}' version {}"
                                + " ({} rule(s))", PARSE_ROUTING_CONFIG_SUBJECT, version,
                                document.getRulesCount());
                    });
                }
                if (taxonomies != null) {
                    // The document gate was already handed the swappable
                    // catalog; from here the mounts follow the config lane.
                    taxonomies.swap(ai.pipestream.proto.config.TaxonomyMounts
                            .follow(distributedConfig, taxonomyNames));
                    LOG.info("taxonomy mounts following {}", taxonomyNames);
                }
                if (postalCodes != null) {
                    // The gate was handed the swappable catalog; unmounted
                    // regions stay unchecked until the pack applies.
                    postalCodes.swap(ai.pipestream.proto.config.PostalCodeMounts
                            .follow(distributedConfig));
                    LOG.info("postal-code pack following '{}'",
                            ai.pipestream.proto.config.PostalCodeMounts.SUBJECT);
                }
                if (screening != null) {
                    // The service was already handed the swappable mount; it
                    // refuses fail-closed until the first document applies.
                    String subject = "screening:" + screeningMountName;
                    distributedConfig.subscribe(subject,
                            ai.pipestream.proto.types.ScreeningConfig.getDefaultInstance())
                            .onChange((document, version) -> {
                                screening.swap(mountScreener(document));
                                LOG.info("screening mount applied from config '{}'"
                                        + " version {} (class '{}', policy {})", subject,
                                        version, document.getSensitivityClass(),
                                        document.getPolicy());
                            });
                    LOG.info("screening mount following '{}'", subject);
                }
                if (callers != null) {
                    // The policy follows the lane like every other mount: a
                    // malformed document throws here, the refresh logs it,
                    // and the previous policy stays live.
                    distributedConfig.subscribe(
                            ai.pipestream.proto.config.AccessPolicyMounts.SUBJECT,
                            ai.pipestream.proto.authz.AccessPolicy.getDefaultInstance())
                            .onChange((policy, version) -> {
                                callers.swap(new AccessPolicyCallers(policy));
                                LOG.info("access policy applied from config '{}'"
                                        + " version {} ({} principal(s))",
                                        ai.pipestream.proto.config
                                                .AccessPolicyMounts.SUBJECT,
                                        version, policy.getPrincipalsCount());
                            });
                }
                // The boot pull: a present config document is the routing
                // truth from the first plan, ahead of the environment's
                // boot rules.
                distributedConfig.refresh();
                this.configRefresher = java.util.concurrent.Executors
                        .newSingleThreadScheduledExecutor(runnable -> {
                            Thread thread = new Thread(runnable, "platform-config-refresh");
                            thread.setDaemon(true);
                            return thread;
                        });
                configRefresher.scheduleWithFixedDelay(distributedConfig::refresh,
                        configRefreshSeconds, configRefreshSeconds,
                        java.util.concurrent.TimeUnit.SECONDS);
            } else {
                this.distributedConfig = null;
                this.configRefresher = null;
            }
        } catch (RuntimeException | IOException e) {
            node.close();
            throw e;
        }

        List<String> surfaces = new ArrayList<>();
        if (repo != null) {
            surfaces.add("repo gRPC " + repo.grpcPort());
        }
        if (intake != null) {
            surfaces.add("intake gRPC " + intake.grpcPort());
        }
        if (parse != null) {
            surfaces.add("parse gRPC " + parse.grpcPort());
        }
        if (search != null) {
            surfaces.add("search gRPC " + search.grpcPort());
        }
        if (snapshotConfig != null) {
            surfaces.add("search snapshots s3://" + snapshotConfig.bucket()
                    + "/" + snapshotConfig.prefix());
        }
        if (metrics != null) {
            surfaces.add("metrics gRPC " + metrics.grpcPort());
        }
        if (lakeConfig != null) {
            surfaces.add("metrics lake " + lakeConfig.catalogUri()
                    + " namespace " + lakeConfig.namespace());
        } else if (defaultRollupLake != null) {
            surfaces.add("metrics rollups land in the local lake "
                    + metricsLakeDir(config.environment()) + " (created on first rebuild)");
        }
        if (registry != null) {
            surfaces.add("registry http " + registry.httpPort());
        }
        if (distributedConfig != null) {
            surfaces.add("distributed config every " + configRefreshSeconds + "s");
        }
        if (playground != null) {
            surfaces.add("playground http " + playground.port());
        }
        if (searchConsole != null) {
            surfaces.add("search console http " + searchConsole.port());
        }
        if (apiToken != null) {
            surfaces.add(accessPolicyFile != null
                    ? "operator token + access policy"
                    : "operator token");
        }
        LOG.info("document platform up as roles {}: {}",
                config.roles(), String.join(", ", surfaces));
    }

    /**
     * The operations panel's actions route: the co-mounted registry when this
     * node runs one, an explicitly configured remote route otherwise
     * ({@code DOCUMENT_PLATFORM_ACTIONS_URL}; absent disables the panel).
     */
    private Supplier<String> actionsBaseUrl(DocumentPlatformConfig config) {
        if (registry != null) {
            return () -> "http://127.0.0.1:" + registry.httpPort() + "/protomolt/actions";
        }
        String remote = config.environment()
                .getOrDefault(DocumentPlatformConfig.ENV_ACTIONS_URL, "");
        return () -> remote;
    }

    /**
     * Builds and starts the platform.
     *
     * @param config the platform configuration
     * @param resolver the intake key store; required exactly when the role
     *        list mounts the intake service, ignored otherwise
     * @return the running platform
     * @throws IOException when a server fails to bind
     */
    public static DocumentPlatform start(DocumentPlatformConfig config, ApiKeyIdentityResolver resolver)
            throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        return new DocumentPlatform(config, resolver);
    }

    /** The parse role's currently live routing rules (tests and probes). */
    RoutingRules parseRoutingRules() {
        return mounted(parse, ParseModule.ROLE).currentRules();
    }

    /** The bound intake gRPC port. */
    public int intakePort() {
        return mounted(intake, IntakeModule.ROLE).grpcPort();
    }

    /** The bound coordinator gRPC port. */
    public int parsePort() {
        return mounted(parse, ParseModule.ROLE).grpcPort();
    }

    /** The bound repo gRPC port. */
    public int repoPort() {
        return mounted(repo, RepoServiceModule.ROLE).grpcPort();
    }

    /** The bound registry HTTP port. */
    public int registryPort() {
        return mounted(registry, RegistryModule.ROLE).httpPort();
    }

    /** The bound playground HTTP port. */
    public int playgroundPort() {
        return mounted(playground, PlaygroundModule.ROLE).port();
    }

    /** The bound search service gRPC port. */
    public int searchPort() {
        return mounted(search, SearchServiceModule.ROLE).grpcPort();
    }

    /** The bound metric service gRPC port. */
    public int metricsPort() {
        return mounted(metrics, MetricServiceModule.ROLE).grpcPort();
    }

    /** The bound search console HTTP port. */
    public int searchConsolePort() {
        return mounted(searchConsole, SearchConsoleModule.ROLE).port();
    }

    private static <T> T mounted(T module, String role) {
        if (module == null) {
            throw new IllegalStateException(
                    "role '" + role + "' is not mounted on this node");
        }
        return module;
    }

    /** The in-process name repo-service answers on inside this JVM. */
    public String repoInProcessName() {
        return node.context().channels().targetOf(RepoServiceModule.ROLE)
                .substring(Channels.IN_PROCESS_PREFIX.length());
    }

    @Override
    public void close() {
        // The node closes first: the search module's close runs the final
        // commit-and-snapshot, which needs the client still open, and the
        // metric service stops serving before its catalog goes away.
        node.close();
        if (snapshotClient != null) {
            snapshotClient.close();
        }
        if (metricsCatalog instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("closing the metrics lake catalog failed", e);
            }
        }
        if (defaultRollupLake != null) {
            defaultRollupLake.close();
        }
        if (configRefresher != null) {
            configRefresher.shutdownNow();
        }
        if (distributedConfig != null) {
            distributedConfig.close();
        }
    }

    /** The default local metrics lake directory, per the environment. */
    static String metricsLakeDir(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_METRICS_LAKE_DIR, "").trim();
        return value.isEmpty() ? DocumentPlatformConfig.DEFAULT_METRICS_LAKE_DIR : value;
    }

    /**
     * The rollup sink's default home when no catalog family is set: a
     * local lake (sqlite catalog, Parquet data, the sink's LocalFileIO)
     * created lazily on the first rebuild, so a node that never rebuilds
     * a rollup never touches the directory. The rollup stays a lake
     * table: protobuf-schema'd Parquet any engine can scan, exportable or
     * indexable later without asking this node.
     */
    private static final class LocalLakeRollupSink
            implements ai.pipestream.proto.metric.spi.RollupSink {

        private final Path dir;
        private JdbcCatalog catalog;
        private ai.pipestream.proto.metric.spi.RollupSink delegate;
        private IcebergRollupSubjects subjects;

        LocalLakeRollupSink(Path dir) {
            this.dir = dir;
        }

        @Override
        public synchronized Written replace(String sourceSubject, String table,
                List<String> dimensions, List<MeasureColumn> measures,
                List<ai.pipestream.proto.metric.MetricRow> rows) {
            initialize(true);
            return delegate.replace(sourceSubject, table, dimensions, measures, rows);
        }

        /**
         * Rollup tables in this lake as queryable subjects. Resolution
         * initializes lazily too, from an existing catalog file only: a
         * lake that was never written has no rollups to resolve, and a
         * read must not create one.
         */
        ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver() {
            return subject -> {
                IcebergRollupSubjects resolved;
                synchronized (this) {
                    if (subjects == null) {
                        if (!java.nio.file.Files.exists(dir.resolve("catalog.db"))) {
                            return null;
                        }
                        initialize(false);
                    }
                    resolved = subjects;
                }
                return resolved.resolve(subject);
            };
        }

        private synchronized void initialize(boolean create) {
            if (delegate != null) {
                return;
            }
            if (create) {
                try {
                    java.nio.file.Files.createDirectories(dir);
                } catch (IOException e) {
                    throw new IllegalStateException("cannot create the default metrics"
                            + " lake at " + dir + ": set "
                            + DocumentPlatformConfig.ENV_METRICS_LAKE_DIR
                            + " to a writable directory, or the "
                            + MetricsIcebergConfig.ENV_CATALOG_URI + " family", e);
                }
            }
            JdbcCatalog jdbc = new JdbcCatalog();
            jdbc.initialize(MetricsIcebergConfig.CATALOG_NAME, Map.of(
                    CatalogProperties.URI,
                    "jdbc:sqlite:" + dir.resolve("catalog.db"),
                    CatalogProperties.WAREHOUSE_LOCATION, dir.toString(),
                    CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
            org.apache.iceberg.catalog.Namespace namespace =
                    org.apache.iceberg.catalog.Namespace.of(
                            MetricsIcebergConfig.DEFAULT_NAMESPACE);
            // This lake is the platform's own storage, like the search
            // index directory, so the namespace is ours to create.
            if (!jdbc.namespaceExists(namespace)) {
                jdbc.createNamespace(namespace);
            }
            catalog = jdbc;
            delegate = new IcebergRollupSink(
                    jdbc, MetricsIcebergConfig.DEFAULT_NAMESPACE);
            subjects = new IcebergRollupSubjects(
                    jdbc, MetricsIcebergConfig.DEFAULT_NAMESPACE);
        }

        synchronized void close() {
            if (catalog != null) {
                try {
                    catalog.close();
                } catch (Exception e) {
                    LOG.warn("closing the default metrics lake failed", e);
                }
            }
        }
    }

    /**
     * The Iceberg catalog the metrics role's lake engine reads, per the
     * family's settings: the URI scheme picks JDBC or REST, and both run
     * the sink's {@code LocalFileIO} because this build is Hadoop-free
     * and DuckDB reads the table's local Parquet paths directly.
     */
    private static Catalog metricsCatalog(MetricsIcebergConfig config) {
        Map<String, String> properties = new java.util.HashMap<>();
        properties.put(CatalogProperties.URI, config.catalogUri());
        if (config.s3()) {
            // The lake's file plane on an S3-compatible store, through
            // Iceberg's own S3FileIO: one credential path shared by the
            // catalog, the sink, and the metric reader.
            properties.putAll(ai.pipestream.proto.iceberg.s3.S3Catalogs
                    .awsRegion(config.s3Region()));
            if (!config.s3Endpoint().isEmpty()) {
                properties.put(org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT,
                        config.s3Endpoint());
                properties.put(
                        org.apache.iceberg.aws.s3.S3FileIOProperties.PATH_STYLE_ACCESS,
                        "true");
            }
            if (!config.s3AccessKey().isEmpty()) {
                properties.put(org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID,
                        config.s3AccessKey());
                properties.put(
                        org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY,
                        config.s3SecretKey());
            }
        } else {
            properties.put(CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName());
        }
        if (!config.warehouse().isEmpty()) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
        }
        Catalog catalog = config.jdbc() ? new JdbcCatalog() : new RESTCatalog();
        // The name scopes a JDBC catalog's table records in the backing
        // database: writers sharing the database must use the same name.
        catalog.initialize(MetricsIcebergConfig.CATALOG_NAME, properties);
        return catalog;
    }

    /**
     * Each metric subject reads the lake table named exactly like it,
     * loaded per query so the sink can write the table after this node
     * boots and each read sees the current snapshot. A subject whose
     * table does not exist yet refuses by name: the sink writes tables,
     * this reader never creates one.
     */
    private static IcebergMetricExecutor.SubjectTables lakeTables(
            Catalog catalog, String namespace) {
        return subject -> {
            TableIdentifier identifier = TableIdentifier.of(namespace, subject);
            try {
                return catalog.loadTable(identifier);
            } catch (NoSuchTableException e) {
                throw new MetricRefusal(MetricRefusal.MISSING_TABLE,
                        "the lake has no table '" + identifier + "' for subject '"
                                + subject + "': the Iceberg sink writes it",
                        List.of());
            }
        };
    }

    /**
     * The strict read of
     * {@link DocumentPlatformConfig#ENV_SEARCH_REFRESH_SECONDS}: absent is
     * restart-only ({@code 0}), anything set must be a positive whole
     * number of seconds.
     */
    static long searchRefreshSeconds(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS, "").trim();
        if (value.isEmpty()) {
            return 0L;
        }
        long seconds;
        try {
            seconds = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS
                            + " must be a positive number of seconds, got '" + value + "'");
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS
                            + " must be a positive number of seconds, got '" + value
                            + "'; unset it for restart-only");
        }
        return seconds;
    }

    /**
     * The strict read of {@link DocumentPlatformConfig#ENV_SCREENING}: one
     * screening mount name, trimmed. Absent means no screening, exactly as
     * before.
     */
    /** The strict read of DOCUMENT_PLATFORM_POSTAL_CODES: true, or unset. */
    static boolean postalCodesFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_POSTAL_CODES, "").trim();
        if (value.isEmpty()) {
            return false;
        }
        if ("true".equals(value)) {
            return true;
        }
        throw new IllegalArgumentException(DocumentPlatformConfig.ENV_POSTAL_CODES
                + " must be 'true' or unset, not '" + value + "'");
    }

    /** The operator token ({@code PROTOMOLT_API_TOKEN}), or null for an open node. */
    static String apiTokenFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_API_TOKEN, "").trim();
        return value.isEmpty() ? null : value;
    }

    /** The boot access-policy file ({@code PROTOMOLT_ACCESS_POLICY}), or null. */
    static java.nio.file.Path accessPolicyFromEnvironment(
            Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_ACCESS_POLICY, "").trim();
        return value.isEmpty() ? null : java.nio.file.Path.of(value);
    }

    static String screeningFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_SCREENING, "").trim();
        return value.isEmpty() ? null : value;
    }

    /**
     * Builds the mounted screener from one applied config document. The
     * model reference resolves as a local file ({@code file:<path>}) in this
     * slice: model artifacts are operator-mounted data, never bundled. A
     * reference the host cannot resolve throws, which the config lane logs
     * without applying, so the previous mount (or the fail-closed absence)
     * stays live.
     */
    static ai.pipestream.proto.screening.Screener mountScreener(
            ai.pipestream.proto.types.ScreeningConfig config) {
        String ref = config.getModelRef();
        if (!ref.startsWith("file:")) {
            throw new IllegalArgumentException("screening model_ref must be a file:"
                    + " reference in this slice, got '" + ref + "'");
        }
        java.nio.file.Path path = java.nio.file.Path.of(ref.substring("file:".length()));
        try (java.io.InputStream model = java.nio.file.Files.newInputStream(path)) {
            return new ai.pipestream.proto.screening.Screener(
                    ai.pipestream.proto.screening.OpenNlpScreeningEngine.load(model),
                    config);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(
                    "cannot read the screening model at " + path, e);
        }
    }

    /**
     * The strict read of {@link DocumentPlatformConfig#ENV_TAXONOMIES}: a
     * comma-separated list of taxonomy names, trimmed, blanks skipped,
     * duplicates collapsed in first-seen order. Absent means no taxonomy
     * follow and no document gate, exactly as before.
     */
    static List<String> taxonomiesFromEnvironment(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_TAXONOMIES, "").trim();
        if (value.isEmpty()) {
            return List.of();
        }
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
        for (String part : value.split(",")) {
            String name = part.trim();
            if (!name.isEmpty()) {
                names.add(name);
            }
        }
        if (names.isEmpty()) {
            throw new IllegalArgumentException(DocumentPlatformConfig.ENV_TAXONOMIES
                    + " is set but names no taxonomy; unset it or name at least one");
        }
        return List.copyOf(names);
    }

    /**
     * The strict read of the config lane's Kafka family. Absent bootstrap
     * servers means the registry source, exactly as before. Set, the family
     * must be complete (the serde's schema registry URL is required, because
     * the lane is verify-then-swap or nothing) and must not contradict the
     * registry source's URL: two sources named is a contradiction, refused
     * by name, never resolved by preference.
     */
    static ai.pipestream.proto.config.kafka.KafkaConfigSource.Config kafkaConfigFamily(
            Map<String, String> environment) {
        String bootstrap = environment.getOrDefault(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS, "").trim();
        if (bootstrap.isEmpty()) {
            return null;
        }
        String configUrl = environment
                .getOrDefault(DocumentPlatformConfig.ENV_CONFIG_URL, "").trim();
        if (!configUrl.isEmpty()) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS + " and "
                            + DocumentPlatformConfig.ENV_CONFIG_URL + " are both set:"
                            + " the config lane reads one source, so unset one");
        }
        String schemaRegistry = environment.getOrDefault(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL, "").trim();
        if (schemaRegistry.isEmpty()) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_CONFIG_KAFKA_SCHEMA_REGISTRY_URL
                            + " is required with "
                            + DocumentPlatformConfig.ENV_CONFIG_KAFKA_BOOTSTRAP_SERVERS
                            + ": the config serde verifies against the registry, and"
                            + " the lane is verify-then-swap or nothing");
        }
        String topic = environment.getOrDefault(
                DocumentPlatformConfig.ENV_CONFIG_KAFKA_TOPIC, "").trim();
        return new ai.pipestream.proto.config.kafka.KafkaConfigSource.Config(
                bootstrap,
                topic.isEmpty()
                        ? DocumentPlatformConfig.DEFAULT_CONFIG_KAFKA_TOPIC : topic,
                schemaRegistry);
    }

    /**
     * The strict read of
     * {@link DocumentPlatformConfig#ENV_CONFIG_REFRESH_SECONDS}: absent is
     * environment-only configuration ({@code 0}), anything set must be a
     * positive whole number of seconds.
     */
    static long configRefreshSeconds(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS, "").trim();
        if (value.isEmpty()) {
            return 0L;
        }
        long seconds;
        try {
            seconds = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                            + " must be a positive number of seconds, got '" + value + "'");
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_CONFIG_REFRESH_SECONDS
                            + " must be a positive number of seconds, got '" + value
                            + "'; unset it for environment-only configuration");
        }
        return seconds;
    }

    /** The strict read of {@link DocumentPlatformConfig#ENV_SEARCH_READ_ONLY}. */
    static boolean searchReadOnly(Map<String, String> environment) {
        String value = environment
                .getOrDefault(DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "").trim();
        return switch (value) {
            case "", "false" -> false;
            case "true" -> true;
            default -> throw new IllegalArgumentException(
                    DocumentPlatformConfig.ENV_SEARCH_READ_ONLY
                            + " must be true or false, got '" + value + "'");
        };
    }

    /** The S3 client the snapshot store rides, per the family's settings. */
    private static S3Client snapshotClient(SearchSnapshotConfig config) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(config.region()))
                .httpClientBuilder(UrlConnectionHttpClient.builder());
        if (!config.endpoint().isEmpty()) {
            builder = builder.endpointOverride(URI.create(config.endpoint()))
                    .forcePathStyle(true);
        }
        if (!config.accessKey().isEmpty()) {
            builder = builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.accessKey(), config.secretKey())));
        }
        return builder.build();
    }

    /**
     * Publishes the fleet document model from this build's own classpath
     * (the proto rides the parse-document jar) through the registry's wire
     * API. Idempotent: an unchanged schema re-registers as the same version.
     */
    private void publishDocumentModel() throws IOException {
        // The routing contract publishes beside the document model, so the
        // registry's config gate can gate parse-routing documents against
        // it (an unregistered type refuses).
        ProtoSourceSet sources = ProtoSourceSet.builder()
                .add(DOCUMENT_SUBJECT, classpathProto(DOCUMENT_SUBJECT), "platform")
                .add(VALIDATE_SUBJECT, classpathProto(VALIDATE_SUBJECT), "platform")
                .add(ROUTING_SUBJECT, classpathProto(ROUTING_SUBJECT), "platform")
                .add(ACCESS_POLICY_SUBJECT, classpathProto(ACCESS_POLICY_SUBJECT),
                        "platform")
                .build();
        try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(
                URI.create("http://127.0.0.1:" + registry.httpPort()), apiToken)) {
            publisher.publish(sources, PublishOptions.defaults()).throwIfFailed();
        } catch (Exception e) {
            throw new IllegalStateException("publishing the document model failed", e);
        }
        LOG.info("registry serves {} ({} subject(s) total)", DOCUMENT_SUBJECT,
                registry.store().subjects().size());
    }

    private static String classpathProto(String path) throws IOException {
        try (InputStream in = DocumentPlatform.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException(
                        "the contract is not on the classpath at " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
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
        int dims;
        // Borrowed only to learn the model's dimension; the derivation loads
        // its own provider, so this instance goes back.
        try (EmbeddingProvider provider =
                EmbeddingProviders.byId(Model2VecEmbeddingProvider.PROVIDER_ID)) {
            dims = provider.dimension();
        }
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        120, 16, 20, 200, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        Model2VecEmbeddingProvider.PROVIDER_ID, dims,
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
