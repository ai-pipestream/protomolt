package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.jdbc.JdbcCallerResolver;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventRelay;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.mesh.cluster.ClusterActions;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.server.ProtoToolsServerConfig;
import ai.protomolt.proto.server.jdk.JdkProtoRestServer;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Package-private startup wiring and banner logging for {@link ProtoMoltServe}.
 */
final class ProtoMoltServeBootstrap {

    private ProtoMoltServeBootstrap() {
    }

    static ProtoMoltServe start(ProtoMoltServe.Options options, CorrectionStarter starter) {
        if (starter != null && (options.apiToken() == null || options.apiToken().isBlank()
                || options.taskConsole() == null)) {
            throw new IllegalArgumentException(
                    "the correction starter requires API and console authentication");
        }
        ActionContext context = ActionContext.create();
        GitSchemaRegistryStore store = null;
        WorkflowRunDatabase jobsDatabase = null;
        WorkflowRunWorker jobsWorker = null;
        WorkflowRunEventRelay jobsRelay = null;
        ConfigLaneMount.TrustSource trust = null;
        OutboundChannelPolicy outboundPolicy = options.outboundPolicy();
        ChannelFactory.Closeable outboundChannels = null;
        try {
            ProfileCredentialResolver credentials =
                    ProfileTlsMount.resolve(options.profileTlsConfig(), starter);
            outboundChannels = ChannelFactory.pooled(outboundPolicy);
            InferenceEngines inference = InferenceMount.engines(options.inferenceModels());
            StructuredGenerator structured = inference == null
                    ? null : new StructuredGenerator(inference, context.registry());
            store = RegistryStoreMount.openStore(options.registryGit(), options.demo());
            WorkflowRepository workflows = store == null
                    ? null : RegistryStoreMount.workflowRepository(store);
            WorkflowVersionRepository workflowVersions = store == null
                    ? null : RegistryStoreMount.workflowVersions(store);
            ServiceProfileRepository serviceProfiles =
                    WorkspaceMount.serviceProfiles(options.serviceWorkspace());
            WorkspaceMount.WorkflowRepositories workspace =
                    WorkspaceMount.workflowRepositories(options.workflowWorkspace(), options.demo());
            ArtifactRepository artifacts = workspace.artifacts();
            RunEvidenceRepository runEvidence = workspace.runEvidence();
            trust = ConfigLaneMount.trustSource(options.configLane(), options.apiToken());

            if (options.jobs() != null) {
                JobsFleetMount.Prepared jobs = JobsFleetMount.prepare(
                        options.jobs(), context, workflows, structured, outboundPolicy,
                        outboundChannels, serviceProfiles, credentials);
                jobsDatabase = jobs.database();
                jobsWorker = jobs.worker();
                jobsRelay = jobs.relay();
                WorkflowAuthoringMount.Prepared authoring = options.workflowAuthoring() == null
                        ? null : WorkflowAuthoringMount.prepare(options.workflowAuthoring(),
                                artifacts, runEvidence, workflowVersions, jobs.store(), context,
                                jobs.runner(), jobs.config().maxAttemptsDefault(), trust);
                WorkflowPreparationMount.Prepared preparation = options.workflowPreparation() == null
                        ? null : WorkflowPreparationMount.prepare(
                                options.workflowPreparation(), authoring);
                ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                        workflows, jobs.store(), jobs.config().maxAttemptsDefault(),
                        inference, serviceProfiles, outboundPolicy, outboundChannels,
                        artifacts, runEvidence, workflowVersions, store, trust, credentials);
                return startWithJobsCatalog(options, context, catalog, store, serviceProfiles,
                        jobs, starter, authoring, preparation, trust, outboundChannels);
            }
            ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                    workflows, null, 0, inference, serviceProfiles, outboundPolicy,
                    outboundChannels, artifacts, runEvidence, workflowVersions, store, trust,
                    credentials);
            return startWithJobsCatalog(options, context, catalog, store, serviceProfiles,
                    null, starter, null, null, trust, outboundChannels);
        } catch (RuntimeException e) {
            ProtoMoltServe.closeQuietly(outboundChannels);
            ProtoMoltServe.closeQuietly(trust);
            ProtoMoltServe.closeQuietly(store);
            ProtoMoltServe.closeQuietly(jobsWorker);
            ProtoMoltServe.closeQuietly(jobsRelay);
            ProtoMoltServe.closeQuietly(jobsDatabase);
            throw e;
        }
    }

    private static ProtoMoltServe startWithJobsCatalog(
            ProtoMoltServe.Options options, ActionContext context, ActionCatalog catalog,
            GitSchemaRegistryStore store, ServiceProfileRepository serviceProfiles,
            JobsFleetMount.Prepared jobs, CorrectionStarter starter,
            WorkflowAuthoringMount.Prepared authoring,
            WorkflowPreparationMount.Prepared preparation,
            ConfigLaneMount.TrustSource trust, ChannelFactory.Closeable outboundChannels) {
        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        McpHttpHandler mcpHandler = null;
        SchemaRegistryServer registry = null;
        DelegationRuntime delegation = null;
        MeshClusterRuntime meshCluster = null;
        JdbcCallerResolver jdbcCallers = null;
        try {
            if (starter != null) {
                starter.install(catalog, serviceProfiles);
            }
            if (options.demo()) {
                DemoSchemas.seed(context.registry(), store);
            }
            delegation = DelegationMount.open(options.delegation(), authoring);
            DelegationBridge bridge = delegation.bridge();
            Map<String, Map<String, String>> authorTaskBindings =
                    DelegationMount.registerAll(catalog, delegation, authoring, preparation);
            meshCluster = MeshClusterRuntime.open(options.meshCluster(), options.delegation());
            if (meshCluster != null) {
                ClusterActions.register(catalog, meshCluster.directory());
            }
            AccessPolicyMount.Callers resolved =
                    AccessPolicyMount.resolve(options.accessPolicy(), options.identityStores());
            CallerResolver callers = resolved.callers();
            jdbcCallers = resolved.jdbcCallers();

            List<ServiceDescriptor> contributed = GrpcMount.contributedServices(
                    meshCluster != null, authoring != null, preparation != null);
            grpc = GrpcMount.start(options, catalog, callers, contributed, authorTaskBindings,
                    authoring != null);
            if (options.demo() && store != null) {
                DemoSchemas.seedWorkflow(store, grpc.port());
            }
            startJobsFleet(jobs);
            int registryPort = -1;
            if (store != null) {
                registry = RegistryStoreMount.server(options.host(), options.registryPort(),
                        options.apiToken(), store, catalog, callers);
                registryPort = registry.start();
            }
            ProtoRestGateway gateway = RestGatewayMount.mount(context, catalog,
                    options.apiToken(), callers, contributed, authorTaskBindings);
            String version = serveVersion();
            mcpHandler = McpMount.mount(catalog, store, serviceProfiles, bridge,
                    options.apiToken(), callers, version);
            int[] selfPort = {-1};
            http = buildHttpServer(options, gateway, mcpHandler, version, callers, bridge,
                    starter, catalog, authoring, registryPort, selfPort);
            int httpPort = http.start();
            selfPort[0] = httpPort;
            return new ProtoMoltServe(grpc, http, mcpHandler, httpPort, store, registry,
                    registryPort, jobs == null ? null : jobs.database(),
                    jobs == null ? null : jobs.worker(), jobs == null ? null : jobs.relay(),
                    delegation, meshCluster, jdbcCallers, trust, outboundChannels);
        } catch (RuntimeException e) {
            ProtoMoltServe.closeQuietly(jdbcCallers);
            ProtoMoltServe.closeQuietly(meshCluster);
            ProtoMoltServe.closeQuietly(delegation);
            ProtoMoltServe.closeQuietly(mcpHandler);
            if (registry != null) {
                registry.close();
            }
            if (http != null) {
                http.close();
            }
            if (grpc != null) {
                grpc.close();
            }
            throw e;
        }
    }

    private static JdkProtoRestServer buildHttpServer(
            ProtoMoltServe.Options options, ProtoRestGateway gateway, McpHttpHandler mcpHandler,
            String version, CallerResolver callers, DelegationBridge bridge,
            CorrectionStarter starter, ActionCatalog catalog,
            WorkflowAuthoringMount.Prepared authoring, int boundRegistryPort, int[] selfPort) {
        ProtoToolsServerConfig config = ProtoToolsServerConfig.defaults()
                .withHost(options.host())
                .withPort(options.httpPort());
        JdkProtoRestServer http = new JdkProtoRestServer(config, gateway,
                new ProtoOpenApiGenerator("ProtoMolt", version, "/", config.restPathPrefix()))
                .withContext("/docs", new SwaggerUiHandler("/docs", config.openApiPath()))
                .withContext("/mcp", mcpHandler);
        ConsoleMount.mount(http, options.apiToken(), options.taskConsole(), callers, bridge,
                starter, catalog, authoring, () -> boundRegistryPort, () -> selfPort[0]);
        return http;
    }

    private static void startJobsFleet(JobsFleetMount.Prepared jobs) {
        if (jobs != null) {
            jobs.worker().start();
            if (jobs.relay() != null) {
                jobs.relay().start();
            }
        }
    }

    private static String serveVersion() {
        String version = ProtoMoltServe.class.getPackage().getImplementationVersion();
        return version != null ? version : "dev";
    }

    static void logStartupBanner(Logger log, ProtoMoltServe.Options options, ProtoMoltServe serve) {
        log.info("""
                ProtoMolt serving:
                  gRPC  {}:{}   ai.protomolt.proto.grpc.service.v1.ProtoMoltService (reflection on)
                  REST  http://{}:{}/grpc-json/ProtoMoltService/{Method}
                  API   http://{}:{}/openapi.json
                  Docs  http://{}:{}/docs
                  MCP   http://{}:{}/mcp   (streamable HTTP)
                """, options.host(), serve.grpcPort(), options.host(), serve.httpPort(),
                options.host(), serve.httpPort(), options.host(), serve.httpPort(),
                options.host(), serve.httpPort());
        if (serve.registryPort() >= 0) {
            log.info("  Reg   http://{}:{} (Confluent protocol, git-backed)",
                    options.host(), serve.registryPort());
        }
        if (options.jobs() != null) {
            log.info("  Jobs  {} (verbs: submit-workflow, get-job, list-jobs, complete-step{})",
                    options.jobs().jdbcUrl(),
                    options.jobs().kafkaBootstrap() != null
                            ? "; Kafka " + options.jobs().kafkaBootstrap()
                            : ", no broker - verb submission only");
        }
        if (options.apiToken() == null || options.taskConsole() != null) {
            log.info("  UI    http://{}:{}/console", options.host(), serve.httpPort());
        }
        logAuthAndDemoBanner(log, options, serve);
    }

    private static void logAuthAndDemoBanner(Logger log, ProtoMoltServe.Options options,
                                             ProtoMoltServe serve) {
        if (options.apiToken() != null) {
            log.info("  Auth  api_token required on gRPC, REST, MCP{}"
                            + " (health, OpenAPI, and docs stay open)",
                    serve.registryPort() >= 0 ? ", and the registry" : "");
            log.info(options.taskConsole() == null
                    ? "  UI    console disabled in token mode"
                    : "  UI    task console uses a scoped browser session; "
                            + "registry and serve proxies stay disabled");
            if (options.accessPolicy() != null) {
                log.info("  Auth  access policy mounted from {}; named principals are "
                                + "scope-checked, the operator token keeps every scope",
                        options.accessPolicy());
            }
            if (options.identityStores() != null) {
                log.info("  Auth  external caller stores mounted:{}{}",
                        options.identityStores().oidcIntrospection() != null
                                ? " OIDC introspection at "
                                        + options.identityStores().oidcIntrospection()
                                : "",
                        options.identityStores().callerStore() != null
                                ? " JDBC caller store" : "");
            }
        }
        if (options.demo()) {
            log.info("""
                    Demo schema seeded: subject {} (types demo.shop.v1.Order, Customer, ...)
                      Try: curl -s -H 'content-type: application/json' \\
                             -d '{"schema": {"type": "demo.shop.v1.Order"}}' \\
                             http://{}:{}/grpc-json/ProtoMoltService/RenderJsonSchema
                      Or open http://{}:{}/docs and call ValidateMessage on a demo.shop.v1.Order.
                    """, DemoSchemas.SHOP_SUBJECT,
                    options.host(), serve.httpPort(), options.host(), serve.httpPort());
        }
    }
}
