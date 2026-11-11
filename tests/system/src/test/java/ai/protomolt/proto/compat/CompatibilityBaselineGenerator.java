package ai.protomolt.proto.compat;

import ai.protomolt.proto.acquire.jdbc.JdbcPull;
import ai.protomolt.proto.acquire.jdbc.JdbcPullAction;
import ai.protomolt.proto.acquire.pull.IntakeFeed;
import ai.protomolt.proto.acquire.s3.S3Pull;
import ai.protomolt.proto.acquire.s3.S3PullAction;
import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.StreamingAction;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
import ai.protomolt.proto.delegation.DelegationActions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.http.rest.ProtoRestMethod;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.intake.service.IntakeHttpServer;
import ai.protomolt.proto.mesh.cluster.ClusterActions;
import ai.protomolt.proto.mesh.cluster.ClusterValidation;
import ai.protomolt.proto.mesh.cluster.InMemoryClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.PersistentClusterDirectory;
import ai.protomolt.proto.mesh.cluster.storage.v1.ClusterEventLog;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.metric.service.MetricActions;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.service.PublishConfigAction;
import ai.protomolt.proto.registry.service.SchemaRegistryServer;
import ai.protomolt.proto.registry.service.SchemaRegistryServerConfig;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.service.UploadHttpServer;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.Drive;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.proto.schema.registry.git.RegistryFederation;
import ai.protomolt.proto.schema.registry.git.service.RegistryRemotesAction;
import ai.protomolt.proto.schema.registry.git.service.RegistrySyncAction;
import ai.protomolt.proto.search.service.DocumentLister;
import ai.protomolt.proto.search.service.ReplayAction;
import ai.protomolt.proto.search.service.SearchAction;
import ai.protomolt.proto.search.service.SubjectIndex;
import ai.protomolt.proto.search.service.SubjectSearch;
import ai.protomolt.proto.serve.ProtoMoltRestMount;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoCompilationException;
import ai.protomolt.proto.sources.ProtoSource;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthorTaskActions;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthorTaskOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringActions;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringEntryActions;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringEntryOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchInputActions;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchInputOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchStatusActions;
import ai.protomolt.proto.workflow.authoring.WorkflowLaunchStatusOperations;
import ai.protomolt.proto.workflow.authoring.WorkflowPreparationActions;
import ai.protomolt.proto.workflow.authoring.WorkflowPreparationOperations;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.Timestamp;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Captures the live five-surface compatibility bundle for every published module in
 * {@code bom/build.gradle} and either updates or verifies {@code compatibility/baseline/}.
 */
public final class CompatibilityBaselineGenerator {

    private static final String DEFAULT_RELEASE_VERSION = "0.1.0-SNAPSHOT";
    private static final String INVENTORY_CLASSPATH_FILE_PROPERTY =
            "protomolt.compat.inventoryClasspathFile";
    private static final String PROTOMOLT_PACKAGE_PREFIX = "ai/protomolt/";
    private static final String DEMO_SHOP_PROTO_PATH =
            "apps/serve/src/main/resources/ai/protomolt/proto/serve/demo_shop.proto";

    private static volatile DiscoveredInventory cachedInventory;

    private CompatibilityBaselineGenerator() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || (!"--update".equals(args[0]) && !"--check".equals(args[0]))) {
            throw new IllegalArgumentException(
                    "usage: CompatibilityBaselineGenerator <--update|--check> <baselineDir>");
        }
        String mode = args[0];
        Path baselineDir = Path.of(args[1]).toAbsolutePath().normalize();

        if ("--update".equals(mode)) {
            ReleaseBaselineManifest manifest = loadExistingManifestOrDefault(baselineDir);
            ReleaseBaselineBundle live = captureLiveBundle(manifest);
            boolean allowMajorBreak = Boolean.getBoolean("protomolt.compat.allowMajorReleaseBreak");
            ReleaseBaselineGate.advanceBaseline(baselineDir, live, allowMajorBreak);
        } else {
            ReleaseBaselineBundle baseline = ReleaseBaselineGate.load(baselineDir);
            ReleaseBaselineBundle live = captureLiveBundle(baseline.manifest());
            ReleaseBaselineReport report = ReleaseBaselineGate.evaluate(baseline, live);
            report.throwIfIncompatible();
        }
    }

    /**
     * Captures the live five-surface compatibility bundle from the current repository build.
     *
     * @param manifest manifest metadata and waivers to attach
     * @return the captured bundle
     */
    public static ReleaseBaselineBundle captureLiveBundle(ReleaseBaselineManifest manifest)
            throws ProtoCompilationException {
        DiscoveredInventory inventory = discoveredInventory();
        Path tempGitDir = createTempGitRegistryDir();
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator();
             DelegationBridge delegationBridge = new DelegationBridge(coordinator);
             GitSchemaRegistryStore gitStore = GitSchemaRegistryStore.builder()
                     .repositoryDir(tempGitDir)
                     .bareInit(true)
                     .build()) {
            ActionContext context = ActionContext.create();
            ActionCatalog fullCatalog = buildCompleteActionCatalog(
                    context, delegationBridge, gitStore);

            JavaApiSnapshot javaApi = captureJavaApi(inventory);
            ProtobufSurfaceSnapshot protobuf = captureProtobufSurface();
            StoredFormatSnapshot storedFormats = captureStoredFormats();
            RestRouteSnapshot restRoutes = captureRestRoutes(fullCatalog);
            ActionEnvelopeSnapshot actionEnvelopes = captureActionEnvelopes(fullCatalog);

            if (javaApi.classes().isEmpty()) {
                throw new IllegalStateException("Captured Java API snapshot must not be empty");
            }
            if (protobuf.descriptorSet().getFileCount() == 0) {
                throw new IllegalStateException("Captured Protobuf descriptor snapshot must not be empty");
            }
            if (storedFormats.formats().isEmpty()) {
                throw new IllegalStateException("Captured stored-format snapshot must not be empty");
            }
            if (restRoutes.routes().isEmpty()) {
                throw new IllegalStateException("Captured REST route snapshot must not be empty");
            }
            if (actionEnvelopes.actions().isEmpty()) {
                throw new IllegalStateException("Captured action-envelope snapshot must not be empty");
            }

            return new ReleaseBaselineBundle(
                    manifest,
                    javaApi,
                    protobuf,
                    storedFormats,
                    restRoutes,
                    actionEnvelopes);
        } finally {
            deleteRecursivelyQuietly(tempGitDir);
        }
    }

    private static ActionCatalog buildCompleteActionCatalog(
            ActionContext context,
            DelegationBridge delegationBridge,
            GitSchemaRegistryStore gitStore) {
        ActionCatalog catalog = ProtoMoltCatalog.full(context);

        DelegationActions.register(catalog, delegationBridge);
        for (ProtoAction metricAction : MetricActions.over(Map.of())) {
            catalog.register(metricAction);
        }

        ClusterDescriptor unsignedCluster = ClusterDescriptor.newBuilder()
                .setClusterId("compat-baseline")
                .setDisplayName("compat-baseline")
                .addProtocolRevisions(1)
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1L).build())
                .build();
        ClusterDescriptor clusterDescriptor = unsignedCluster.toBuilder()
                .setFingerprint(ClusterValidation.descriptorFingerprint(unsignedCluster))
                .build();
        PersistentClusterDirectory clusterDirectory = new PersistentClusterDirectory(
                clusterDescriptor,
                Clock.systemUTC(),
                new InMemoryClusterEventRepository());
        ClusterActions.register(catalog, clusterDirectory);

        catalog.register(new PublishConfigAction(stubInterface(ConfigDocumentStore.class)));
        RegistryFederation federation = RegistryFederation.over(gitStore);
        catalog.register(new RegistryRemotesAction(federation));
        catalog.register(new RegistrySyncAction(federation));

        ProtoAction submitWorkflow;
        try {
            submitWorkflow = catalog.get("submit-workflow");
        } catch (ActionException e) {
            throw new IllegalStateException("Missing built-in submit-workflow action", e);
        }
        catalog.register(new SearchAction(stubInterface(SubjectSearch.class)));
        catalog.register(new ReplayAction(
                stubInterface(DocumentLister.class),
                submitWorkflow,
                stubInterface(SubjectIndex.class)));

        IntakeFeed intakeFeed = stubInterface(IntakeFeed.class);
        catalog.register(new JdbcPullAction(
                new JdbcPull(stubInterface(JdbcPull.ConnectionFactory.class), intakeFeed)));
        catalog.register(new S3PullAction(
                new S3Pull(stubInterface(S3Client.class), intakeFeed)));

        WorkflowAuthoringActions.register(
                catalog, stubInterface(WorkflowAuthoringOperations.class));
        WorkflowAuthoringEntryActions.register(
                catalog, stubInterface(WorkflowAuthoringEntryOperations.class));
        WorkflowLaunchInputActions.register(
                catalog, stubInterface(WorkflowLaunchInputOperations.class));
        WorkflowLaunchStatusActions.register(
                catalog, stubInterface(WorkflowLaunchStatusOperations.class));
        WorkflowPreparationActions.register(
                catalog, stubInterface(WorkflowPreparationOperations.class));
        WorkflowAuthorTaskActions.register(
                catalog, stubInterface(WorkflowAuthorTaskOperations.class));

        return catalog;
    }

    @SuppressWarnings("unchecked")
    private static <T> T stubInterface(Class<T> iface) {
        return (T) Proxy.newProxyInstance(
                iface.getClassLoader(),
                new Class<?>[]{iface},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                        return null;
                    }
                    if ("toString".equals(method.getName()) && method.getParameterCount() == 0) {
                        return "CompatibilityBaselineStub[" + iface.getSimpleName() + "]";
                    }
                    if ("hashCode".equals(method.getName()) && method.getParameterCount() == 0) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(method.getName()) && method.getParameterCount() == 1) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(
                            "Baseline inspection stub invoked unexpectedly: "
                                    + iface.getName() + "#" + method.getName());
                });
    }

    private static ReleaseBaselineManifest loadExistingManifestOrDefault(Path baselineDir)
            throws IOException {
        Path manifestFile = baselineDir.resolve(ReleaseBaselineGate.MANIFEST_FILE);
        if (Files.exists(manifestFile)) {
            return ReleaseBaselineGate.load(baselineDir).manifest();
        }
        return ReleaseBaselineManifest.unreleasedInitial(DEFAULT_RELEASE_VERSION, List.of());
    }

    private static JavaApiSnapshot captureJavaApi(DiscoveredInventory inventory) {
        return JavaApiSnapshot.capture(inventory.javaApiClasses(), StabilityTier.STABLE);
    }

    private static ProtobufSurfaceSnapshot captureProtobufSurface() throws ProtoCompilationException {
        return ProtobufSurfaceSnapshot.fromFiles(compilePublishedDescriptors().fileDescriptors());
    }

    static CompiledProtos compilePublishedDescriptors() throws ProtoCompilationException {
        Path root;
        Map<String, Path> moduleDirs;
        try {
            root = repositoryRoot();
            moduleDirs = publishedModuleDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<ProtoSource> sources = new ArrayList<>();
        for (Path moduleDir : moduleDirs.values()) {
            collectProtoSources(root, moduleDir.resolve("src/main/proto"), sources);
            collectProtoSources(root, moduleDir.resolve("src/main/resources"), sources);
        }
        sources.sort(Comparator.comparing(ProtoSource::path));

        if (sources.isEmpty()) {
            throw new IllegalStateException(
                    "No published .proto sources discovered across BOM modules under " + root);
        }

        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.of(sources));
        if (compiled.fileDescriptors().isEmpty()) {
            throw new IllegalStateException(
                    "Compiled published descriptor set is empty for " + sources.size() + " sources");
        }
        return compiled;
    }

    private static void collectProtoSources(
            Path repoRoot,
            Path sourceRoot,
            List<ProtoSource> sink) {
        if (!Files.isDirectory(sourceRoot)) {
            return;
        }
        try (var paths = Files.walk(sourceRoot)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".proto"))
                    .sorted()
                    .forEach(path -> {
                        String repoRelative = relativePath(repoRoot, path);
                        if (DEMO_SHOP_PROTO_PATH.equals(repoRelative)) {
                            return;
                        }
                        String importPath = relativePath(sourceRoot, path);
                        try {
                            sink.add(new ProtoSource(
                                    importPath,
                                    Files.readString(path),
                                    path.toString()));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String relativePath(Path base, Path path) {
        return base.relativize(path).toString().replace(base.getFileSystem().getSeparator(), "/");
    }

    private static Path repositoryRoot() throws IOException {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null && !Files.exists(current.resolve("settings.gradle"))) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IOException("could not locate repository root from "
                    + Path.of("").toAbsolutePath());
        }
        return current;
    }

    static Map<String, Path> publishedModuleDirectories(Path repoRoot) throws IOException {
        String bomText = Files.readString(repoRoot.resolve("bom/build.gradle"));
        List<String> supportedModules = parseSupportedBomModules(bomText);
        if (supportedModules.isEmpty()) {
            throw new IllegalStateException("Parsed zero supportedModules from bom/build.gradle");
        }

        String settingsText = Files.readString(repoRoot.resolve("settings.gradle"));
        Map<String, String> projectDirByModule = parseSettingsProjectDirs(settingsText);

        Map<String, Path> resolved = new LinkedHashMap<>();
        for (String moduleName : supportedModules) {
            String relDir = projectDirByModule.getOrDefault(moduleName, moduleName);
            Path dir = repoRoot.resolve(relDir).toAbsolutePath().normalize();
            if (!Files.isDirectory(dir)) {
                throw new IllegalStateException("Published BOM module '" + moduleName
                        + "' points to missing projectDir: " + dir);
            }
            resolved.put(moduleName, dir);
        }
        return Map.copyOf(resolved);
    }

    private static List<String> parseSupportedBomModules(String bomText) {
        String anchor = "def supportedModules = [";
        int start = bomText.indexOf(anchor);
        if (start < 0) {
            throw new IllegalStateException("Could not find 'def supportedModules = [' in bom/build.gradle");
        }
        int end = bomText.indexOf(']', start + anchor.length());
        if (end < 0) {
            throw new IllegalStateException("Unterminated supportedModules list in bom/build.gradle");
        }
        String block = bomText.substring(start + anchor.length(), end);
        List<String> modules = new ArrayList<>();
        int cursor = 0;
        while (cursor < block.length()) {
            int openQuote = block.indexOf('\'', cursor);
            if (openQuote < 0) {
                break;
            }
            int closeQuote = block.indexOf('\'', openQuote + 1);
            if (closeQuote < 0) {
                throw new IllegalStateException("Unterminated quote in bom/build.gradle supportedModules");
            }
            String candidate = block.substring(openQuote + 1, closeQuote).trim();
            if (!candidate.isEmpty()) {
                modules.add(candidate);
            }
            cursor = closeQuote + 1;
        }
        return List.copyOf(modules);
    }

    private static Map<String, String> parseSettingsProjectDirs(String settingsText) {
        Map<String, String> dirs = new LinkedHashMap<>();
        String prefix = "project(':";
        String middle = "').projectDir = file('";
        int cursor = 0;
        while (cursor < settingsText.length()) {
            int pStart = settingsText.indexOf(prefix, cursor);
            if (pStart < 0) {
                break;
            }
            int nameStart = pStart + prefix.length();
            int middleIdx = settingsText.indexOf(middle, nameStart);
            if (middleIdx < 0) {
                break;
            }
            String moduleName = settingsText.substring(nameStart, middleIdx);
            int dirStart = middleIdx + middle.length();
            int dirEnd = settingsText.indexOf("')", dirStart);
            if (dirEnd < 0) {
                break;
            }
            String relDir = settingsText.substring(dirStart, dirEnd);
            if (moduleName.indexOf('\n') < 0 && relDir.indexOf('\n') < 0) {
                dirs.put(moduleName, relDir);
            }
            cursor = dirEnd + 2;
        }
        return dirs;
    }

    private static StoredFormatSnapshot captureStoredFormats() {
        return new StoredFormatSnapshot(List.of(
                StoredFormatEntry.fromDescriptor(
                        "repo.document.v1",
                        Document.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.FULL,
                        true),
                StoredFormatEntry.fromDescriptor(
                        "repo.drive.v1",
                        Drive.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.manifest.document.v1",
                        ai.protomolt.proto.repo.v1.DocumentManifest.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.manifest.accession.v2",
                        Accession.getDescriptor(),
                        2,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.manifest.document.v2",
                        DocumentManifest.getDescriptor(),
                        2,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.catalog.stored-manifest.v2",
                        StoredManifest.getDescriptor(),
                        2,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        true),
                StoredFormatEntry.fromDescriptor(
                        "repo.catalog.entry.v2",
                        CatalogEntry.getDescriptor(),
                        2,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.archive.v1",
                        Archive.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.archive.version-manifest.v1",
                        VersionManifest.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "repo.archive.entry-info.v1",
                        EntryInfo.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "authz.access-policy.v1",
                        AccessPolicy.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "authz.account-bindings.v1",
                        AccountBindings.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "receipt.signed-work-record.v1",
                        SignedWorkRecord.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "receipt.trust-snapshot.v1",
                        TrustSnapshot.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "grpc.service-profile.v1",
                        ServiceProfile.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "mesh.cluster.event-log.v1",
                        ClusterEventLog.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "delegation.encrypted-repository-state.v1",
                        EncryptedRepositoryState.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "workflow.definition.v1",
                        Workflow.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "workflow.versioned-workflow.v1",
                        VersionedWorkflow.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "workflow.run-evidence.v1",
                        RunEvidence.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false),
                StoredFormatEntry.fromDescriptor(
                        "workflow.authoring.preparation-record.v1",
                        WorkflowPreparationRecord.getDescriptor(),
                        1,
                        StabilityTier.STABLE,
                        CompatibilityMode.BACKWARD,
                        false)));
    }

    private static RestRouteSnapshot captureRestRoutes(ActionCatalog fullCatalog) {
        ProtoRestMethodRegistry restRegistry = new ProtoRestMethodRegistry();
        ProtoMoltRestMount.registerAllContributed(restRegistry, fullCatalog);

        List<RestRouteEntry> entries = new ArrayList<>();
        for (ProtoRestMethod method : restRegistry.all()) {
            String requestType = method.methodDescriptor() != null
                    ? method.methodDescriptor().getInputType().getFullName()
                    : "";
            String responseType = method.methodDescriptor() != null
                    ? method.methodDescriptor().getOutputType().getFullName()
                    : "";
            StabilityTier tier = method.methodDescriptor() != null
                    ? DescriptorMetadata.stability(method.methodDescriptor(), method.stability())
                    : method.stability();
            entries.add(new RestRouteEntry(
                    "grpc-json:" + method.routeKey(),
                    "/grpc-json/" + method.routeKey(),
                    method.allowedHttpVerbs(),
                    List.of(),
                    List.of(),
                    "application/json",
                    "application/json",
                    requestType,
                    responseType,
                    List.of(200, 400, 401, 403),
                    method.apiToken().isPresent(),
                    tier));
        }

        for (SchemaRegistryServer.RouteContract route :
                SchemaRegistryServer.routeContracts(SchemaRegistryServerConfig.defaults(), true)) {
            entries.add(new RestRouteEntry(
                    route.routeId(),
                    route.pathTemplate(),
                    route.httpMethods(),
                    route.pathParameters(),
                    route.requiredQueryParameters(),
                    route.requestMediaType(),
                    route.responseMediaType(),
                    route.requestType(),
                    route.responseType(),
                    route.successStatusCodes(),
                    route.authRequired(),
                    route.stability()));
        }

        for (UploadHttpServer.RouteContract route : UploadHttpServer.routeContracts()) {
            entries.add(new RestRouteEntry(
                    route.routeId(),
                    route.pathTemplate(),
                    route.httpMethods(),
                    route.pathParameters(),
                    route.requiredQueryParameters(),
                    route.requestMediaType(),
                    route.responseMediaType(),
                    route.requestType(),
                    route.responseType(),
                    route.successStatusCodes(),
                    route.authRequired(),
                    route.stability()));
        }

        for (IntakeHttpServer.RouteContract route : IntakeHttpServer.routeContracts()) {
            entries.add(new RestRouteEntry(
                    route.routeId(),
                    route.pathTemplate(),
                    route.httpMethods(),
                    route.pathParameters(),
                    route.requiredQueryParameters(),
                    route.requestMediaType(),
                    route.responseMediaType(),
                    route.requestType(),
                    route.responseType(),
                    route.successStatusCodes(),
                    route.authRequired(),
                    route.stability()));
        }

        return new RestRouteSnapshot(entries);
    }

    private static ActionEnvelopeSnapshot captureActionEnvelopes(ActionCatalog fullCatalog) {
        List<ActionEnvelopeEntry> entries = new ArrayList<>();
        for (String name : fullCatalog.names()) {
            try {
                ProtoAction action = fullCatalog.get(name);
                entries.add(ActionEnvelopeEntry.fromDescriptors(
                        action.name(),
                        action.requiredScope(),
                        action instanceof StreamingAction,
                        action.requestType(),
                        action.responseType(),
                        action.inputSchema(),
                        action.stability()));
            } catch (ActionException e) {
                throw new IllegalStateException("Failed to load registered action: " + name, e);
            }
        }
        return new ActionEnvelopeSnapshot(entries);
    }

    private static DiscoveredInventory discoveredInventory() {
        DiscoveredInventory existing = cachedInventory;
        if (existing != null) {
            return existing;
        }
        synchronized (CompatibilityBaselineGenerator.class) {
            if (cachedInventory == null) {
                cachedInventory = scanPublishedInventory();
            }
            return cachedInventory;
        }
    }

    private static DiscoveredInventory scanPublishedInventory() {
        Path repoRoot;
        Map<String, Path> publishedModules;
        try {
            repoRoot = repositoryRoot();
            publishedModules = publishedModuleDirectories(repoRoot);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<Path> inventoryEntries = resolveInventoryClasspathEntries();
        URLClassLoader inventoryLoader = createInventoryClassLoader(inventoryEntries);

        List<Path> publishedModuleOutputs = matchPublishedModuleOutputs(
                publishedModules, inventoryEntries);

        Map<String, Class<?>> javaClassesByName = new LinkedHashMap<>();
        for (Path entry : publishedModuleOutputs) {
            List<String> classResources = collectProtomoltClassResources(entry);
            for (String resourcePath : classResources) {
                inspectClassResource(resourcePath, inventoryLoader, javaClassesByName);
            }
        }

        List<Class<?>> sortedClasses = new ArrayList<>(javaClassesByName.values());
        sortedClasses.sort(Comparator.comparing(Class::getName));
        if (sortedClasses.isEmpty()) {
            throw new IllegalStateException("Discovered zero published Java API classes");
        }
        return new DiscoveredInventory(inventoryLoader, List.copyOf(sortedClasses));
    }

    private static List<Path> matchPublishedModuleOutputs(
            Map<String, Path> publishedModules,
            List<Path> classpathEntries) {
        List<Path> matchedOutputs = new ArrayList<>();
        List<String> missingModules = new ArrayList<>();

        for (Map.Entry<String, Path> module : publishedModules.entrySet()) {
            String moduleName = module.getKey();
            Path moduleDir = module.getValue();
            boolean hasMainSources = Files.isDirectory(moduleDir.resolve("src/main/java"))
                    || Files.isDirectory(moduleDir.resolve("src/main/proto"));

            String normalizedModuleDir = moduleDir.toString().replace('\\', '/');
            String buildLibsPrefix = normalizedModuleDir + "/build/libs/";
            String mainClassesDir = normalizedModuleDir + "/build/classes/java/main";

            boolean foundForModule = false;
            for (Path cpEntry : classpathEntries) {
                String normalizedEntry = cpEntry.toString().replace('\\', '/');
                if (normalizedEntry.equals(mainClassesDir)) {
                    matchedOutputs.add(cpEntry);
                    foundForModule = true;
                } else if (normalizedEntry.startsWith(buildLibsPrefix)
                        && normalizedEntry.endsWith(".jar")
                        && !isClassifierJar(cpEntry.getFileName().toString())) {
                    matchedOutputs.add(cpEntry);
                    foundForModule = true;
                }
            }
            if (hasMainSources && !foundForModule) {
                missingModules.add(moduleName + " (" + moduleDir + ")");
            }
        }

        if (!missingModules.isEmpty()) {
            throw new IllegalStateException(
                    "Published BOM modules missing from compatibility inventory classpath: "
                            + missingModules);
        }
        return List.copyOf(matchedOutputs);
    }

    private static boolean isClassifierJar(String fileName) {
        return fileName.endsWith("-sources.jar")
                || fileName.endsWith("-javadoc.jar")
                || fileName.endsWith("-tests.jar")
                || fileName.endsWith("-test-fixtures.jar");
    }

    private static List<Path> resolveInventoryClasspathEntries() {
        String classpathFile = System.getProperty(INVENTORY_CLASSPATH_FILE_PROPERTY);
        if (classpathFile != null && !classpathFile.isBlank()) {
            Path file = Path.of(classpathFile.trim()).toAbsolutePath().normalize();
            if (!Files.isRegularFile(file)) {
                throw new IllegalStateException("Compatibility inventory classpath file does not exist: " + file);
            }
            try {
                List<Path> entries = new ArrayList<>();
                for (String line : Files.readAllLines(file)) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        entries.add(Path.of(trimmed).toAbsolutePath().normalize());
                    }
                }
                if (entries.isEmpty()) {
                    throw new IllegalStateException(
                            "Compatibility inventory classpath file is empty: " + file);
                }
                return List.copyOf(entries);
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to read compatibility inventory classpath file: " + file, e);
            }
        }
        return systemClasspathEntries();
    }

    private static URLClassLoader createInventoryClassLoader(List<Path> entries) {
        URL[] urls = new URL[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            try {
                urls[i] = entries.get(i).toUri().toURL();
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Invalid classpath entry URI: " + entries.get(i), e);
            }
        }
        return new URLClassLoader(urls, CompatibilityBaselineGenerator.class.getClassLoader());
    }

    static void inspectClassResource(
            String resourcePath,
            ClassLoader loader,
            Map<String, Class<?>> javaClassesByName) {
        if (!resourcePath.startsWith(PROTOMOLT_PACKAGE_PREFIX)
                || !resourcePath.endsWith(".class")) {
            return;
        }
        String simpleFile = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        if ("package-info.class".equals(simpleFile)
                || "module-info.class".equals(simpleFile)) {
            return;
        }

        String className = resourcePath
                .substring(0, resourcePath.length() - ".class".length())
                .replace('/', '.');
        Class<?> clazz;
        try {
            clazz = Class.forName(className, false, loader);
        } catch (ClassNotFoundException | LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to load published class resource: " + resourcePath, e);
        }

        boolean isProtocOuterHolder = declaresStaticFileDescriptor(clazz, resourcePath);
        if (!isExportedType(clazz, resourcePath)
                || isGeneratedProtobufClass(clazz, isProtocOuterHolder, resourcePath)) {
            return;
        }

        verifyMemberLinkage(clazz, resourcePath);
        javaClassesByName.put(clazz.getName(), clazz);
    }

    private static boolean isExportedType(Class<?> clazz, String resourcePath) {
        try {
            for (Class<?> current = clazz; current != null; current = current.getEnclosingClass()) {
                if (current.isSynthetic()
                        || current.isAnonymousClass()
                        || current.isLocalClass()) {
                    return false;
                }
                int mods = current.getModifiers();
                if (!Modifier.isPublic(mods) && !Modifier.isProtected(mods)) {
                    return false;
                }
            }
            return true;
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to inspect enclosing hierarchy of published class resource: " + resourcePath, e);
        }
    }

    private static boolean isGeneratedProtobufClass(
            Class<?> clazz,
            boolean isProtocOuterHolder,
            String resourcePath) {
        try {
            if (isProtocOuterHolder) {
                return true;
            }
            for (Class<?> current = clazz; current != null; current = current.getEnclosingClass()) {
                if (MessageOrBuilder.class.isAssignableFrom(current)
                        || ProtocolMessageEnum.class.isAssignableFrom(current)
                        || declaresStaticFileDescriptor(current, resourcePath)) {
                    return true;
                }
            }
            return false;
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to inspect protobuf hierarchy of published class resource: " + resourcePath, e);
        }
    }

    private static boolean declaresStaticFileDescriptor(Class<?> type, String resourcePath) {
        try {
            Method m = type.getDeclaredMethod("getDescriptor");
            return Modifier.isPublic(m.getModifiers())
                    && Modifier.isStatic(m.getModifiers())
                    && m.getParameterCount() == 0
                    && m.getReturnType() == FileDescriptor.class;
        } catch (NoSuchMethodException expected) {
            return false;
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to inspect getDescriptor() on published class resource: " + resourcePath, e);
        }
    }

    private static void verifyMemberLinkage(Class<?> clazz, String resourcePath) {
        try {
            clazz.getDeclaredConstructors();
            clazz.getDeclaredMethods();
            clazz.getDeclaredFields();
            clazz.getDeclaredClasses();
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to reflect members of published class resource: " + resourcePath, e);
        }
    }

    private static List<Path> systemClasspathEntries() {
        String classpath = System.getProperty("java.class.path", "");
        List<Path> paths = new ArrayList<>();
        char sep = File.pathSeparatorChar;
        int start = 0;
        while (start <= classpath.length()) {
            int idx = classpath.indexOf(sep, start);
            String token = idx < 0
                    ? classpath.substring(start)
                    : classpath.substring(start, idx);
            if (!token.isBlank()) {
                paths.add(Path.of(token).toAbsolutePath().normalize());
            }
            if (idx < 0) {
                break;
            }
            start = idx + 1;
        }
        return paths;
    }

    private static List<String> collectProtomoltClassResources(Path entry) {
        List<String> resources = new ArrayList<>();
        if (Files.isDirectory(entry)) {
            Path root = entry.resolve("ai/protomolt");
            if (!Files.isDirectory(root)) {
                return resources;
            }
            try {
                Files.walkFileTree(root, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        String rel = entry.relativize(file).toString().replace('\\', '/');
                        if (rel.endsWith(".class")) {
                            resources.add(rel);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new IllegalStateException("Failed to scan classpath directory: " + entry, e);
            }
        } else if (Files.isRegularFile(entry) && entry.getFileName().toString().endsWith(".jar")) {
            try (JarFile jar = new JarFile(entry.toFile())) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry jarEntry = entries.nextElement();
                    String name = jarEntry.getName();
                    if (!jarEntry.isDirectory()
                            && name.startsWith(PROTOMOLT_PACKAGE_PREFIX)
                            && name.endsWith(".class")) {
                        resources.add(name);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to scan classpath JAR: " + entry, e);
            }
        }
        resources.sort(Comparator.naturalOrder());
        return resources;
    }

    private static Path createTempGitRegistryDir() {
        try {
            return Files.createTempDirectory("protomolt-compat-registry-");
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create temporary git registry directory", e);
        }
    }

    private static void deleteRecursivelyQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path d, IOException exc)
                        throws IOException {
                    Files.deleteIfExists(d);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
            // Best-effort cleanup of temporary directory.
        }
    }

    @SuppressWarnings("unused")
    private record DiscoveredInventory(
            URLClassLoader classLoader,
            List<Class<?>> javaApiClasses) {
    }
}
