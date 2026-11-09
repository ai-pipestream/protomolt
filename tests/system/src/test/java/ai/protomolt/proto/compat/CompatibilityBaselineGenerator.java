package ai.protomolt.proto.compat;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.actions.StreamEmitter;
import ai.protomolt.proto.actions.StreamingAction;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
import ai.protomolt.proto.delegation.DelegationActions;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.ApiStabilityResolver;
import ai.protomolt.proto.descriptors.ClasspathDescriptorLoader;
import ai.protomolt.proto.descriptors.DescriptorCatalog;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorLoader;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.protomolt.proto.helpers.AnyHandler;
import ai.protomolt.proto.helpers.BinaryProtobufIdentifierValidator;
import ai.protomolt.proto.helpers.MappingHelper;
import ai.protomolt.proto.helpers.MessageDiff;
import ai.protomolt.proto.helpers.PayloadCodec;
import ai.protomolt.proto.helpers.ProtoFqnConflictDetector;
import ai.protomolt.proto.helpers.ProtoSchemaValidationException;
import ai.protomolt.proto.helpers.TypeConverter;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoApiToken;
import ai.protomolt.proto.http.rest.ProtoRestAnnotationRegistrar;
import ai.protomolt.proto.http.rest.ProtoRestException;
import ai.protomolt.proto.http.rest.ProtoRestExposed;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.http.rest.ProtoRestMethod;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.mesh.cluster.storage.v1.ClusterEventLog;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.meta.StabilityProto;
import ai.protomolt.proto.metric.service.MetricActions;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.registry.CompatibilityModes;
import ai.protomolt.proto.registry.CompatibilityWriteGate;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaContents;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.StoredSchema;
import ai.protomolt.proto.registry.StoredSchemaSources;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.Drive;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.serve.ProtoMoltRestMount;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoCompilationException;
import ai.protomolt.proto.sources.ProtoImports;
import ai.protomolt.proto.sources.ProtoSource;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.RuleEvaluationException;
import ai.protomolt.proto.validate.ValidateProto;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.ProtocolMessageEnum;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Captures the live five-surface compatibility bundle for the repository and either
 * updates or verifies {@code compatibility/baseline/}.
 */
public final class CompatibilityBaselineGenerator {

    private static final String DEFAULT_RELEASE_VERSION = "0.1.0-SNAPSHOT";
    private static final String PROTOMOLT_CLASS_PREFIX = "ai/protomolt/proto/";
    private static final String SAMPLES_CLASS_PREFIX = "ai/protomolt/proto/samples/";
    private static final String CORRECTION_CLASS_PREFIX = "ai/protomolt/proto/correction/";

    private static final List<Class<?>> CORE_JAVA_API_SEEDS = List.of(
            // :protomolt-descriptors
            DescriptorRegistry.class,
            DescriptorCatalog.class,
            DescriptorLoader.class,
            ClasspathDescriptorLoader.class,
            GoogleDescriptorLoader.class,
            DescriptorFingerprints.class,
            MessageFingerprints.class,
            StabilityTier.class,
            ApiStability.class,
            ApiStabilityResolver.class,
            // :protomolt-helpers
            AnyHandler.class,
            TypeConverter.class,
            PayloadCodec.class,
            MappingHelper.class,
            MessageDiff.class,
            ProtoFqnConflictDetector.class,
            BinaryProtobufIdentifierValidator.class,
            ProtoSchemaValidationException.class,
            // :protomolt-sources
            ProtoSource.class,
            ProtoSourceSet.class,
            ProtoImports.class,
            ProtoSourceCompiler.class,
            CompiledProtos.class,
            ProtoCompilationException.class,
            // :protomolt-compat
            SchemaDiff.class,
            SchemaChange.class,
            ChangeRules.class,
            Impact.class,
            CompatibilityMode.class,
            CompatibilityChecker.class,
            CompatibilityResult.class,
            CompatibilityException.class,
            IncompatibleSchemaException.class,
            CompatibilitySurface.class,
            SurfaceChange.class,
            CompatibilityWaiver.class,
            IncompatibleSurfaceException.class,
            JavaApiSnapshot.class,
            JavaApiCompatibilityChecker.class,
            ProtobufSurfaceSnapshot.class,
            ProtobufSurfaceCompatibilityChecker.class,
            StoredFormatSnapshot.class,
            StoredFormatCompatibilityChecker.class,
            RestRouteSnapshot.class,
            RestRouteCompatibilityChecker.class,
            ActionEnvelopeSnapshot.class,
            ActionEnvelopeCompatibilityChecker.class,
            ReleaseBaselineStatus.class,
            ReleaseBaselineManifest.class,
            ReleaseBaselineBundle.class,
            ReleaseBaselineReport.class,
            ReleaseBaselineGate.class,
            // :protomolt-protobuf-metadata
            DescriptorMetadata.class,
            SensitivityMasker.class,
            // :protomolt-protobuf-validation
            ProtoValidator.class,
            ValidationResult.class,
            RuleCompilationException.class,
            RuleEvaluationException.class,
            // :protomolt-actions
            ProtoAction.class,
            StreamingAction.class,
            StreamEmitter.class,
            ActionCatalog.class,
            ActionContext.class,
            ai.protomolt.proto.actions.ActionException.class,
            Caller.class,
            Scopes.class,
            SchemaResolver.class,
            // :protomolt-http-rest & :protomolt-http-openapi
            ProtoRestGateway.class,
            ProtoRestMethod.class,
            ProtoRestMethodRegistry.class,
            ProtoRestExposed.class,
            ProtoApiToken.class,
            ApiTokenRequirement.class,
            ProtoRestAnnotationRegistrar.class,
            ProtoRestException.class,
            ProtoOpenApiGenerator.class,
            // :protomolt-registry
            SchemaRegistryStore.class,
            InMemorySchemaRegistryStore.class,
            CompatibilityWriteGate.class,
            CompatibilityModes.class,
            StoredSchema.class,
            SchemaReference.class,
            SchemaContents.class,
            StoredSchemaSources.class,
            // :protomolt-repo-admission
            BuiltinDocumentSchema.class,
            DocumentSchemaAdmission.class
    );

    private static volatile DiscoveredInventory cachedInventory;

    private CompatibilityBaselineGenerator() {
    }

    public static void main(String[] args) throws IOException {
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
    public static ReleaseBaselineBundle captureLiveBundle(ReleaseBaselineManifest manifest) {
        ActionContext context = ActionContext.create();
        DiscoveredInventory inventory = discoveredInventory();
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator();
             DelegationBridge delegationBridge = new DelegationBridge(coordinator)) {
            ActionCatalog fullCatalog = ProtoMoltCatalog.full(context);
            DelegationActions.register(fullCatalog, delegationBridge);
            for (ProtoAction metricAction : MetricActions.over(Map.of())) {
                fullCatalog.register(metricAction);
            }

            return new ReleaseBaselineBundle(
                    manifest,
                    captureJavaApi(inventory),
                    captureProtobufSurface(inventory),
                    captureStoredFormats(),
                    captureRestRoutes(fullCatalog),
                    captureActionEnvelopes(fullCatalog));
        }
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

    private static ProtobufSurfaceSnapshot captureProtobufSurface(DiscoveredInventory inventory) {
        return ProtobufSurfaceSnapshot.fromFiles(inventory.fileDescriptors());
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
                        false)));
    }

    private static RestRouteSnapshot captureRestRoutes(ActionCatalog fullCatalog) {
        ProtoRestMethodRegistry restRegistry = new ProtoRestMethodRegistry();
        ServiceDescriptor delegationService =
                ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                        .findServiceByName("DelegationService");
        ProtoMoltRestMount.register(
                restRegistry,
                fullCatalog,
                null,
                null,
                List.of(delegationService));

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

        // Confluent-compatible and native SchemaRegistryServer HTTP endpoints
        String srJson = "application/vnd.schemaregistry.v1+json";
        entries.add(new RestRouteEntry(
                "schema-registry:subjects.list",
                "/subjects",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                srJson,
                "",
                "SubjectList",
                List.of(200),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:subject.versions",
                "/subjects/{subject}/versions",
                List.of("GET", "POST"),
                List.of("subject"),
                List.of(),
                srJson,
                srJson,
                "RegisterSchemaRequest",
                "SubjectVersionsOrIdResponse",
                List.of(200, 404, 409, 422),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:subject.version.get",
                "/subjects/{subject}/versions/{version}",
                List.of("GET"),
                List.of("subject", "version"),
                List.of(),
                "",
                srJson,
                "",
                "RegisteredSchemaResponse",
                List.of(200, 404, 422),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:subject.lookup",
                "/subjects/{subject}",
                List.of("POST"),
                List.of("subject"),
                List.of(),
                srJson,
                srJson,
                "LookupSchemaRequest",
                "RegisteredSchemaResponse",
                List.of(200, 404, 422),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:schemas.by-id",
                "/schemas/ids/{id}",
                List.of("GET"),
                List.of("id"),
                List.of(),
                "",
                srJson,
                "",
                "SchemaByIdResponse",
                List.of(200, 404),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:config.global",
                "/config",
                List.of("GET", "PUT"),
                List.of(),
                List.of(),
                srJson,
                srJson,
                "CompatibilityConfigRequest",
                "CompatibilityConfigResponse",
                List.of(200, 422),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:config.subject",
                "/config/{subject}",
                List.of("GET", "PUT"),
                List.of("subject"),
                List.of(),
                srJson,
                srJson,
                "CompatibilityConfigRequest",
                "CompatibilityConfigResponse",
                List.of(200, 404, 422),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:native.descriptor-set",
                "/v1/subjects/{subject}/descriptor-set",
                List.of("GET"),
                List.of("subject"),
                List.of(),
                "",
                "application/x-protobuf",
                "",
                "google.protobuf.FileDescriptorSet",
                List.of(200, 404),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:native.parquet-schema",
                "/v1/subjects/{subject}/parquet-schema",
                List.of("GET"),
                List.of("subject"),
                List.of("message"),
                "",
                "text/plain; charset=utf-8",
                "",
                "ParquetMessageTypeText",
                List.of(200, 400, 404),
                false,
                StabilityTier.STABLE));
        entries.add(new RestRouteEntry(
                "schema-registry:health",
                "/health",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                srJson,
                "",
                "HealthStatusResponse",
                List.of(200),
                false,
                StabilityTier.STABLE));

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
            } catch (ai.protomolt.proto.actions.ActionException e) {
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
        Map<String, FileDescriptor> filesByName = new TreeMap<>();
        putFileDescriptor(filesByName, MetadataProto.getDescriptor());
        putFileDescriptor(filesByName, StabilityProto.getDescriptor());
        putFileDescriptor(filesByName, ValidateProto.getDescriptor());
        putFileDescriptor(filesByName, ProtoMoltServiceSchema.service().getFile());

        Map<String, Class<?>> javaClassesByName = new LinkedHashMap<>();
        for (Class<?> seed : CORE_JAVA_API_SEEDS) {
            javaClassesByName.put(seed.getName(), seed);
        }

        ClassLoader loader = CompatibilityBaselineGenerator.class.getClassLoader();
        for (Path entry : classpathEntries()) {
            if (isExcludedClasspathEntry(entry)) {
                continue;
            }
            List<String> classResources = collectProtomoltClassResources(entry);
            for (String resourcePath : classResources) {
                inspectClassResource(resourcePath, loader, filesByName, javaClassesByName);
            }
        }

        List<Class<?>> sortedClasses = new ArrayList<>(javaClassesByName.values());
        sortedClasses.sort(Comparator.comparing(Class::getName));
        return new DiscoveredInventory(
                List.copyOf(filesByName.values()),
                List.copyOf(sortedClasses));
    }

    static void inspectClassResource(
            String resourcePath,
            ClassLoader loader,
            Map<String, FileDescriptor> filesByName,
            Map<String, Class<?>> javaClassesByName) {
        if (!resourcePath.startsWith(PROTOMOLT_CLASS_PREFIX)
                || resourcePath.startsWith(SAMPLES_CLASS_PREFIX)
                || resourcePath.startsWith(CORRECTION_CLASS_PREFIX)
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

        boolean isProtocOuterHolder = false;
        Method getDescriptor = findZeroArgGetDescriptor(clazz, resourcePath);
        if (getDescriptor != null
                && Modifier.isPublic(getDescriptor.getModifiers())
                && Modifier.isStatic(getDescriptor.getModifiers())) {
            isProtocOuterHolder = invokeGetDescriptor(getDescriptor, resourcePath, filesByName);
        }

        if (!isExportedType(clazz, resourcePath)
                || isGeneratedProtobufClass(clazz, isProtocOuterHolder, resourcePath)) {
            return;
        }

        verifyMemberLinkage(clazz, resourcePath);
        javaClassesByName.put(clazz.getName(), clazz);
    }

    private static Method findZeroArgGetDescriptor(Class<?> clazz, String resourcePath) {
        try {
            return clazz.getDeclaredMethod("getDescriptor");
        } catch (NoSuchMethodException expected) {
            return null;
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to inspect methods of published class resource: " + resourcePath, e);
        }
    }

    private static boolean invokeGetDescriptor(
            Method getDescriptor,
            String resourcePath,
            Map<String, FileDescriptor> filesByName) {
        Class<?> returnType = getDescriptor.getReturnType();
        try {
            if (returnType == FileDescriptor.class) {
                FileDescriptor fd = (FileDescriptor) getDescriptor.invoke(null);
                putFileDescriptor(filesByName, fd);
                return true;
            } else if (returnType == Descriptor.class) {
                Descriptor md = (Descriptor) getDescriptor.invoke(null);
                if (md != null) {
                    putFileDescriptor(filesByName, md.getFile());
                }
            } else if (returnType == EnumDescriptor.class) {
                EnumDescriptor ed = (EnumDescriptor) getDescriptor.invoke(null);
                if (ed != null) {
                    putFileDescriptor(filesByName, ed.getFile());
                }
            }
            return false;
        } catch (ReflectiveOperationException | LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to invoke getDescriptor() on published class resource: " + resourcePath, e);
        }
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
                        || declaresStaticFileDescriptor(current)) {
                    return true;
                }
            }
            return false;
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to inspect protobuf hierarchy of published class resource: " + resourcePath, e);
        }
    }

    private static boolean declaresStaticFileDescriptor(Class<?> type) {
        try {
            Method m = type.getDeclaredMethod("getDescriptor");
            return Modifier.isPublic(m.getModifiers())
                    && Modifier.isStatic(m.getModifiers())
                    && m.getParameterCount() == 0
                    && m.getReturnType() == FileDescriptor.class;
        } catch (NoSuchMethodException expected) {
            return false;
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

    private static void putFileDescriptor(Map<String, FileDescriptor> filesByName, FileDescriptor fd) {
        if (fd == null || fd.getFullName() == null) {
            return;
        }
        String name = fd.getFullName();
        if (name.startsWith(SAMPLES_CLASS_PREFIX) || name.startsWith(CORRECTION_CLASS_PREFIX)) {
            return;
        }
        if (filesByName.putIfAbsent(name, fd) == null) {
            for (FileDescriptor dependency : fd.getDependencies()) {
                if (dependency.getFullName().startsWith(PROTOMOLT_CLASS_PREFIX)) {
                    putFileDescriptor(filesByName, dependency);
                }
            }
        }
    }

    private static List<Path> classpathEntries() {
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

    private static boolean isExcludedClasspathEntry(Path entry) {
        String normalized = entry.toString().replace('\\', '/');
        return normalized.endsWith("/build/classes/java/test")
                || normalized.contains("/samples/")
                || normalized.contains("/apps/correction/")
                || normalized.contains("/tests/system/");
    }

    private static List<String> collectProtomoltClassResources(Path entry) {
        List<String> resources = new ArrayList<>();
        if (Files.isDirectory(entry)) {
            Path root = entry.resolve("ai/protomolt/proto");
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
                            && name.startsWith(PROTOMOLT_CLASS_PREFIX)
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

    private record DiscoveredInventory(
            List<FileDescriptor> fileDescriptors,
            List<Class<?>> javaApiClasses) {
    }
}
