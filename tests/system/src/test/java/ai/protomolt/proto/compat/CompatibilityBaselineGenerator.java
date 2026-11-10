package ai.protomolt.proto.compat;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.actions.StreamEmitter;
import ai.protomolt.proto.actions.StreamingAction;
import ai.protomolt.proto.compat.ActionEnvelopeSnapshot.ActionEnvelopeEntry;
import ai.protomolt.proto.compat.RestRouteSnapshot.RestRouteEntry;
import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
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
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.meta.StabilityProto;
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
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentManifest;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Captures the live five-surface compatibility bundle for the repository and either
 * updates or verifies {@code compatibility/baseline/}.
 */
public final class CompatibilityBaselineGenerator {

    private static final String DEFAULT_RELEASE_VERSION = "0.1.0-SNAPSHOT";

    private static final List<Class<?>> TRACKED_JAVA_API_TYPES = List.of(
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
        ActionContext context = ActionContext.create();
        ActionCatalog fullCatalog = ProtoMoltCatalog.full(context);

        return new ReleaseBaselineBundle(
                manifest,
                captureJavaApi(),
                captureProtobufSurface(),
                captureStoredFormats(),
                captureRestRoutes(fullCatalog),
                captureActionEnvelopes(fullCatalog));
    }

    private static ReleaseBaselineManifest loadExistingManifestOrDefault(Path baselineDir)
            throws IOException {
        Path manifestFile = baselineDir.resolve(ReleaseBaselineGate.MANIFEST_FILE);
        if (Files.exists(manifestFile)) {
            return ReleaseBaselineGate.load(baselineDir).manifest();
        }
        return ReleaseBaselineManifest.unreleasedInitial(DEFAULT_RELEASE_VERSION, List.of());
    }

    private static JavaApiSnapshot captureJavaApi() {
        return JavaApiSnapshot.capture(TRACKED_JAVA_API_TYPES, StabilityTier.STABLE);
    }

    private static ProtobufSurfaceSnapshot captureProtobufSurface() throws ProtoCompilationException {
        return ProtobufSurfaceSnapshot.fromFiles(compilePublishedDescriptors().fileDescriptors());
    }

    static CompiledProtos compilePublishedDescriptors() throws ProtoCompilationException {
        Path root;
        try {
            root = repositoryRoot();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        List<ProtoSource> sources = new ArrayList<>();
        try (var paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".proto"))
                    .filter(path -> {
                        String relative = relativePath(root, path);
                        return relative.contains("/src/main/proto/")
                                || relative.contains("/src/main/resources/");
                    })
                    .sorted()
                    .forEach(path -> {
                        try {
                            sources.add(new ProtoSource(importPath(root, path),
                                    Files.readString(path), path.toString()));
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ProtoSourceCompiler().compile(ProtoSourceSet.of(sources));
    }

    private static String importPath(Path root, Path path) {
        String relative = relativePath(root, path);
        String sourceRoot = relative.substring(relative.indexOf("/src/main/") + "/src/main/".length());
        int separator = sourceRoot.indexOf('/');
        return sourceRoot.substring(separator + 1);
    }

    private static String relativePath(Path root, Path path) {
        return root.relativize(path).toString().replace(root.getFileSystem().getSeparator(), "/");
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
                        false)));
    }

    private static RestRouteSnapshot captureRestRoutes(ActionCatalog fullCatalog) {
        ProtoRestMethodRegistry restRegistry = new ProtoRestMethodRegistry();
        ProtoMoltRestMount.register(restRegistry, fullCatalog);

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
}
