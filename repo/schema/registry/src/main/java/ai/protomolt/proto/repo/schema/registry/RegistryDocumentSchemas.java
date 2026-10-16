package ai.protomolt.proto.repo.schema.registry;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.StoredSchema;
import ai.protomolt.proto.registry.StoredSchemaSources;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerDetails;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.sources.ProtoCompilationException;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import com.google.protobuf.Descriptors.Descriptor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The latest {@code Document} version registered in a schema registry store
 * (requirements §15.7, decisions 34 and 35). The subject's registered source and
 * its references are compiled with the registry's own pipeline
 * ({@link ProtoSourceCompiler}), and the {@code Document} descriptor closure becomes
 * the definition every save decodes with. The registry coordinates of the version
 * are kept in the definition's compiler options, so a revision's retained schema
 * names the registered version it was saved with.
 * <p>
 * Which versions can be registered is the subject's compatibility mode, enforced
 * by the store's write gate: the {@code Document} subject is configured
 * BACKWARD_TRANSITIVE (decision 34), so a new version is checked against every
 * registered one, while other subjects keep the registry default. A save under a
 * weaker effective mode is a host failure, never a silent downgrade. Compiled
 * definitions are cached by registered content; the registry is asked for the
 * latest version on every call.
 */
public final class RegistryDocumentSchemas implements DocumentSchemas {
    private static final String TYPE = "ai.protomolt.proto.repo.v1.Document";
    private static final ProtoSourceCompiler COMPILER = new ProtoSourceCompiler();
    private final SchemaRegistryStore store;
    private final String subject;
    private final Map<String, DocumentSchemaAdmission.Definition> compiled = new ConcurrentHashMap<>();

    public RegistryDocumentSchemas(SchemaRegistryStore store) { this(store, SUBJECT); }

    public RegistryDocumentSchemas(SchemaRegistryStore store, String subject) {
        this.store = Objects.requireNonNull(store, "store");
        this.subject = Objects.requireNonNull(subject, "subject");
    }

    /** Configures the {@code Document} subject's compatibility mode (decision 34). Administrative. */
    public static void configureSubject(SchemaRegistryStore store) {
        store.setCompatibilityMode(SUBJECT, REQUIRED_MODE);
    }

    /** Decision 34: the {@code Document} subject's mode. FULL_TRANSITIVE also checks every version. */
    static final String REQUIRED_MODE = "BACKWARD_TRANSITIVE";

    @Override public DocumentSchemaAdmission.Definition latest() {
        final Optional<StoredSchema> latest;
        final String mode;
        try {
            mode = store.compatibilityMode(subject).orElseGet(store::globalCompatibilityMode);
            latest = store.latest(subject);
        } catch (RegistryStoreException failure) {
            throw new DocumentCandidateAdmission.SchemaUnavailable("Schema registry unavailable", failure);
        }
        if (!mode.equals(REQUIRED_MODE) && !mode.equals("FULL_TRANSITIVE"))
            throw new DocumentCandidateAdmission.HostFailure("The " + subject + " subject's compatibility mode is " + mode
                    + "; decision 34 requires " + REQUIRED_MODE, null);
        var version = latest.orElseThrow(() -> new DocumentCandidateAdmission.HostFailure(
                "The schema registry has no " + subject + " subject", null));
        if (compiled.size() > 64) compiled.clear();
        return compiled.computeIfAbsent(version.contentHash(), hash -> compile(version));
    }

    private DocumentSchemaAdmission.Definition compile(StoredSchema version) {
        final Descriptor type;
        try {
            var sources = StoredSchemaSources.resolve(store, version);
            type = COMPILER.compile(sources.sources()).fileDescriptors().stream()
                    .flatMap(file -> file.getMessageTypes().stream()).filter(message -> message.getFullName().equals(TYPE))
                    .findFirst().orElseThrow(() -> new DocumentCandidateAdmission.HostFailure(
                            subject + " version " + version.version() + " does not define " + TYPE, null));
        } catch (RegistryStoreException failure) {
            throw new DocumentCandidateAdmission.SchemaUnavailable("Schema registry unavailable", failure);
        } catch (ProtoCompilationException failure) {
            throw new DocumentCandidateAdmission.HostFailure(subject + " version " + version.version()
                    + " does not compile: " + failure.getMessage(), failure);
        }
        var closure = DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var compiler = SchemaCompilerDetails.newBuilder()
                .setTool(SchemaToolIdentity.newBuilder().setName("protomolt-sources ProtoSourceCompiler")
                        .setVersion(implementationVersion()))
                .putOptions("registry.subject", version.subject())
                .putOptions("registry.version", Integer.toString(version.version()))
                .putOptions("registry.global_id", Integer.toString(version.globalId()))
                .putOptions("registry.content_sha256", version.contentHash());
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.googleapis.com/" + TYPE)
                .setArtifactSha256(sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(TYPE)
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_PRODUCER_REPORTED)
                        .setKnownCompiler(compiler).setAdmissionRuntime(BuiltinDocumentSchema.admissionRuntime()))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    private static String implementationVersion() {
        var version = ProtoSourceCompiler.class.getPackage().getImplementationVersion();
        return version == null || version.isBlank() ? "unversioned build" : version;
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
