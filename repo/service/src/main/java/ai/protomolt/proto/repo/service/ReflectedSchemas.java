package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.ReflectionClient;
import ai.protomolt.proto.grpc.invoke.ReflectionException;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Schemas the host reads from named gRPC servers over the server-reflection protocol, so a
 * typed {@code Any} whose message a producing service owns (a parser's document model)
 * resolves at admission without that service's {@code .proto} files being copied into this
 * build.
 *
 * <p>The host names the servers; a save never does. A type the repository contract or a
 * co-mounted module already defines is never asked of them. Every admission that needs a
 * reflected type asks the server again, so the descriptor a save is validated against is the
 * one the server serves at that save and a schema the producer has since extended is never
 * decoded with a stale copy. Built definitions are kept by the SHA-256 of their descriptor
 * closure, never by type name.</p>
 *
 * <p>The three outcomes stay distinct: a server that answers it does not know the type
 * leaves the type unresolved ({@code SCHEMA_MISSING}); a server that cannot be asked is
 * {@link DocumentCandidateAdmission.SchemaUnavailable} ({@code UNAVAILABLE}) and is never
 * read as "does not know"; a descriptor set that does not link, or does not hold the type it
 * was asked for, is {@link DocumentCandidateAdmission.SchemaUnavailable} as well, because
 * the source is broken and nothing is known about the type.</p>
 */
public final class ReflectedSchemas implements AutoCloseable {

    /** Comma-separated {@code host:port} gRPC targets whose reflected types are admitted. */
    public static final String ENV_TARGETS = "DOCUMENT_PLATFORM_REFLECTED_SCHEMA_TARGETS";
    /** {@code true} dials every target with TLS against the system roots; default plaintext. */
    public static final String ENV_TLS = "DOCUMENT_PLATFORM_REFLECTED_SCHEMA_TLS";
    /** Deadline of one reflection lookup in milliseconds, 1 to 60000; default 5000. */
    public static final String ENV_TIMEOUT_MS = "DOCUMENT_PLATFORM_REFLECTED_SCHEMA_TIMEOUT_MS";

    static final long DEFAULT_TIMEOUT_MS = 5_000;
    /** Most targets one host may name. */
    static final int MAX_TARGETS = 16;
    /** Most built definitions kept; at the bound they are all dropped and rebuilt on demand. */
    static final int MAX_DEFINITIONS = 256;

    private static final String TYPE_URL_PREFIX = "type.googleapis.com/";
    /** The bounds admission itself links a schema artifact under; a larger closure is refused there too. */
    private static final ClosedDescriptorSet.Limits LINK_LIMITS =
            new ClosedDescriptorSet.Limits(16 * 1024 * 1024, 256, 4096, 64);

    private record Source(String target, ManagedChannel channel) {
    }

    private final List<Source> sources;
    private final long timeoutMs;
    private final Map<String, DocumentSchemaAdmission.Definition> byArtifact = new ConcurrentHashMap<>();

    private ReflectedSchemas(List<Source> sources, long timeoutMs) {
        this.sources = List.copyOf(sources);
        this.timeoutMs = timeoutMs;
    }

    /**
     * The reflected schema sources the environment names, or empty when
     * {@value #ENV_TARGETS} is unset or blank. No connection is made here: a channel dials
     * on the first lookup.
     *
     * @param environment the process environment
     * @return the configured sources, if any
     * @throws IllegalArgumentException naming the variable, for a value outside the above
     */
    public static Optional<ReflectedSchemas> fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment);
        String raw = environment.get(ENV_TARGETS);
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        List<String> targets = new ArrayList<>();
        int start = 0;
        while (start <= raw.length()) {
            int comma = raw.indexOf(',', start);
            int end = comma < 0 ? raw.length() : comma;
            String target = raw.substring(start, end).strip();
            if (target.isEmpty()) {
                throw new IllegalArgumentException(ENV_TARGETS + " holds an empty target");
            }
            if (targets.contains(target)) {
                throw new IllegalArgumentException(ENV_TARGETS + " names " + target + " twice");
            }
            targets.add(target);
            start = end + 1;
        }
        if (targets.size() > MAX_TARGETS) {
            throw new IllegalArgumentException(ENV_TARGETS + " names more than " + MAX_TARGETS + " targets");
        }
        boolean tls = flag(environment.get(ENV_TLS));
        long timeoutMs = timeout(environment.get(ENV_TIMEOUT_MS));
        return Optional.of(open(targets, tls, timeoutMs, ChannelFactory.standard()));
    }

    /**
     * Opens the sources over {@code channels}; visible for hosts and tests that own the
     * transport.
     *
     * @param targets the gRPC targets, asked in this order
     * @param tls whether every target is dialled with TLS
     * @param timeoutMs the deadline of one reflection lookup
     * @param channels the factory that opens each target's channel
     * @return the open sources
     */
    public static ReflectedSchemas open(List<String> targets, boolean tls, long timeoutMs, ChannelFactory channels) {
        Objects.requireNonNull(targets);
        Objects.requireNonNull(channels);
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("At least one reflected schema target is required");
        }
        if (timeoutMs < 1 || timeoutMs > ReflectionClient.MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException(ENV_TIMEOUT_MS + " must be from 1 to " + ReflectionClient.MAX_TIMEOUT_MS);
        }
        List<Source> opened = new ArrayList<>();
        try {
            for (String target : targets) {
                channels.validateTarget(target, tls);
                opened.add(new Source(target, channels.open(target, tls)));
            }
        } catch (RuntimeException | Error failure) {
            for (Source source : opened) {
                source.channel().shutdownNow();
            }
            throw failure;
        }
        return new ReflectedSchemas(opened, timeoutMs);
    }

    /**
     * The definition of {@code typeUrl} as a configured server serves it now.
     *
     * @param typeUrl an {@code Any} type URL
     * @return the definition, or {@code null} when every server answered that it does not
     *         know the type
     * @throws DocumentCandidateAdmission.SchemaUnavailable when no server defined the type
     *         and at least one could not be asked, or a server's answer cannot be used
     */
    public DocumentSchemaAdmission.Definition select(String typeUrl) {
        Objects.requireNonNull(typeUrl);
        if (!typeUrl.startsWith(TYPE_URL_PREFIX) || typeUrl.length() == TYPE_URL_PREFIX.length()) {
            return null;
        }
        String typeName = typeUrl.substring(TYPE_URL_PREFIX.length());
        DocumentCandidateAdmission.SchemaUnavailable unavailable = null;
        for (Source source : sources) {
            Optional<FileDescriptorSet> closure;
            try {
                // A channel that failed to connect refuses calls until its backoff ends, which
                // can keep refusing saves for minutes after the server is back. Every ask of a
                // failed channel starts a new dial instead, so the wait is one dial: this ask
                // still fails at once, and the caller's retry finds the server.
                if (source.channel().getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
                    source.channel().resetConnectBackoff();
                }
                closure = ReflectionClient.closure(source.channel(), typeName, timeoutMs);
            } catch (ReflectionException failure) {
                var outage = new DocumentCandidateAdmission.SchemaUnavailable(
                        "Reflected schema source " + source.target() + " could not be asked for " + typeName, failure);
                if (unavailable == null) {
                    unavailable = outage;
                } else {
                    unavailable.addSuppressed(outage);
                }
                continue;
            }
            if (closure.isPresent()) {
                return definition(source, typeName, closure.get());
            }
        }
        if (unavailable != null) {
            throw unavailable;
        }
        return null;
    }

    private DocumentSchemaAdmission.Definition definition(Source source, String typeName, FileDescriptorSet reflected) {
        Descriptors.Descriptor type;
        try {
            type = link(reflected, typeName);
        } catch (IllegalArgumentException broken) {
            throw new DocumentCandidateAdmission.SchemaUnavailable(
                    "Reflected schema source " + source.target() + " served descriptors for " + typeName
                            + " that do not link: " + broken.getMessage(), broken);
        }
        if (type == null) {
            throw new DocumentCandidateAdmission.SchemaUnavailable(
                    "Reflected schema source " + source.target() + " answered for " + typeName
                            + " with descriptors that do not declare it", null);
        }
        FileDescriptorSet closure = DescriptorFingerprints.closure(type);
        ByteString bytes = closure.toByteString();
        String artifactSha = sha256(bytes);
        // One definition per artifact and type: the same closure read twice is the same
        // definition object, and a changed schema is a different key, never a replacement.
        String key = artifactSha + ' ' + typeName;
        DocumentSchemaAdmission.Definition known = byArtifact.get(key);
        if (known != null) {
            return known;
        }
        var metadata = RepositorySchemaAsset.newBuilder()
                .setTypeUrl(TYPE_URL_PREFIX + typeName)
                .setArtifactSha256(artifactSha)
                .setSchema(PublicationSchemaCondition.newBuilder()
                        .setTypeName(typeName)
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("Descriptor read over gRPC server reflection from " + source.target())
                        .setAdmissionRuntime(BuiltinDocumentSchema.admissionRuntime()))
                .build();
        var built = new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
        if (byArtifact.size() >= MAX_DEFINITIONS) {
            byArtifact.clear();
        }
        DocumentSchemaAdmission.Definition raced = byArtifact.putIfAbsent(key, built);
        return raced == null ? built : raced;
    }

    /** Links the reflected files and returns the named message, or {@code null} when absent. */
    private static Descriptors.Descriptor link(FileDescriptorSet reflected, String typeName) {
        for (Descriptors.FileDescriptor file : ClosedDescriptorSet.load(reflected.toByteString(), LINK_LIMITS)) {
            Descriptors.Descriptor found = message(file, typeName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** The message {@code typeName} of {@code file}, top-level or nested, or {@code null}. */
    private static Descriptors.Descriptor message(Descriptors.FileDescriptor file, String typeName) {
        String scope = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
        if (!typeName.startsWith(scope)) {
            return null;
        }
        Descriptors.Descriptor current = null;
        int start = scope.length();
        while (start <= typeName.length()) {
            int dot = typeName.indexOf('.', start);
            String segment = typeName.substring(start, dot < 0 ? typeName.length() : dot);
            current = current == null ? file.findMessageTypeByName(segment) : current.findNestedTypeByName(segment);
            if (current == null) {
                return null;
            }
            if (dot < 0) {
                return current;
            }
            start = dot + 1;
        }
        return null;
    }

    private static boolean flag(String raw) {
        if (raw == null || raw.isBlank() || raw.strip().equals("false")) {
            return false;
        }
        if (raw.strip().equals("true")) {
            return true;
        }
        throw new IllegalArgumentException(ENV_TLS + " must be true or false");
    }

    private static long timeout(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT_TIMEOUT_MS;
        }
        long value;
        try {
            value = Long.parseLong(raw.strip());
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(ENV_TIMEOUT_MS + " must be a whole number of milliseconds", notANumber);
        }
        if (value < 1 || value > ReflectionClient.MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException(ENV_TIMEOUT_MS + " must be from 1 to " + ReflectionClient.MAX_TIMEOUT_MS);
        }
        return value;
    }

    private static String sha256(ByteString bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    /** The configured targets, in the order they are asked. */
    public List<String> targets() {
        return sources.stream().map(Source::target).toList();
    }

    @Override
    public void close() {
        for (Source source : sources) {
            source.channel().shutdownNow();
        }
        for (Source source : sources) {
            try {
                source.channel().awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
