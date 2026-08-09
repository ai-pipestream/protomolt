package ai.pipestream.proto.grpc.profile;

import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.HealthProbe;
import ai.pipestream.proto.grpc.profile.v1.MethodPolicy;
import ai.pipestream.proto.grpc.profile.v1.Operation;
import ai.pipestream.proto.grpc.profile.v1.SchemaSource;
import ai.pipestream.proto.grpc.profile.v1.ServiceEndpoint;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.grpc.profile.v1.SourceKind;
import ai.pipestream.proto.grpc.profile.v1.Transport;
import com.google.protobuf.InvalidProtocolBufferException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.regex.Pattern;

/** Structural and safety validation shared by repository implementations and callers. */
public final class ServiceProfileValidation {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final Pattern FINGERPRINT = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern DNS_HOST = Pattern.compile(
            "(?=.{1,253}\\.?$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\\.)*"
                    + "[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\\.?");
    private static final Pattern IPV6_HOST = Pattern.compile("[0-9A-Fa-f:.]+");
    private static final Pattern SECRET_REFERENCE = Pattern.compile(
            "[A-Za-z][A-Za-z0-9+._-]{1,31}:(?=[A-Za-z0-9._:/@+-]{1,256}$)"
                    + "(?=.*[A-Za-z0-9])[A-Za-z0-9._:/@+-]+");

    /** Maximum serialized profile size accepted by the durable repository. */
    public static final int MAX_PROFILE_BYTES = 256 * 1024;

    /** Maximum serialized descriptor set accepted as one artifact. */
    public static final int MAX_DESCRIPTOR_ARTIFACT_BYTES = 16 * 1024 * 1024;

    private ServiceProfileValidation() {
    }

    /** Validates a profile or throws {@link IllegalArgumentException} with the bad field. */
    public static void validate(ServiceProfile profile) {
        validateConnectionProfile(profile);
        validateSchemaSource(profile.getSchemaSource());
    }

    /**
     * Validates the caller-authored portion of a profile before opening a network connection.
     * The schema source is intentionally excluded because reflection supplies it.
     */
    public static void validateConnectionProfile(ServiceProfile profile) {
        require(profile != null, "profile must not be null");
        require(profile.getSerializedSize() <= MAX_PROFILE_BYTES,
                "profile exceeds the maximum serialized size of " + MAX_PROFILE_BYTES + " bytes");
        validateName(profile.getName(), "name");
        require(profile.getEndpointsCount() > 0, "endpoints must not be empty");

        Set<String> endpointNames = new HashSet<>();
        for (ServiceEndpoint endpoint : profile.getEndpointsList()) {
            validateName(endpoint.getName(), "endpoint.name");
            require(endpointNames.add(endpoint.getName()),
                    "duplicate endpoint name: " + endpoint.getName());
            validateHost(endpoint.getHost());
            require(endpoint.getPort() >= 1 && endpoint.getPort() <= 65535,
                    "endpoint.port must be between 1 and 65535");
            require(endpoint.getTransport() == Transport.TRANSPORT_PLAINTEXT
                            || endpoint.getTransport() == Transport.TRANSPORT_TLS,
                    "endpoint.transport must be plaintext or TLS");
            validateSecretReference(endpoint.getCredentialRef(), "endpoint.credential_ref");
            validateSecretReference(endpoint.getTrustRef(), "endpoint.trust_ref");
            validateSecretReference(endpoint.getClientCertificateRef(),
                    "endpoint.client_certificate_ref");
        }

        if (profile.hasHealthProbe()) {
            validateHealthProbe(profile.getHealthProbe());
        }

        Set<String> methods = new HashSet<>();
        for (MethodPolicy policy : profile.getMethodPoliciesList()) {
            validateMethod(policy.getMethod(), "method_policies.method");
            require(methods.add(policy.getMethod()),
                    "duplicate method policy: " + policy.getMethod());
            require(policy.getOperationCount() > 0, "method policy operation must not be empty");
            for (Operation operation : policy.getOperationList()) {
                require(operation == Operation.OPERATION_READ_ONLY
                                || operation == Operation.OPERATION_MUTATING
                                || operation == Operation.OPERATION_IDEMPOTENT
                                || operation == Operation.OPERATION_APPROVAL_REQUIRED,
                        "method policy operation must be recognized");
            }
            validateDuration(policy.getDeadline().getSeconds(), policy.getDeadline().getNanos(),
                    "method_policies.deadline");
        }
    }

    /** Validates a descriptor artifact and its content-addressed fingerprint. */
    public static void validate(DescriptorArtifact artifact) {
        require(artifact != null, "artifact must not be null");
        require(FINGERPRINT.matcher(artifact.getFingerprint()).matches(),
                "artifact.fingerprint must be a lowercase SHA-256 fingerprint");
        require(artifact.getDescriptorSet().size() > 0, "artifact.descriptor_set must not be empty");
        require(artifact.getDescriptorSet().size() <= MAX_DESCRIPTOR_ARTIFACT_BYTES,
                "artifact.descriptor_set exceeds the maximum size of "
                        + MAX_DESCRIPTOR_ARTIFACT_BYTES + " bytes");
        try {
            var descriptorSet = com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(
                    artifact.getDescriptorSet());
            require(descriptorSet.getFileCount() > 0,
                    "artifact.descriptor_set must contain at least one file");
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("artifact.descriptor_set is not a FileDescriptorSet", e);
        }
        String actual = sha256(artifact.getDescriptorSet().toByteArray());
        require(actual.equals(artifact.getFingerprint()),
                "artifact.fingerprint does not match descriptor_set SHA-256");
    }

    /** Validates the path-safe profile name used by filesystem repositories. */
    public static void validateName(String name, String field) {
        require(name != null && NAME.matcher(name).matches(),
                field + " must be a single path-safe name");
    }

    /** Validates a descriptor fingerprint used as a content-addressed artifact key. */
    public static void validateFingerprint(String fingerprint) {
        require(fingerprint != null && FINGERPRINT.matcher(fingerprint).matches(),
                "fingerprint must be a lowercase SHA-256 fingerprint");
    }

    /** Computes the lowercase SHA-256 fingerprint used by descriptor artifacts. */
    public static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("JDK does not provide SHA-256", e);
        }
    }

    private static void validateSchemaSource(SchemaSource source) {
        require(source.getKind() == SourceKind.SOURCE_KIND_REFLECTION
                        || source.getKind() == SourceKind.SOURCE_KIND_REGISTRY
                        || source.getKind() == SourceKind.SOURCE_KIND_ARTIFACT,
                "schema_source.kind must be recognized");
        validateReference(source.getSourceRef(), "schema_source.source_ref");
        validateFingerprint(source.getDescriptorFingerprint());
        validateReference(source.getDescriptorArtifactRef(), "schema_source.descriptor_artifact_ref");
        require(!source.getDescriptorArtifactRef().isBlank(),
                "schema_source.descriptor_artifact_ref must not be blank");
    }

    private static void validateHealthProbe(HealthProbe probe) {
        if (probe.getEnabled()) {
            validateMethod(probe.getMethod(), "health_probe.method");
            require(!probe.getTimeout().equals(com.google.protobuf.Duration.getDefaultInstance()),
                    "enabled health_probe.timeout must be positive");
            validateDuration(probe.getTimeout().getSeconds(), probe.getTimeout().getNanos(),
                    "health_probe.timeout");
        } else if (!probe.getMethod().isBlank()) {
            validateMethod(probe.getMethod(), "health_probe.method");
        }
        validateReference(probe.getService(), "health_probe.service");
    }

    private static void validateMethod(String method, String field) {
        require(method != null && !method.isBlank() && method.indexOf('/') > 0
                        && method.indexOf('/') == method.lastIndexOf('/')
                        && method.indexOf('/') < method.length() - 1
                        && method.codePoints().noneMatch(Character::isWhitespace),
                field + " must use Service/Method form");
    }

    private static void validateReference(String value, String field) {
        require(value == null || value.codePoints().noneMatch(Character::isWhitespace),
                field + " must be an opaque reference without whitespace");
    }

    private static void validateSecretReference(String value, String field) {
        require(value == null || value.isBlank() || SECRET_REFERENCE.matcher(value).matches(),
                field + " must be an empty value or a namespaced opaque reference");
    }

    private static void validateHost(String host) {
        require(host != null && !host.isBlank(), "endpoint.host must not be blank");
        boolean dns = DNS_HOST.matcher(host).matches();
        boolean ipv6 = host.indexOf(':') >= 0 && host.chars().filter(c -> c == ':').count() >= 2
                && IPV6_HOST.matcher(host).matches();
        require(dns || ipv6,
                "endpoint.host must be a DNS name or unbracketed IP address, not a URI target");
    }

    private static void validateDuration(long seconds, int nanos, String field) {
        require(seconds >= 0 && nanos >= 0 && nanos < 1_000_000_000,
                field + " must be a non-negative valid duration");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
