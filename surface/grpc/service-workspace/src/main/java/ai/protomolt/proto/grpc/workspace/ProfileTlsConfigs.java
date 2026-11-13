package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.grpc.workspace.v1.ApprovedTlsEndpoint;
import ai.protomolt.proto.grpc.workspace.v1.ClientCertificateMount;
import ai.protomolt.proto.grpc.workspace.v1.ProfileTlsConfig;
import ai.protomolt.proto.grpc.workspace.v1.TrustBundleMount;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Loads and validates {@link ProfileTlsConfig} documents for host launchers ({@code protomolt-serve}
 * and {@code protomolt-mcp}) and builds a {@link FileProfileTlsResolver} over mounted PEM files.
 *
 * <p>Every field is checked against its declared {@code validate.v1} and {@code buf.validate}
 * rules, reference and binding uniqueness is enforced, and every {@code trust_ref} or
 * {@code client_certificate_ref} named by an approved binding must exist in the document. Relative
 * file paths resolve against {@code root_directory} (or the configuration file's parent directory
 * when {@code root_directory} is unset) so container mounts never hardcode host paths.
 */
public final class ProfileTlsConfigs {

    /** Maximum byte size accepted for a profile TLS configuration file (1 MiB). */
    public static final int MAX_CONFIG_BYTES = 1_048_576;

    private ProfileTlsConfigs() {
    }

    /** Refuses an invalid, duplicate, or dangling configuration; returns {@code config} for chaining. */
    public static ProfileTlsConfig requireWellFormed(ProfileTlsConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("profile TLS config must not be null");
        }
        ValidationResult rules = ProtoValidator.create().validate(config);
        if (!rules.valid()) {
            throw new IllegalArgumentException("profile TLS config is invalid: "
                    + rules.violations().stream()
                            .map(violation -> violation.path() + ": " + violation.message())
                            .collect(Collectors.joining("; ")));
        }
        if (!config.getRootDirectory().isEmpty()) {
            requireCleanPath(config.getRootDirectory(), "root_directory");
        }
        Set<String> trustRefs = new LinkedHashSet<>();
        for (TrustBundleMount mount : config.getTrustBundlesList()) {
            requireCleanToken(mount.getRef(), "trust_bundles.ref");
            requireCleanPath(mount.getBundlePath(), "trust_bundles.bundle_path");
            if (!trustRefs.add(mount.getRef())) {
                throw new IllegalArgumentException(
                        "profile TLS config duplicates trust_bundles ref '" + mount.getRef() + "'");
            }
        }
        Set<String> clientRefs = new LinkedHashSet<>();
        for (ClientCertificateMount mount : config.getClientCertificatesList()) {
            requireCleanToken(mount.getRef(), "client_certificates.ref");
            requireCleanPath(mount.getCertificateChainPath(),
                    "client_certificates.certificate_chain_path");
            requireCleanPath(mount.getPrivateKeyPath(), "client_certificates.private_key_path");
            if (!clientRefs.add(mount.getRef())) {
                throw new IllegalArgumentException(
                        "profile TLS config duplicates client_certificates ref '"
                                + mount.getRef() + "'");
            }
        }
        Set<FileProfileTlsResolver.Binding> bindings = new LinkedHashSet<>();
        for (ApprovedTlsEndpoint approved : config.getBindingsList()) {
            requireCleanToken(approved.getProfile(), "bindings.profile");
            requireCleanToken(approved.getEndpoint(), "bindings.endpoint");
            requireCleanToken(approved.getHost(), "bindings.host");
            if (!approved.getTrustRef().isEmpty()) {
                requireCleanToken(approved.getTrustRef(), "bindings.trust_ref");
                if (!trustRefs.contains(approved.getTrustRef())) {
                    throw new IllegalArgumentException("binding for profile '"
                            + approved.getProfile() + "' endpoint '" + approved.getEndpoint()
                            + "' names unconfigured trust_ref '" + approved.getTrustRef() + "'");
                }
            }
            if (!approved.getClientCertificateRef().isEmpty()) {
                requireCleanToken(
                        approved.getClientCertificateRef(), "bindings.client_certificate_ref");
                if (!clientRefs.contains(approved.getClientCertificateRef())) {
                    throw new IllegalArgumentException("binding for profile '"
                            + approved.getProfile() + "' endpoint '" + approved.getEndpoint()
                            + "' names unconfigured client_certificate_ref '"
                            + approved.getClientCertificateRef() + "'");
                }
            }
            FileProfileTlsResolver.Binding binding = new FileProfileTlsResolver.Binding(
                    approved.getProfile(), approved.getEndpoint(), approved.getHost(),
                    approved.getPort(), approved.getTrustRef(),
                    approved.getClientCertificateRef());
            if (!bindings.add(binding)) {
                throw new IllegalArgumentException("profile TLS config duplicates binding for "
                        + "profile '" + approved.getProfile() + "' endpoint '"
                        + approved.getEndpoint() + "'");
            }
        }
        return config;
    }

    /**
     * Loads and validates a profile TLS configuration file ({@code .json}, {@code .binpb}, or
     * {@code .pb}), enforcing {@link #MAX_CONFIG_BYTES} before parsing.
     */
    public static ProfileTlsConfig load(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        Path fileName = file.getFileName();
        String name = fileName == null ? file.toString() : fileName.toString();
        byte[] bytes = readBoundedFile(file, name);
        if (name.endsWith(".json")) {
            ProfileTlsConfig.Builder builder = ProfileTlsConfig.newBuilder();
            try {
                JsonFormat.parser().merge(new String(bytes, StandardCharsets.UTF_8), builder);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "profile TLS config file '" + name + "' does not parse: " + e.getMessage(),
                        e);
            }
            return requireWellFormed(builder.build());
        }
        if (name.endsWith(".binpb") || name.endsWith(".pb")) {
            try {
                return requireWellFormed(ProfileTlsConfig.parseFrom(bytes));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "profile TLS config file '" + name + "' does not parse: " + e.getMessage(),
                        e);
            }
        }
        throw new IllegalArgumentException(
                "profile TLS config file '" + name + "' must end in .json, .binpb, or .pb");
    }

    /**
     * Loads {@code file} and constructs a {@link FileProfileTlsResolver}, resolving relative PEM
     * paths against {@code root_directory} or {@code file}'s parent directory.
     */
    public static FileProfileTlsResolver loadResolver(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        Path normalized = file.toAbsolutePath().normalize();
        return toResolver(load(normalized), normalized.getParent());
    }

    /**
     * Builds a {@link FileProfileTlsResolver} from a validated {@link ProfileTlsConfig}.
     *
     * @param config validated or unvalidated configuration message
     * @param defaultBaseDirectory base directory used when {@code config.root_directory} is empty,
     *        and for resolving a relative {@code root_directory}; may be null when paths are absolute
     */
    public static FileProfileTlsResolver toResolver(ProfileTlsConfig config,
                                                    Path defaultBaseDirectory) {
        requireWellFormed(config);
        Path base = resolveBaseDirectory(config.getRootDirectory(), defaultBaseDirectory);

        Map<String, Path> trustBundles = new LinkedHashMap<>();
        for (TrustBundleMount mount : config.getTrustBundlesList()) {
            trustBundles.put(mount.getRef(), resolvePath(base, mount.getBundlePath()));
        }

        Map<String, FileProfileTlsResolver.KeyPairFiles> clientCertificates = new LinkedHashMap<>();
        for (ClientCertificateMount mount : config.getClientCertificatesList()) {
            clientCertificates.put(mount.getRef(), new FileProfileTlsResolver.KeyPairFiles(
                    resolvePath(base, mount.getCertificateChainPath()),
                    resolvePath(base, mount.getPrivateKeyPath())));
        }

        Set<FileProfileTlsResolver.Binding> allowed = new LinkedHashSet<>();
        for (ApprovedTlsEndpoint approved : config.getBindingsList()) {
            allowed.add(new FileProfileTlsResolver.Binding(
                    approved.getProfile(), approved.getEndpoint(), approved.getHost(),
                    approved.getPort(), approved.getTrustRef(),
                    approved.getClientCertificateRef()));
        }
        return new FileProfileTlsResolver(allowed, trustBundles, clientCertificates);
    }

    private static Path resolveBaseDirectory(String configuredRoot, Path fallbackBase) {
        Path normalizedFallback = fallbackBase == null
                ? null : fallbackBase.toAbsolutePath().normalize();
        if (configuredRoot.isEmpty()) {
            return normalizedFallback;
        }
        Path root = Path.of(configuredRoot);
        if (root.isAbsolute()) {
            return root.normalize();
        }
        return normalizedFallback == null
                ? root.toAbsolutePath().normalize()
                : normalizedFallback.resolve(root).normalize();
    }

    private static Path resolvePath(Path base, String rawPath) {
        Path parsed = Path.of(rawPath);
        if (parsed.isAbsolute()) {
            return parsed.normalize();
        }
        return base == null
                ? parsed.toAbsolutePath().normalize()
                : base.resolve(parsed).normalize();
    }

    private static byte[] readBoundedFile(Path file, String displayName) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
        if (!attrs.isRegularFile()) {
            throw new IllegalArgumentException(
                    "profile TLS config '" + displayName + "' is not a regular file");
        }
        if (attrs.size() > MAX_CONFIG_BYTES) {
            throw new IllegalArgumentException("profile TLS config '" + displayName
                    + "' exceeds the " + MAX_CONFIG_BYTES + "-byte limit");
        }
        try (InputStream in = Files.newInputStream(file)) {
            byte[] bytes = in.readNBytes(MAX_CONFIG_BYTES + 1);
            if (bytes.length > MAX_CONFIG_BYTES) {
                throw new IllegalArgumentException("profile TLS config '" + displayName
                        + "' exceeds the " + MAX_CONFIG_BYTES + "-byte limit");
            }
            return bytes;
        }
    }

    private static void requireCleanToken(String value, String field) {
        if (value.isBlank() || !value.equals(value.trim())) {
            throw new IllegalArgumentException("profile TLS config field '" + field
                    + "' must be non-blank without surrounding whitespace");
        }
    }

    private static void requireCleanPath(String value, String field) {
        requireCleanToken(value, field);
        try {
            Path.of(value);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(
                    "profile TLS config field '" + field + "' is not a valid path", e);
        }
    }
}
