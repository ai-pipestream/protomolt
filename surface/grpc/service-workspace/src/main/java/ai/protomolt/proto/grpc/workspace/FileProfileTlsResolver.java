package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS material from PEM files the host names, constrained to exact host-approved endpoints.
 *
 * <p>A profile's {@code trust_ref} and {@code client_certificate_ref} are opaque names; this
 * resolver maps each name to files through host configuration, never through the profile. The
 * files are read every time a channel opens, so replacing them rotates the material for the
 * next call. A trust reference replaces the system roots with its bundle. Every failure is a
 * {@code credential-unavailable} refusal that names neither the files nor their contents.
 */
public final class FileProfileTlsResolver implements ProfileTlsResolver {

    private static final String KEY_BEGIN = "-----BEGIN PRIVATE KEY-----";
    private static final String KEY_END = "-----END PRIVATE KEY-----";
    private static final List<String> KEY_ALGORITHMS = List.of("EC", "RSA", "Ed25519");
    private static final char[] TRANSIENT_STORE_PASSWORD = new char[0];

    /** One approved endpoint and the exact TLS references it may present. */
    public record Binding(String profile, String endpoint, String host, int port,
                          String trustRef, String clientCertificateRef) {
        public Binding {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(endpoint, "endpoint");
            Objects.requireNonNull(host, "host");
            Objects.requireNonNull(trustRef, "trustRef");
            Objects.requireNonNull(clientCertificateRef, "clientCertificateRef");
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("binding port must be 1..65535");
            }
            if (trustRef.isBlank() && clientCertificateRef.isBlank()) {
                throw new IllegalArgumentException(
                        "a TLS binding names a trust or a client-certificate reference");
            }
        }
    }

    /**
     * A client certificate: the PEM certificate chain, leaf first, and its unencrypted PKCS#8
     * PEM private key.
     */
    public record KeyPairFiles(Path certificateChain, Path privateKey) {
        public KeyPairFiles {
            Objects.requireNonNull(certificateChain, "certificateChain");
            Objects.requireNonNull(privateKey, "privateKey");
        }
    }

    private final Set<Binding> allowed;
    private final Map<String, Path> trustBundles;
    private final Map<String, KeyPairFiles> clientCertificates;

    /**
     * @param allowed endpoints that may present TLS material, with their exact references
     * @param trustBundles trust reference to a PEM file of one or more CA certificates
     * @param clientCertificates client-certificate reference to its certificate and key files
     */
    public FileProfileTlsResolver(Set<Binding> allowed, Map<String, Path> trustBundles,
                                  Map<String, KeyPairFiles> clientCertificates) {
        this.allowed = Set.copyOf(Objects.requireNonNull(allowed, "allowed"));
        this.trustBundles = Map.copyOf(Objects.requireNonNull(trustBundles, "trustBundles"));
        this.clientCertificates =
                Map.copyOf(Objects.requireNonNull(clientCertificates, "clientCertificates"));
    }

    @Override
    public ChannelCredentials channelCredentials(ServiceProfile profile,
                                                 ServiceEndpoint endpoint)
            throws ActionException {
        if (endpoint.getTransport() != Transport.TRANSPORT_TLS) {
            throw new ActionException("unsupported-transport",
                    "trust and client-certificate references need TLS transport");
        }
        Binding actual;
        try {
            actual = new Binding(profile.getName(), endpoint.getName(), endpoint.getHost(),
                    endpoint.getPort(), endpoint.getTrustRef(),
                    endpoint.getClientCertificateRef());
        } catch (IllegalArgumentException invalid) {
            throw unavailable("profile TLS reference is unsupported");
        }
        if (!allowed.contains(actual)) {
            throw unavailable("profile endpoint is not approved for TLS material");
        }
        TlsChannelCredentials.Builder builder = TlsChannelCredentials.newBuilder();
        try {
            if (!actual.trustRef().isBlank()) {
                Path bundle = trustBundles.get(actual.trustRef());
                if (bundle == null) {
                    throw unavailable("profile trust reference is not configured");
                }
                builder.trustManager(trustManagers(bundle).getTrustManagers());
            }
            if (!actual.clientCertificateRef().isBlank()) {
                KeyPairFiles files = clientCertificates.get(actual.clientCertificateRef());
                if (files == null) {
                    throw unavailable("profile client-certificate reference is not configured");
                }
                builder.keyManager(keyManagers(files).getKeyManagers());
            }
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw unavailable("profile TLS material could not be loaded");
        }
        return builder.build();
    }

    private static TrustManagerFactory trustManagers(Path bundle)
            throws IOException, GeneralSecurityException {
        Collection<? extends Certificate> roots = certificates(bundle);
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        int index = 0;
        for (Certificate root : roots) {
            store.setCertificateEntry("trust-" + index++, root);
        }
        TrustManagerFactory factory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        return factory;
    }

    private static KeyManagerFactory keyManagers(KeyPairFiles files)
            throws IOException, GeneralSecurityException {
        List<Certificate> chain = new ArrayList<>(certificates(files.certificateChain()));
        PrivateKey key = privateKey(files.privateKey());
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("client", key, TRANSIENT_STORE_PASSWORD,
                chain.toArray(Certificate[]::new));
        KeyManagerFactory factory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(store, TRANSIENT_STORE_PASSWORD);
        return factory;
    }

    private static Collection<? extends Certificate> certificates(Path file)
            throws IOException, GeneralSecurityException {
        byte[] bytes = Files.readAllBytes(file);
        Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(new ByteArrayInputStream(bytes));
        if (certificates.isEmpty()) {
            throw new GeneralSecurityException("no certificate in file");
        }
        return certificates;
    }

    /** Reads one unencrypted PKCS#8 PEM key; the algorithm is taken from the key itself. */
    private static PrivateKey privateKey(Path file) throws IOException, GeneralSecurityException {
        String pem = Files.readString(file, StandardCharsets.US_ASCII);
        int begin = pem.indexOf(KEY_BEGIN);
        int end = pem.indexOf(KEY_END);
        if (begin < 0 || end < begin || pem.indexOf(KEY_BEGIN, begin + 1) >= 0) {
            throw new GeneralSecurityException("expected exactly one PKCS#8 private key");
        }
        byte[] der = Base64.getMimeDecoder()
                .decode(pem.substring(begin + KEY_BEGIN.length(), end));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        for (String algorithm : KEY_ALGORITHMS) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException wrongAlgorithm) {
                // Try the next algorithm; the key's own encoding decides which one parses.
            }
        }
        throw new GeneralSecurityException("unsupported private key algorithm");
    }

    private static ActionException unavailable(String message) {
        return new ActionException("credential-unavailable", message);
    }
}
