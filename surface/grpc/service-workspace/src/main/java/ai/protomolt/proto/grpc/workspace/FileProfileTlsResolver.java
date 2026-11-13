package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS material from PEM files the host names, constrained to exact host-approved endpoints.
 *
 * <p>A profile's {@code trust_ref} and {@code client_certificate_ref} are opaque names; this
 * resolver maps each name to files through host configuration, never through the profile. Every
 * PEM file is bounded to {@link #DEFAULT_MAX_PEM_BYTES} (unless overridden for testing) before
 * bytes are read or parsed. Parsed key/trust managers and {@link ChannelCredentials} are cached by
 * canonical file identity, modification timestamp, and byte length so repeated inspections and
 * RPCs reuse warm material until a referenced file changes on disk, at which point stale pooled
 * channels are evicted. A trust reference replaces the system roots with its bundle. Non-TLS
 * endpoints are refused with {@code unsupported-transport}; every unapproved reference or
 * unreadable, oversized, or malformed file is a {@code credential-unavailable} refusal whose wire
 * message names neither the files nor their contents.
 */
public final class FileProfileTlsResolver implements ProfileTlsResolver, EvictableTlsResolver {

    /** Maximum byte size accepted for a single PEM certificate bundle or PKCS#8 key file (1 MiB). */
    public static final int DEFAULT_MAX_PEM_BYTES = 1_048_576;

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

    static final class PemSizeLimitException extends IOException {
        PemSizeLimitException(int maxBytes) {
            super("PEM file exceeds maximum allowed size of " + maxBytes + " bytes");
        }
    }

    private record FileStamp(Path realPath, Object fileKey, FileTime lastModifiedTime, long size) {
    }

    private record CredentialsKey(String trustRef, String clientCertificateRef) {
    }

    private record CachedTrust(FileStamp stamp, TrustManager[] trustManagers) {
    }

    private record CachedKeyPair(FileStamp certStamp, FileStamp keyStamp, KeyManager[] keyManagers) {
    }

    private record CachedCredentials(FileStamp trustStamp, FileStamp certStamp, FileStamp keyStamp,
                                     ChannelCredentials credentials) {
    }

    private final Set<Binding> allowed;
    private final Map<String, Path> trustBundles;
    private final Map<String, KeyPairFiles> clientCertificates;
    private final int maxPemBytes;

    private final Object cacheLock = new Object();
    private final Map<Path, CachedTrust> trustCache = new HashMap<>();
    private final Map<KeyPairFiles, CachedKeyPair> keyPairCache = new HashMap<>();
    private final Map<CredentialsKey, CachedCredentials> credentialsCache = new HashMap<>();
    private final Set<ChannelFactory> evictionTargets =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    private final AtomicInteger pemReads = new AtomicInteger();
    private final AtomicInteger trustFactoryBuilds = new AtomicInteger();
    private final AtomicInteger keyFactoryBuilds = new AtomicInteger();
    private final AtomicInteger credentialsBuilds = new AtomicInteger();

    /**
     * @param allowed endpoints that may present TLS material, with their exact references
     * @param trustBundles trust reference to a PEM file of one or more CA certificates
     * @param clientCertificates client-certificate reference to its certificate and key files
     */
    public FileProfileTlsResolver(Set<Binding> allowed, Map<String, Path> trustBundles,
                                  Map<String, KeyPairFiles> clientCertificates) {
        this(allowed, trustBundles, clientCertificates, DEFAULT_MAX_PEM_BYTES);
    }

    FileProfileTlsResolver(Set<Binding> allowed, Map<String, Path> trustBundles,
                           Map<String, KeyPairFiles> clientCertificates, int maxPemBytes) {
        if (maxPemBytes <= 0) {
            throw new IllegalArgumentException("maxPemBytes must be positive");
        }
        this.allowed = Set.copyOf(Objects.requireNonNull(allowed, "allowed"));
        this.trustBundles = Map.copyOf(Objects.requireNonNull(trustBundles, "trustBundles"));
        this.clientCertificates =
                Map.copyOf(Objects.requireNonNull(clientCertificates, "clientCertificates"));
        this.maxPemBytes = maxPemBytes;
    }

    @Override
    public void registerEvictionTarget(ChannelFactory channels) {
        if (channels != null) {
            evictionTargets.add(channels);
        }
    }

    int pemReads() {
        return pemReads.get();
    }

    int trustFactoryBuilds() {
        return trustFactoryBuilds.get();
    }

    int keyFactoryBuilds() {
        return keyFactoryBuilds.get();
    }

    int credentialsBuilds() {
        return credentialsBuilds.get();
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
            throw unavailable("profile TLS reference is unsupported", invalid);
        }
        if (!allowed.contains(actual)) {
            throw unavailable("profile endpoint is not approved for TLS material");
        }
        Path bundle = null;
        if (!actual.trustRef().isBlank()) {
            bundle = trustBundles.get(actual.trustRef());
            if (bundle == null) {
                throw unavailable("profile trust reference is not configured");
            }
        }
        KeyPairFiles files = null;
        if (!actual.clientCertificateRef().isBlank()) {
            files = clientCertificates.get(actual.clientCertificateRef());
            if (files == null) {
                throw unavailable("profile client-certificate reference is not configured");
            }
        }

        CredentialsKey credentialsKey =
                new CredentialsKey(actual.trustRef(), actual.clientCertificateRef());
        ChannelCredentials evicted = null;
        ChannelCredentials resolved;
        ActionException failure = null;

        synchronized (cacheLock) {
            CachedCredentials previous = credentialsCache.get(credentialsKey);
            try {
                FileStamp trustStamp = bundle == null ? null : stampOf(bundle);
                FileStamp certStamp = files == null ? null : stampOf(files.certificateChain());
                FileStamp keyStamp = files == null ? null : stampOf(files.privateKey());

                if (previous != null
                        && Objects.equals(previous.trustStamp(), trustStamp)
                        && Objects.equals(previous.certStamp(), certStamp)
                        && Objects.equals(previous.keyStamp(), keyStamp)) {
                    return previous.credentials();
                }

                TrustManager[] trustManagers = null;
                if (bundle != null) {
                    CachedTrust cachedTrust = trustCache.get(bundle);
                    if (cachedTrust != null && cachedTrust.stamp().equals(trustStamp)) {
                        trustManagers = cachedTrust.trustManagers();
                    } else {
                        trustManagers = buildTrustManagers(readBoundedBytes(trustStamp.realPath()));
                        trustStamp = refreshStampIfChanged(bundle, trustStamp);
                        trustCache.put(bundle, new CachedTrust(trustStamp, trustManagers));
                    }
                }

                KeyManager[] keyManagers = null;
                if (files != null) {
                    CachedKeyPair cachedKeyPair = keyPairCache.get(files);
                    if (cachedKeyPair != null
                            && cachedKeyPair.certStamp().equals(certStamp)
                            && cachedKeyPair.keyStamp().equals(keyStamp)) {
                        keyManagers = cachedKeyPair.keyManagers();
                    } else {
                        byte[] certBytes = readBoundedBytes(certStamp.realPath());
                        byte[] keyBytes = readBoundedBytes(keyStamp.realPath());
                        keyManagers = buildKeyManagers(certBytes, keyBytes);
                        certStamp = refreshStampIfChanged(files.certificateChain(), certStamp);
                        keyStamp = refreshStampIfChanged(files.privateKey(), keyStamp);
                        keyPairCache.put(files, new CachedKeyPair(certStamp, keyStamp, keyManagers));
                    }
                }

                TlsChannelCredentials.Builder builder = TlsChannelCredentials.newBuilder();
                if (trustManagers != null) {
                    builder.trustManager(trustManagers);
                }
                if (keyManagers != null) {
                    builder.keyManager(keyManagers);
                }
                resolved = builder.build();
                credentialsBuilds.incrementAndGet();
                credentialsCache.put(credentialsKey,
                        new CachedCredentials(trustStamp, certStamp, keyStamp, resolved));
                if (previous != null) {
                    evicted = previous.credentials();
                }
            } catch (PemSizeLimitException oversized) {
                evicted = dropCached(credentialsKey, bundle, files, previous);
                failure = unavailable("profile TLS material could not be loaded: exceeds "
                        + maxPemBytes + "-byte limit", oversized);
                resolved = null;
            } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
                evicted = dropCached(credentialsKey, bundle, files, previous);
                failure = unavailable("profile TLS material could not be loaded", e);
                resolved = null;
            }
        }

        notifyEvicted(evicted);
        if (failure != null) {
            throw failure;
        }
        return resolved;
    }

    private ChannelCredentials dropCached(CredentialsKey credentialsKey, Path bundle,
                                          KeyPairFiles files, CachedCredentials previous) {
        credentialsCache.remove(credentialsKey);
        if (bundle != null) {
            trustCache.remove(bundle);
        }
        if (files != null) {
            keyPairCache.remove(files);
        }
        return previous == null ? null : previous.credentials();
    }

    private void notifyEvicted(ChannelCredentials evicted) {
        if (evicted == null) {
            return;
        }
        List<ChannelFactory> targets;
        synchronized (evictionTargets) {
            targets = List.copyOf(evictionTargets);
        }
        for (ChannelFactory target : targets) {
            target.evict(evicted);
        }
    }

    private FileStamp stampOf(Path file) throws IOException {
        Path real = file.toRealPath();
        BasicFileAttributes attrs = Files.readAttributes(real, BasicFileAttributes.class);
        if (!attrs.isRegularFile()) {
            throw new IOException("TLS PEM path is not a regular file");
        }
        if (attrs.size() > maxPemBytes) {
            throw new PemSizeLimitException(maxPemBytes);
        }
        return new FileStamp(real, attrs.fileKey(), attrs.lastModifiedTime(), attrs.size());
    }

    private FileStamp refreshStampIfChanged(Path file, FileStamp beforeRead) throws IOException {
        FileStamp afterRead = stampOf(file);
        return afterRead.equals(beforeRead) ? beforeRead : afterRead;
    }

    private byte[] readBoundedBytes(Path realPath) throws IOException {
        pemReads.incrementAndGet();
        try (InputStream in = Files.newInputStream(realPath)) {
            byte[] bytes = in.readNBytes(maxPemBytes + 1);
            if (bytes.length > maxPemBytes) {
                throw new PemSizeLimitException(maxPemBytes);
            }
            return bytes;
        }
    }

    private TrustManager[] buildTrustManagers(byte[] bundleBytes)
            throws IOException, GeneralSecurityException {
        Collection<? extends Certificate> roots = certificates(bundleBytes);
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        int index = 0;
        for (Certificate root : roots) {
            store.setCertificateEntry("trust-" + index++, root);
        }
        TrustManagerFactory factory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        trustFactoryBuilds.incrementAndGet();
        return factory.getTrustManagers();
    }

    private KeyManager[] buildKeyManagers(byte[] certificateChainBytes, byte[] privateKeyBytes)
            throws IOException, GeneralSecurityException {
        List<Certificate> chain = new ArrayList<>(certificates(certificateChainBytes));
        PrivateKey key = privateKey(privateKeyBytes);
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("client", key, TRANSIENT_STORE_PASSWORD,
                chain.toArray(Certificate[]::new));
        KeyManagerFactory factory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(store, TRANSIENT_STORE_PASSWORD);
        keyFactoryBuilds.incrementAndGet();
        return factory.getKeyManagers();
    }

    private static Collection<? extends Certificate> certificates(byte[] bytes)
            throws GeneralSecurityException {
        Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(new ByteArrayInputStream(bytes));
        if (certificates.isEmpty()) {
            throw new GeneralSecurityException("no certificate in file");
        }
        return certificates;
    }

    /** Parses one unencrypted PKCS#8 PEM key from bounded bytes; the key's encoding selects the algorithm. */
    private static PrivateKey privateKey(byte[] bytes) throws GeneralSecurityException {
        String pem = new String(bytes, StandardCharsets.US_ASCII);
        int begin = pem.indexOf(KEY_BEGIN);
        if (begin < 0) {
            throw new GeneralSecurityException("expected exactly one PKCS#8 private key");
        }
        int contentStart = begin + KEY_BEGIN.length();
        int end = pem.indexOf(KEY_END, contentStart);
        if (end < 0
                || pem.indexOf(KEY_BEGIN, contentStart) >= 0
                || pem.indexOf(KEY_END) != end
                || pem.indexOf(KEY_END, end + KEY_END.length()) >= 0) {
            throw new GeneralSecurityException("expected exactly one PKCS#8 private key");
        }
        byte[] der = Base64.getMimeDecoder().decode(pem.substring(contentStart, end));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        GeneralSecurityException unsupported =
                new GeneralSecurityException("unsupported private key algorithm");
        for (String algorithm : KEY_ALGORITHMS) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException wrongAlgorithm) {
                // The key's own encoding decides which algorithm parses; keep each refusal.
                unsupported.addSuppressed(wrongAlgorithm);
            }
        }
        throw unsupported;
    }

    private static ActionException unavailable(String message) {
        return new ActionException("credential-unavailable", message);
    }

    /**
     * The cause stays attached for host diagnostics; the code and message are what reach the
     * wire, and neither names a file or its contents.
     */
    private static ActionException unavailable(String message, Exception cause) {
        return new ActionException("credential-unavailable", message, null, cause);
    }
}
