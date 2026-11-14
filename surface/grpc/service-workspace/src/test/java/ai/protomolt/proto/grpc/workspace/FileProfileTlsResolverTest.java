package ai.protomolt.proto.grpc.workspace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Deterministic unit tests for {@link FileProfileTlsResolver} stamp verification across mid-read
 * and mid-bundle PEM rotations (including Kubernetes-style atomic symlink swaps).
 */
class FileProfileTlsResolverTest {

    private static final String PROFILE_NAME = "orders";
    private static final String ENDPOINT_NAME = "prod";
    private static final String HOST = "orders.internal";
    private static final int PORT = 8443;
    private static final String TRUST_REF = "tls:corp-ca";
    private static final String CLIENT_REF = "tls:orders-client";

    private static final ServiceProfile PROFILE =
            ServiceProfile.newBuilder().setName(PROFILE_NAME).build();

    @TempDir
    static Path pki;

    private static PemBundle materialOne;
    private static PemBundle materialTwo;
    private static final List<String> sensitiveKeyFragments = new ArrayList<>();

    @TempDir
    Path mountDir;

    private record PemBundle(Path caCert, Path clientCert, Path clientKey) {
    }

    @BeforeAll
    static void generateTestMaterial() throws Exception {
        materialOne = issueBundle("one");
        materialTwo = issueBundle("two");
        for (PemBundle bundle : List.of(materialOne, materialTwo)) {
            sensitiveKeyFragments.add(Files.readString(bundle.clientKey(), StandardCharsets.US_ASCII)
                    .lines()
                    .filter(line -> !line.startsWith("-----"))
                    .findFirst()
                    .orElseThrow());
        }
    }

    @Test
    void singleMidReadRotationRetriesAndCachesPostRotationMaterialUnderNewStamp() throws Exception {
        Path rev1 = populateRevision(mountDir.resolve("rev-1"), materialOne);
        Path rev2 = populateRevision(mountDir.resolve("rev-2"), materialTwo);
        Path activeLink = Files.createSymbolicLink(mountDir.resolve("current"), rev1);
        Path trustBundlePath = activeLink.resolve("ca.pem");

        AtomicBoolean rotatedOnRead = new AtomicBoolean();
        FileProfileTlsResolver.PemFileOps swapOnceDuringRead =
                new FileProfileTlsResolver.PemFileOps() {
                    @Override
                    public FileProfileTlsResolver.FileStamp stamp(Path file, int maxPemBytes)
                            throws IOException {
                        return FileProfileTlsResolver.DEFAULT_FILE_OPS.stamp(file, maxPemBytes);
                    }

                    @Override
                    public byte[] readBounded(Path realPath, int maxPemBytes) throws IOException {
                        byte[] bytes =
                                FileProfileTlsResolver.DEFAULT_FILE_OPS.readBounded(
                                        realPath, maxPemBytes);
                        if (rotatedOnRead.compareAndSet(false, true)) {
                            atomicFlipSymlink(activeLink, rev2);
                        }
                        return bytes;
                    }
                };

        FileProfileTlsResolver resolver = new FileProfileTlsResolver(
                Set.of(new FileProfileTlsResolver.Binding(
                        PROFILE_NAME, ENDPOINT_NAME, HOST, PORT, TRUST_REF, "")),
                Map.of(TRUST_REF, trustBundlePath),
                Map.of(),
                FileProfileTlsResolver.DEFAULT_MAX_PEM_BYTES,
                swapOnceDuringRead);
        ServiceEndpoint endpoint = endpoint(TRUST_REF, "");

        ChannelCredentials first = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(rotatedOnRead.get()).isTrue();
        // Read 1 (rev-1) was discarded because post-read stat observed rev-2; read 2 loaded rev-2.
        assertThat(resolver.pemReads()).isEqualTo(2);
        assertThat(resolver.trustFactoryBuilds()).isEqualTo(1);
        assertThat(resolver.credentialsBuilds()).isEqualTo(1);
        assertThat(acceptedIssuerCommonNames(first)).containsExactly("CN=ca-two");

        // Second call hits the warm cache under rev-2's FileStamp with zero additional PEM reads.
        ChannelCredentials second = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(second).isSameAs(first);
        assertThat(resolver.pemReads()).isEqualTo(2);
        assertThat(resolver.credentialsBuilds()).isEqualTo(1);
        assertThat(acceptedIssuerCommonNames(second)).containsExactly("CN=ca-two");
    }

    @Test
    void midBundleSymlinkSwapBetweenClientCertAndPrivateKeyRetriesAndCachesNewPair()
            throws Exception {
        Path rev1 = populateRevision(mountDir.resolve("rev-1"), materialOne);
        Path rev2 = populateRevision(mountDir.resolve("rev-2"), materialTwo);
        Path activeLink = Files.createSymbolicLink(mountDir.resolve("current"), rev1);
        Path trustPath = activeLink.resolve("ca.pem");
        Path certPath = activeLink.resolve("client.pem");
        Path keyPath = activeLink.resolve("client.key");

        AtomicBoolean swappedBeforeKey = new AtomicBoolean();
        FileProfileTlsResolver.PemFileOps swapBetweenCertAndKey =
                new FileProfileTlsResolver.PemFileOps() {
                    @Override
                    public FileProfileTlsResolver.FileStamp stamp(Path file, int maxPemBytes)
                            throws IOException {
                        if (file.equals(keyPath)
                                && resolverHasReadCertOnce()
                                && swappedBeforeKey.compareAndSet(false, true)) {
                            atomicFlipSymlink(activeLink, rev2);
                        }
                        return FileProfileTlsResolver.DEFAULT_FILE_OPS.stamp(file, maxPemBytes);
                    }

                    private final AtomicInteger certReads = new AtomicInteger();

                    private boolean resolverHasReadCertOnce() {
                        return certReads.get() == 1;
                    }

                    @Override
                    public byte[] readBounded(Path realPath, int maxPemBytes) throws IOException {
                        byte[] bytes =
                                FileProfileTlsResolver.DEFAULT_FILE_OPS.readBounded(
                                        realPath, maxPemBytes);
                        if (realPath.getFileName().toString().equals("client.pem")) {
                            certReads.incrementAndGet();
                        }
                        return bytes;
                    }
                };

        FileProfileTlsResolver resolver = new FileProfileTlsResolver(
                Set.of(new FileProfileTlsResolver.Binding(
                        PROFILE_NAME, ENDPOINT_NAME, HOST, PORT, TRUST_REF, CLIENT_REF)),
                Map.of(TRUST_REF, trustPath),
                Map.of(CLIENT_REF, new FileProfileTlsResolver.KeyPairFiles(certPath, keyPath)),
                FileProfileTlsResolver.DEFAULT_MAX_PEM_BYTES,
                swapBetweenCertAndKey);
        ServiceEndpoint endpoint = endpoint(TRUST_REF, CLIENT_REF);

        ChannelCredentials resolved = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(swappedBeforeKey.get()).isTrue();
        assertThat(acceptedIssuerCommonNames(resolved)).containsExactly("CN=ca-two");
        assertThat(clientLeafCommonName(resolved)).isEqualTo("CN=client-two");

        int readsAfterFirst = resolver.pemReads();
        ChannelCredentials cached = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(cached).isSameAs(resolved);
        assertThat(resolver.pemReads()).isEqualTo(readsAfterFirst);
    }

    @Test
    void persistentMidReadRotationFailsClosedEvictsWarmCacheAndRecoversOnceStable()
            throws Exception {
        List<Path> revisions = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            PemBundle source = (i % 2 == 0) ? materialOne : materialTwo;
            revisions.add(populateRevision(mountDir.resolve("rev-" + i), source));
        }
        Path activeLink = Files.createSymbolicLink(mountDir.resolve("current"), revisions.get(0));
        Path trustPath = activeLink.resolve("ca.pem");
        Path certPath = activeLink.resolve("client.pem");
        Path keyPath = activeLink.resolve("client.key");

        AtomicBoolean churnDuringRead = new AtomicBoolean(false);
        AtomicInteger revisionCursor = new AtomicInteger(0);
        FileProfileTlsResolver.PemFileOps churningFileOps =
                new FileProfileTlsResolver.PemFileOps() {
                    @Override
                    public FileProfileTlsResolver.FileStamp stamp(Path file, int maxPemBytes)
                            throws IOException {
                        return FileProfileTlsResolver.DEFAULT_FILE_OPS.stamp(file, maxPemBytes);
                    }

                    @Override
                    public byte[] readBounded(Path realPath, int maxPemBytes) throws IOException {
                        byte[] bytes =
                                FileProfileTlsResolver.DEFAULT_FILE_OPS.readBounded(
                                        realPath, maxPemBytes);
                        if (churnDuringRead.get()) {
                            int next = revisionCursor.incrementAndGet();
                            atomicFlipSymlink(activeLink, revisions.get(next % revisions.size()));
                        }
                        return bytes;
                    }
                };

        FileProfileTlsResolver resolver = new FileProfileTlsResolver(
                Set.of(new FileProfileTlsResolver.Binding(
                        PROFILE_NAME, ENDPOINT_NAME, HOST, PORT, TRUST_REF, CLIENT_REF)),
                Map.of(TRUST_REF, trustPath),
                Map.of(CLIENT_REF, new FileProfileTlsResolver.KeyPairFiles(certPath, keyPath)),
                FileProfileTlsResolver.DEFAULT_MAX_PEM_BYTES,
                churningFileOps);
        List<ChannelCredentials> evicted = new ArrayList<>();
        resolver.registerEvictionTarget(new ChannelFactory() {
            @Override
            public ManagedChannel open(String target, boolean tls) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void evict(ChannelCredentials credentials) {
                evicted.add(credentials);
            }
        });
        ServiceEndpoint endpoint = endpoint(TRUST_REF, CLIENT_REF);

        // 1. Warm the cache with rev-0 (materialOne).
        ChannelCredentials initial = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(clientLeafCommonName(initial)).isEqualTo("CN=client-one");
        assertThat(resolver.pemReads()).isEqualTo(3);

        // 2. Advance symlink to rev-1 and enable continuous mid-read rotation across all retries.
        revisionCursor.set(1);
        atomicFlipSymlink(activeLink, revisions.get(1));
        churnDuringRead.set(true);

        assertThatThrownBy(() -> resolver.channelCredentials(PROFILE, endpoint))
                .isInstanceOfSatisfying(ActionException.class, refused -> {
                    assertThat(refused.code())
                            .isEqualTo(FileProfileTlsResolver.CODE_CREDENTIAL_UNAVAILABLE);
                    assertThat(refused.getMessage())
                            .isEqualTo("TLS PEM file changed concurrently during read for profile '"
                                    + PROFILE_NAME + "'");
                    assertThat(refused.getCause())
                            .isInstanceOf(
                                    FileProfileTlsResolver.ConcurrentPemChangeException.class);
                    assertNoPathOrPemLeak(refused.getMessage());
                    assertNoPathOrPemLeak(refused.getCause().getMessage());
                });

        // Bounded retry cap respected (3 attempts on the first re-read file), warm cache evicted.
        assertThat(resolver.pemReads())
                .isEqualTo(3 + FileProfileTlsResolver.MAX_STABLE_READ_ATTEMPTS);
        assertThat(evicted).containsExactly(initial);

        // 3. Stop churning on rev-3 (materialTwo); subsequent read recovers on an unpoisoned cache.
        churnDuringRead.set(false);
        atomicFlipSymlink(activeLink, revisions.get(3));

        ChannelCredentials recovered = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(acceptedIssuerCommonNames(recovered)).containsExactly("CN=ca-two");
        assertThat(clientLeafCommonName(recovered)).isEqualTo("CN=client-two");

        int readsAfterRecovery = resolver.pemReads();
        assertThat(resolver.channelCredentials(PROFILE, endpoint)).isSameAs(recovered);
        assertThat(resolver.pemReads()).isEqualTo(readsAfterRecovery);
    }

    @Test
    void persistentMidBundleRotationFailsClosedAndLeavesCacheUnpoisoned() throws Exception {
        List<Path> revisions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            PemBundle source = (i % 2 == 0) ? materialOne : materialTwo;
            revisions.add(populateRevision(mountDir.resolve("bundle-rev-" + i), source));
        }
        Path activeLink = Files.createSymbolicLink(mountDir.resolve("current"), revisions.get(0));
        Path certPath = activeLink.resolve("client.pem");
        Path keyPath = activeLink.resolve("client.key");

        AtomicBoolean churnBetweenFiles = new AtomicBoolean(true);
        AtomicInteger certReadsCompleted = new AtomicInteger();
        AtomicInteger flippedCount = new AtomicInteger();
        FileProfileTlsResolver.PemFileOps midBundleChurnOps =
                new FileProfileTlsResolver.PemFileOps() {
                    @Override
                    public FileProfileTlsResolver.FileStamp stamp(Path file, int maxPemBytes)
                            throws IOException {
                        if (churnBetweenFiles.get()
                                && file.equals(keyPath)
                                && flippedCount.get() < certReadsCompleted.get()) {
                            int next = flippedCount.incrementAndGet();
                            atomicFlipSymlink(activeLink, revisions.get(next % revisions.size()));
                        }
                        return FileProfileTlsResolver.DEFAULT_FILE_OPS.stamp(file, maxPemBytes);
                    }

                    @Override
                    public byte[] readBounded(Path realPath, int maxPemBytes) throws IOException {
                        byte[] bytes =
                                FileProfileTlsResolver.DEFAULT_FILE_OPS.readBounded(
                                        realPath, maxPemBytes);
                        if (realPath.getFileName().toString().equals("client.pem")) {
                            certReadsCompleted.incrementAndGet();
                        }
                        return bytes;
                    }
                };

        FileProfileTlsResolver resolver = new FileProfileTlsResolver(
                Set.of(new FileProfileTlsResolver.Binding(
                        PROFILE_NAME, ENDPOINT_NAME, HOST, PORT, "", CLIENT_REF)),
                Map.of(),
                Map.of(CLIENT_REF, new FileProfileTlsResolver.KeyPairFiles(certPath, keyPath)),
                FileProfileTlsResolver.DEFAULT_MAX_PEM_BYTES,
                midBundleChurnOps);
        ServiceEndpoint endpoint = endpoint("", CLIENT_REF);

        assertThatThrownBy(() -> resolver.channelCredentials(PROFILE, endpoint))
                .isInstanceOfSatisfying(ActionException.class, refused -> {
                    assertThat(refused.code())
                            .isEqualTo(FileProfileTlsResolver.CODE_CREDENTIAL_UNAVAILABLE);
                    assertThat(refused.getMessage())
                            .isEqualTo("TLS PEM file changed concurrently during read for profile '"
                                    + PROFILE_NAME + "'");
                    assertNoPathOrPemLeak(refused.getMessage());
                });

        churnBetweenFiles.set(false);
        atomicFlipSymlink(activeLink, revisions.get(1));

        ChannelCredentials recovered = resolver.channelCredentials(PROFILE, endpoint);
        assertThat(clientLeafCommonName(recovered)).isEqualTo("CN=client-two");
    }

    @Test
    void symlinkTargetUnlinkedMidReadRetriesAgainstNewRevisionInsteadOfFailing() throws Exception {
        Path rev1 = populateRevision(mountDir.resolve("unlink-rev-1"), materialOne);
        Path rev2 = populateRevision(mountDir.resolve("unlink-rev-2"), materialTwo);
        Path activeLink = Files.createSymbolicLink(mountDir.resolve("current"), rev1);
        Path trustPath = activeLink.resolve("ca.pem");

        AtomicBoolean unlinkedOldTarget = new AtomicBoolean();
        FileProfileTlsResolver.PemFileOps unlinkOldRevisionOnOpen =
                new FileProfileTlsResolver.PemFileOps() {
                    @Override
                    public FileProfileTlsResolver.FileStamp stamp(Path file, int maxPemBytes)
                            throws IOException {
                        return FileProfileTlsResolver.DEFAULT_FILE_OPS.stamp(file, maxPemBytes);
                    }

                    @Override
                    public byte[] readBounded(Path realPath, int maxPemBytes) throws IOException {
                        if (unlinkedOldTarget.compareAndSet(false, true)) {
                            atomicFlipSymlink(activeLink, rev2);
                            Files.delete(realPath);
                            throw new NoSuchFileException(realPath.toString());
                        }
                        return FileProfileTlsResolver.DEFAULT_FILE_OPS.readBounded(
                                realPath, maxPemBytes);
                    }
                };

        FileProfileTlsResolver resolver = new FileProfileTlsResolver(
                Set.of(new FileProfileTlsResolver.Binding(
                        PROFILE_NAME, ENDPOINT_NAME, HOST, PORT, TRUST_REF, "")),
                Map.of(TRUST_REF, trustPath),
                Map.of(),
                FileProfileTlsResolver.DEFAULT_MAX_PEM_BYTES,
                unlinkOldRevisionOnOpen);

        ChannelCredentials resolved = resolver.channelCredentials(PROFILE, endpoint(TRUST_REF, ""));
        assertThat(unlinkedOldTarget.get()).isTrue();
        assertThat(acceptedIssuerCommonNames(resolved)).containsExactly("CN=ca-two");
    }

    private void assertNoPathOrPemLeak(String text) {
        assertThat(text)
                .doesNotContain(mountDir.toString())
                .doesNotContain(pki.toString())
                .doesNotContain("ca.pem")
                .doesNotContain("client.pem")
                .doesNotContain("client.key")
                .doesNotContain("PRIVATE KEY")
                .doesNotContain("BEGIN CERTIFICATE");
        for (String fragment : sensitiveKeyFragments) {
            assertThat(text).doesNotContain(fragment);
        }
    }

    private static ServiceEndpoint endpoint(String trustRef, String clientCertRef) {
        ServiceEndpoint.Builder builder = ServiceEndpoint.newBuilder()
                .setName(ENDPOINT_NAME)
                .setHost(HOST)
                .setPort(PORT)
                .setTransport(Transport.TRANSPORT_TLS);
        if (!trustRef.isBlank()) {
            builder.setTrustRef(trustRef);
        }
        if (!clientCertRef.isBlank()) {
            builder.setClientCertificateRef(clientCertRef);
        }
        return builder.build();
    }

    private static Path populateRevision(Path revDir, PemBundle source) throws IOException {
        Files.createDirectories(revDir);
        Files.copy(source.caCert(), revDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(source.clientCert(), revDir.resolve("client.pem"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(source.clientKey(), revDir.resolve("client.key"),
                StandardCopyOption.REPLACE_EXISTING);
        return revDir;
    }

    private static void atomicFlipSymlink(Path link, Path newTarget) throws IOException {
        Path staging = link.resolveSibling(link.getFileName() + ".next");
        Files.deleteIfExists(staging);
        Files.createSymbolicLink(staging, newTarget);
        Files.move(staging, link,
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static List<String> acceptedIssuerCommonNames(ChannelCredentials credentials) {
        TlsChannelCredentials tls = (TlsChannelCredentials) credentials;
        X509TrustManager trustManager = (X509TrustManager) tls.getTrustManagers().get(0);
        List<String> names = new ArrayList<>();
        for (X509Certificate cert : trustManager.getAcceptedIssuers()) {
            names.add(cert.getSubjectX500Principal().getName());
        }
        return names;
    }

    private static String clientLeafCommonName(ChannelCredentials credentials) {
        TlsChannelCredentials tls = (TlsChannelCredentials) credentials;
        X509KeyManager keyManager = (X509KeyManager) tls.getKeyManagers().get(0);
        X509Certificate[] chain = keyManager.getCertificateChain("client");
        PrivateKey privateKey = keyManager.getPrivateKey("client");
        assertThat(chain).isNotNull().isNotEmpty();
        assertThat(privateKey).isNotNull();
        return chain[0].getSubjectX500Principal().getName();
    }

    private static PemBundle issueBundle(String suffix) throws Exception {
        String password = "test-only-password";
        Path bundleDir = Files.createDirectories(pki.resolve("bundle-" + suffix));
        Path caStore = bundleDir.resolve("ca.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=ca-" + suffix, "-ext", "bc:c", "-validity", "2",
                "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        Path caCert = bundleDir.resolve("ca.pem");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-file", caCert.toString(),
                "-keystore", caStore.toString(), "-storepass", password);

        Path leafStore = bundleDir.resolve("client.p12");
        keytool("-genkeypair", "-alias", "client", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=client-" + suffix, "-validity", "2",
                "-keystore", leafStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        Path csr = bundleDir.resolve("client.csr");
        keytool("-certreq", "-alias", "client", "-file", csr.toString(),
                "-keystore", leafStore.toString(), "-storepass", password);
        Path clientCert = bundleDir.resolve("client.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", csr.toString(),
                "-outfile", clientCert.toString(), "-validity", "2",
                "-keystore", caStore.toString(), "-storepass", password);

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(leafStore)) {
            ks.load(in, password.toCharArray());
        }
        PrivateKey key = (PrivateKey) ks.getKey("client", password.toCharArray());
        Path clientKey = bundleDir.resolve("client.key");
        Files.writeString(clientKey, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n", StandardCharsets.US_ASCII);
        return new PemBundle(caCert, clientCert, clientKey);
    }

    private static void keytool(String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.add("-noprompt");
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
    }
}
