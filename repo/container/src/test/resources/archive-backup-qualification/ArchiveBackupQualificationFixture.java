package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.service.ManagedStoragePolicy;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.ByteString;
import java.net.URI;
import java.util.Random;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Shared production composition for the archive qualification hosts. Every operation goes
 * through public repo-service APIs ({@code RepoServices}, the archive and mutation
 * repositories, the drive repository); nothing here writes catalog rows. The environment
 * variable names are the ones the document rehearsal hosts use, so the reused ledger helper
 * reads the same connection.
 */
final class ArchiveBackupQualificationFixture {
    static final String ARCHIVE = "qualification-docs";
    static final String SCRATCH_ARCHIVE = "qualification-scratch";
    static final String DRIVE = "qualification-archive";
    static final String API_TOKEN_ENV = "PROTOMOLT_REHEARSAL_API_TOKEN";
    static final String FIXTURE_LABEL = "archive-backup-qualification synthetic fixture";
    static final String SEED_MODULE = "archive-backup-qualification-seed";
    /** One hour: a lane parked this long never runs during a host's lifetime. */
    static final long PARKED_MS = 3_600_000L;
    /** The recovered host observes pending cleanup within seconds. */
    static final long FAST_MUTATION_RECOVERY_MS = 1_000L;

    private ArchiveBackupQualificationFixture() {}

    static String env(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new RepositoryBackupRehearsalFailure("Missing required environment variable " + name);
        return value;
    }

    /** The hosts run on the production JAR set plus this probe; no test framework may be present. */
    static void requireNoTestFramework() {
        for (String name : new String[] {"org.junit.jupiter.api.Test", "org.testcontainers.containers.GenericContainer",
                "org.assertj.core.api.Assertions"}) {
            try { Class.forName(name); throw new RepositoryBackupRehearsalFailure("Test framework class on the production host classpath: " + name); }
            catch (ClassNotFoundException expected) { /* production JARs only */ }
        }
    }

    /** Seed host: both archive cleanup lanes parked, so an admitted mutation stays pending across the cut. */
    static RepoServiceConfig seedConfig(String endpoint, String generation, String realm) {
        return config(endpoint, generation, realm, env("PROTOMOLT_REHEARSAL_S3_ACCESS"), env("PROTOMOLT_REHEARSAL_S3_SECRET"), PARKED_MS);
    }

    /** Recovered host: mutation recovery every second; the aged reconciliation lane stays parked. */
    static RepoServiceConfig recoveredConfig(String endpoint, String generation, String realm) {
        return config(endpoint, generation, realm, env("PROTOMOLT_REHEARSAL_S3_ACCESS"), env("PROTOMOLT_REHEARSAL_S3_SECRET"), FAST_MUTATION_RECOVERY_MS);
    }

    static RepoServiceConfig config(String endpoint, String generation, String realm, String access, String secret, long purgeIntervalMs) {
        return new RepoServiceConfig(0, new LedgerConfig(env("PROTOMOLT_REHEARSAL_JDBC"), env("PROTOMOLT_REHEARSAL_DB_USER"),
                env("PROTOMOLT_REHEARSAL_DB_PASSWORD")), endpoint, env("PROTOMOLT_REHEARSAL_S3_REGION"), access, secret,
                "qualification", 0, null, null, null, null, 0, 0L, true, purgeIntervalMs, PARKED_MS, false, true, PARKED_MS)
                .withManagedStorage(new ManagedStoragePolicy(generation, realm, true));
    }

    static RepositoryCaller operator() { return new RepositoryCaller("operator", true); }

    /** Account member without process authority: the archive doors admit only process authority. */
    static RepositoryCaller member(String account) {
        return new RepositoryCaller("member-" + account, false, Set.of(account),
                Set.of(Principal.newBuilder().setIdentityType("user").setIdentity("qualification-member-" + account).build()));
    }

    static RepositoryCaller stranger(String account) {
        return new RepositoryCaller("stranger-" + account, false, Set.of(account), Set.of());
    }

    static RepositoryCaller outsider() {
        return new RepositoryCaller("outsider", false, Set.of("qualification-other-account"), Set.of());
    }

    static EntryAddress address(String account, String archive, String entryId) {
        return EntryAddress.newBuilder().setAccountId(account).setArchive(archive).setEntryId(entryId).build();
    }

    static RenditionContent rendition(String name, String mediaType, byte[] data) {
        return RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName(name).setMediaType(mediaType))
                .setData(ByteString.copyFrom(data)).build();
    }

    /** Deterministic synthetic payload bytes; the label names the fixture, never real content. */
    static byte[] bytes(String label, int length) {
        var random = new Random(label.hashCode() * 2654435761L);
        var data = new byte[length];
        random.nextBytes(data);
        return data;
    }

    /** The logical admission of a mutation receipt: everything a physical observation may not change. */
    static ArchiveMutationReceipt logical(ArchiveMutationReceipt receipt) {
        return receipt.toBuilder().clearState().clearObjectsPending().clearObjectsConfirmedAbsent().clearErrorCode()
                .clearObservedAt().clearStatusRevision().build();
    }

    static S3Client s3(String endpoint) {
        return s3(endpoint, env("PROTOMOLT_REHEARSAL_S3_ACCESS"), env("PROTOMOLT_REHEARSAL_S3_SECRET"));
    }

    static S3Client s3(String endpoint, String access, String secret) {
        return S3Client.builder().endpointOverride(URI.create(endpoint)).region(Region.of(env("PROTOMOLT_REHEARSAL_S3_REGION")))
                .forcePathStyle(true).httpClientBuilder(UrlConnectionHttpClient.builder())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(access, secret))).build();
    }
}
