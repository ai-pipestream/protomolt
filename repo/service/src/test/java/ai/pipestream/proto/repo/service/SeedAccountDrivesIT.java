package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test of the seeded default account
 * ({@code DOCUMENT_PLATFORM_SEED_ACCOUNT_ID}) against REAL infrastructure:
 * shared testcontainers PostgreSQL 17 and LocalStack S3, with the service set
 * built through {@link RepoServiceConfig} + {@link RepoServices} and the
 * seeder invoked by hand — exactly the opt-in contract embedded hosts get.
 */
@Testcontainers(disabledWithoutDocker = true)
class SeedAccountDrivesIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static RepoServiceConfig config(String seedAccountId) {
        return new RepoServiceConfig(
                0, // unused — this IT never starts a transport
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "seed-it-docs",
                0, // HTTP upload route not exercised by this IT
                null, null, null, null, 0, 0L, // blob store: the default direct-S3 path
                true, -1L, -1L, false, true, -1L, // lifecycle/reconcile defaults
                null, null, null, // eventing off
                seedAccountId);
    }

    @Test
    void seedingCreatesIntakeAndPipelineAndIsIdempotent() {
        try (RepoServices services = RepoServices.build(config("seed-acct"))) {
            services.seedAccountDrives();

            DriveRecord intake = services.driveLedger()
                    .findByName("seed-acct", "intake").orElseThrow();
            DriveRecord pipeline = services.driveLedger()
                    .findByName("seed-acct", "pipeline").orElseThrow();
            assertThat(intake.driveType).isEqualTo("INTAKE");
            assertThat(pipeline.driveType).isEqualTo("PIPELINE");
            assertThat(intake.status).isEqualTo("ACTIVE");
            assertThat(pipeline.status).isEqualTo("ACTIVE");
            // The provisioning defaults match the CreateDrive path.
            assertThat(intake.bucket).isEqualTo("seed-it-docs-seed-acct-intake");
            assertThat(pipeline.bucket).isEqualTo("seed-it-docs-seed-acct-pipeline");
            assertThat(intake.prefix).isEqualTo("intake");
            assertThat(pipeline.prefix).isEqualTo("pipeline");

            // The buckets actually exist in LocalStack.
            services.s3Client().headBucket(b -> b.bucket(intake.bucket));
            services.s3Client().headBucket(b -> b.bucket(pipeline.bucket));

            // A second run (the reboot case) finds both drives: same
            // deterministic ids, still exactly two rows for the account.
            services.seedAccountDrives();
            assertThat(services.driveLedger().findByName("seed-acct", "intake").orElseThrow().driveId)
                    .isEqualTo(intake.driveId);
            assertThat(services.driveLedger().findByName("seed-acct", "pipeline").orElseThrow().driveId)
                    .isEqualTo(pipeline.driveId);
            assertThat(services.driveLedger().listByAccount("seed-acct", 100, null)).hasSize(2);
        }
    }

    @Test
    void noSeedAccountMeansSeedingIsANoOp() {
        try (RepoServices services = RepoServices.build(config(null))) {
            int before = services.driveLedger().listAll(1000).size();
            services.seedAccountDrives();
            assertThat(services.driveLedger().listAll(1000)).hasSize(before);
        }
    }
}
