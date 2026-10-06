package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentSuccessorLatePutIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real process death after a real provider write; recovery retries only public request data. */
@Testcontainers
class DocumentPublicationProcessRecoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    private static final List<String> INITIAL_TABLES=List.of("repository_execution_claims","repository_coordinator_bindings",
            "repository_publication_preparations","repository_publication_modes","repository_operations","repository_operation_owners");

    @ParameterizedTest @ValueSource(booleans={false,true})
    void killedAtInitialCommitIsRetriedByFreshJvm(boolean afterCommit,@TempDir Path temp) throws Exception {
        try(var c=context(POSTGRES);var sdk=S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(),S3.getSecretKey()))).build()) {
            new S3NamespaceProvisioner(sdk).ensureNamespace(BUCKET);
            sdk.putBucketVersioning(b -> b.bucket(BUCKET).versioningConfiguration(v -> v.status("Enabled")));
            var profile=new ManagedBackendLedger.Profile(S3BackendIdentity.of(S3.getEndpoint().toString(),S3.getRegion(),true),"late-realm");
            new ManagedBackendLedger(c.tx()).bind(GENERATION,profile);
            var input=input(c.tx(),profile);
            var command=temp.resolve("command.pb");var payload=temp.resolve("payload.pb");
            Files.write(command,input.command().intent().toByteArray());Files.write(payload,input.body().bytes());
            var writerLog=temp.resolve("initial-writer.log");
            var writer=start(c,writerLog,afterCommit?"initial-after":"initial-before",command,payload);
            try {
                long deadline=System.nanoTime()+Duration.ofSeconds(45).toNanos();boolean held=false;
                while(writer.isAlive() && System.nanoTime()<deadline) {
                    if(log(writerLog).contains("INITIAL_COMMIT_HELD")) {held=true;break;}
                    Thread.sleep(50);
                }
                assertThat(held).as(log(writerLog)).isTrue();
                assertInitialRows(c,input.command().operationId(),afterCommit);
                Object[] original=afterCommit?initialIdentity(c,input.command().operationId()):null;
                writer.destroyForcibly();assertThat(writer.waitFor(10,TimeUnit.SECONDS)).isTrue();
                assertThat(writer.exitValue()).isEqualTo(137);
                assertInitialRows(c,input.command().operationId(),afterCommit);
                if(afterCommit) assertThat(initialIdentity(c,input.command().operationId())).containsExactly(original);
                var readerLog=temp.resolve("initial-reader.log");
                var reader=start(c,readerLog,afterCommit?"recover-initial":"publish",command,payload);
                try {
                    assertThat(reader.waitFor(60,TimeUnit.SECONDS)).as("fresh retry completes: %s",readerLog).isTrue();
                    assertThat(reader.exitValue()).as(log(readerLog)).isZero();
                    assertThat(log(readerLog)).contains("PROCESS_RECOVERY_OK");
                    var current=initialIdentity(c,input.command().operationId());
                    assertThat(count(c,"document_part_attempts",input.command().operationId())).isEqualTo(1);
                    assertThat(count(c,"repository_operation_success",input.command().operationId())).isEqualTo(1);
                    assertThat(((Number)current[0]).longValue()).isEqualTo(afterCommit?2:1);
                    assertThat(((Number)current[2]).longValue()).isEqualTo(afterCommit?2:1);
                    if(afterCommit) {assertThat(current[1]).isNotEqualTo(original[1]);assertThat(current[3]).isNotEqualTo(original[3]);}
                    var object=c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                            SELECT o.object_key,o.provider_version FROM document_part_attempt_objects o
                            JOIN document_part_attempts a USING(attempt_id)
                            JOIN document_revision_parts p ON p.object_id=o.physical_object_id
                            WHERE a.operation_id=:id AND o.verified
                            """).setParameter("id",input.command().operationId()).getSingleResult());
                    assertThat((String)object[1]).isNotBlank();
                    assertThat(new S3BlobStore(sdk).getBounded(BUCKET,(String)object[0],(String)object[1],input.body().bytes().length).data())
                            .containsExactly(input.body().bytes());
                } finally {reap(reader);}
            } finally {reap(writer);}
        }
    }

    private static void assertInitialRows(DocumentNativePublicationFixture.Context c,UUID operation,boolean committed) {
        for(String table:INITIAL_TABLES) assertThat(count(c,table,operation)).as(table).isEqualTo(committed?1:0);
        for(String table:List.of("document_part_attempts","repository_publication_assessment_starts",
                "repository_operation_success","repository_operation_rejection"))
            assertThat(count(c,table,operation)).as(table).isZero();
    }
    private static long count(DocumentNativePublicationFixture.Context c,String table,UUID operation) {
        return c.tx().<Number>readOnly(em -> (Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:id")
                .setParameter("id",operation).getSingleResult()).longValue();
    }
    private static Object[] initialIdentity(DocumentNativePublicationFixture.Context c,UUID operation) {
        return c.tx().readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,o.owner_generation,o.owner_token,c.lease_until,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",operation).getSingleResult());
    }

    @ParameterizedTest @ValueSource(strings = {"none", "reserve", "install", "scoped", "scoped-revoked-grant", "scoped-revoked-key"})
    void killedWriterIsRecoveredByFreshJvm(String replacementStage, @TempDir Path temp) throws Exception {
        boolean scoped = replacementStage.startsWith("scoped");
        boolean revoked = replacementStage.startsWith("scoped-revoked-");
        try (var c = context(POSTGRES); var sdk = S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey()))).build()) {
            new S3NamespaceProvisioner(sdk).ensureNamespace(BUCKET);
            sdk.putBucketVersioning(b -> b.bucket(BUCKET).versioningConfiguration(v -> v.status("Enabled")));
            var profile = new ManagedBackendLedger.Profile(S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true), "late-realm");
            new ManagedBackendLedger(c.tx()).bind(GENERATION, profile);
            var security = ai.protomolt.proto.repo.v1.DocumentSecurity.newBuilder().addPermissions(
                    ai.protomolt.proto.repo.v1.AccessRule.newBuilder().setIdentityType("public").setIdentity("public")
                            .setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ)).build();
            var input = scoped ? input(c.tx(), profile, security) : input(c.tx(), profile);
            if (scoped) {
                var caller = DocumentPublicationProcessWorker.scopedCaller(input.command());
                new RepositoryCredentialAuthorities(c.tx()).register(DocumentPublicationProcessWorker.ADMIN,
                        caller.credentialBinding().orElseThrow(), caller.principalName());
                var grant = RepositoryCreationGrants.prepare(caller, input.command(), input.placements(),
                        (System.currentTimeMillis()+300_000)*1000);
                new RepositoryCreationGrants(c.tx(), new DriveLedger(c.tx())).install(DocumentPublicationProcessWorker.ADMIN, grant);
            }
            var command = temp.resolve("public-command.pb"); var payload = temp.resolve("public-payload.pb");
            Files.write(command, input.command().intent().toByteArray()); Files.write(payload, input.body().bytes());
            var writerLog = temp.resolve("writer.log");
            var writer = start(c, writerLog, scoped ? "scoped-write" : "write", command, payload);
            try {
                long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
                boolean held = false;
                while (writer.isAlive() && System.nanoTime() < deadline) {
                    if (log(writerLog).contains("REAL_PUT_HELD")) { held = true; break; }
                    Thread.sleep(50);
                }
                assertThat(held).as(log(writerLog)).isTrue();
                var object = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                        SELECT a.attempt_id,o.object_key,o.verified,o.provider_version
                        FROM document_part_attempts a JOIN document_part_attempt_objects o USING(attempt_id)
                        WHERE a.operation_id=:id
                        """).setParameter("id", input.command().operationId()).getSingleResult());
                assertThat(object[2]).isEqualTo(false); assertThat(object[3]).isNull();
                assertThat(new S3BlobStore(sdk).get(BUCKET, (String) object[1]).data()).containsExactly(input.body().bytes());
                List<?> oldReaders = c.tx().readOnly(em -> em.createNativeQuery(
                        "SELECT incarnation FROM repository_reader_incarnations WHERE state='ACTIVE'", UUID.class).getResultList());
                assertThat(oldReaders).isNotEmpty();
                writer.destroyForcibly();
                assertThat(writer.waitFor(10, TimeUnit.SECONDS)).isTrue();
                assertThat(writer.exitValue()).isEqualTo(137);
                assertThat(writer.isAlive()).isFalse();
                if (!scoped && !replacementStage.equals("none")) {
                    var replacementLog = temp.resolve("replacement.log");
                    var replacement = start(c, replacementLog, replacementStage, command, payload);
                    try {
                        long replacementDeadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
                        boolean replacementHeld = false;
                        while (replacement.isAlive() && System.nanoTime() < replacementDeadline) {
                            if (log(replacementLog).contains("REPLACEMENT_HELD")) { replacementHeld = true; break; }
                            Thread.sleep(50);
                        }
                        assertThat(replacementHeld).as(log(replacementLog)).isTrue();
                        replacement.destroyForcibly();
                        assertThat(replacement.waitFor(10, TimeUnit.SECONDS)).isTrue();
                        assertThat(replacement.exitValue()).isEqualTo(137);
                    } finally { reap(replacement); }
                }
                if (revoked) {
                    var caller = DocumentPublicationProcessWorker.scopedCaller(input.command());
                    if (replacementStage.equals("scoped-revoked-key")) {
                        new RepositoryCredentialAuthorities(c.tx()).revoke(DocumentPublicationProcessWorker.ADMIN,
                                caller.credentialBinding().orElseThrow(), caller.principalName());
                    } else {
                        new RepositoryCreationGrants(c.tx(), new DriveLedger(c.tx())).revoke(DocumentPublicationProcessWorker.ADMIN,
                                new RepositoryOperationLedger.Key("account", "principal", input.command().operationId()));
                    }
                }
                // No capability files or writer output are passed to this separate process.
                var readerLog = temp.resolve("reader.log");
                var reader = start(c, readerLog, scoped ? "scoped-recover" : "recover", command, payload);
                try {
                    assertThat(reader.waitFor(60, TimeUnit.SECONDS)).as("fresh reader completes: %s", readerLog).isTrue();
                    if (revoked) {
                        assertThat(reader.exitValue()).as(log(readerLog)).isNotZero();
                        assertThat(log(readerLog)).contains("ai.protomolt.proto.repo.spi.RepositoryException",
                                "RepositoryReservedPreparation.load",
                                replacementStage.equals("scoped-revoked-key")
                                        ? "Repository credential is unavailable" : "Creation grant is unavailable")
                                .doesNotContain("PROCESS_RECOVERY_OK");
                        for (String table : List.of("repository_operation_success", "repository_successor_executions",
                                "document_revision_commits")) {
                            assertThat(count(c, table, input.command().operationId())).as(table).isZero();
                        }
                        assertThat(count(c, "document_part_attempts", input.command().operationId())).isEqualTo(1);
                        assertThat(new DocumentLedger(c.tx()).findByNodeId(
                                ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(
                                        input.command().intent().getMembers(0).getDestination().getAddress()))).isEmpty();
                    } else {
                        assertThat(reader.exitValue()).as(log(readerLog)).isZero();
                        assertThat(log(readerLog)).contains("PROCESS_RECOVERY_OK");
                    }
                    if (!scoped && !replacementStage.equals("none")) {
                        var supersession = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                                SELECT s.predecessor_epoch,s.phase,c.claim_epoch,b.claim_epoch
                                FROM repository_coordinator_supersessions s
                                JOIN repository_execution_claims c USING(account_id,principal,operation_id)
                                JOIN repository_coordinator_bindings b ON
                                  (b.account_id,b.principal,b.operation_id,b.claim_epoch)=
                                  (c.account_id,c.principal,c.operation_id,c.claim_epoch)
                                WHERE s.operation_id=:id
                                """).setParameter("id", input.command().operationId()).getSingleResult());
                        assertThat(((Number) supersession[0]).longValue()).isEqualTo(2);
                        assertThat(supersession[1]).isEqualTo(replacementStage.equals("reserve") ? "RESERVED_ONLY" : "INSTALLED");
                        assertThat(((Number) supersession[2]).longValue()).isEqualTo(3);
                        assertThat(((Number) supersession[3]).longValue()).isEqualTo(3);
                        for (String table : List.of("repository_coordinator_bindings", "repository_coordinator_drains",
                                "repository_coordinator_local_drains")) {
                            assertThat(c.tx().<Number>readOnly(em -> (Number) em.createNativeQuery(
                                    "SELECT count(*) FROM " + table + " WHERE operation_id=:id AND claim_epoch=2")
                                    .setParameter("id", input.command().operationId()).getSingleResult()).longValue()).isZero();
                        }
                    }
                    var retained = c.tx().readOnly(em -> (Object[]) em.createNativeQuery(
                            "SELECT object_key,verified,provider_version FROM document_part_attempt_objects WHERE attempt_id=:id")
                            .setParameter("id", object[0]).getSingleResult());
                    assertThat(retained).containsExactly(object[1], false, null);
                    assertThat(new S3BlobStore(sdk).get(BUCKET, (String) object[1]).data()).containsExactly(input.body().bytes());
                    for (var incarnation : oldReaders) {
                        var state = c.tx().readOnly(em -> (Object[]) em.createNativeQuery(
                                "SELECT state,quiesced_at,quiescence_source FROM repository_reader_incarnations WHERE incarnation=:id")
                                .setParameter("id", incarnation).getSingleResult());
                        assertThat(state).containsExactly("ACTIVE", null, null);
                    }
                } finally { reap(reader); }
            } finally { reap(writer); }
        }
    }
    private static Process start(DocumentNativePublicationFixture.Context c, Path log, String mode, Path command, Path payload) throws Exception {
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                Objects.requireNonNull(System.getProperty("protomolt.test.runtimeClasspath")),
                DocumentPublicationProcessWorker.class.getName(), mode, command.toString(), payload.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().putAll(Map.of("TEST_DB_URL", POSTGRES.getJdbcUrl(), "TEST_DB_USER", POSTGRES.getUsername(),
                "TEST_DB_PASSWORD", POSTGRES.getPassword(), "TEST_DB_SCHEMA", c.pool().getSchema(),
                "TEST_S3_ENDPOINT", S3.getEndpoint().toString(), "TEST_S3_REGION", S3.getRegion(),
                "TEST_S3_ACCESS", S3.getAccessKey(), "TEST_S3_SECRET", S3.getSecretKey()));
        return builder.start();
    }
    private static String log(Path path) throws Exception {
        assertThat(Files.size(path)).isLessThan(1_048_576); return Files.readString(path);
    }
    private static void reap(Process child) throws Exception {
        if (child.isAlive()) { child.destroyForcibly(); assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue(); }
    }
}
