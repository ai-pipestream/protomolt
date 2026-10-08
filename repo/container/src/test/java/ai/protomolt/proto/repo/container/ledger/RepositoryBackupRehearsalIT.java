package ai.protomolt.proto.repo.container.ledger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Isolated repository backup and recovery rehearsal over pinned PostgreSQL 18 and RustFS.
 *
 * <p>Two complete clean-room rehearsals seed a repository through production-JAR hosts, take a
 * QUIESCED backup (every writer exited, no client backend, no active reader incarnation), make the
 * source unreachable, restore into fresh stores and verify in a fresh production-JAR host with no
 * schema registry. Negative cases then restore disposable copies and prove explicit refusals.
 * Evidence is kept under {@code build/backup-rehearsal/<timestamp>/} on success and failure.
 *
 * <p>Run the ordinary {@code test} task with {@code -I backup-recovery/rehearsal.init.gradle}, which
 * supplies the admission bundle and production host classpath; without it this class is disabled
 * with an explicit reason rather than passing vacuously.
 */
@EnabledIfSystemProperty(named = "protomolt.test.admissionRuntimeBundle", matches = ".+",
        disabledReason = "Needs the admission transport runtime bundle: run with -I repo/container/src/test/resources/backup-recovery/rehearsal.init.gradle")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RepositoryBackupRehearsalIT {
    static final Path ROOT = Path.of("build", "backup-rehearsal", Instant.now().toString().replace(':', '-')).toAbsolutePath();
    private RepositoryBackupRehearsalProbeCompiler.Compiled compiled;
    private final Map<String, Object> summary = new LinkedHashMap<>();
    private final Map<String, Object> outcomes = new LinkedHashMap<>();
    private Path firstBackup, secondBackup;

    @BeforeAll void compileHosts() throws Exception {
        Files.createDirectories(ROOT);
        compiled = RepositoryBackupRehearsalProbeCompiler.compile(Files.createDirectories(ROOT.resolve("hosts")));
        Files.writeString(ROOT.resolve("runtime-inventory.tsv"), RepositoryBackupRehearsalProbeCompiler.runtimeInventory(compiled));
        Files.writeString(ROOT.resolve("host-sources.tsv"), String.join("\n", compiled.sourceDigests()) + "\n");
        // Bind the evidence to the exact driver sources and init script that ran, not only to the host sources.
        var driverSources = new StringBuilder();
        try (var paths = Files.list(Path.of("src/test/java/ai/protomolt/proto/repo/container/ledger"))) {
            for (var path : paths.filter(path -> path.getFileName().toString().startsWith("RepositoryBackupRehearsal")).sorted().toList())
                driverSources.append(path.getFileName()).append('\t').append(RepositoryBackupRehearsalBackupSet.sha256(path)).append('\n');
        }
        var init = Path.of("src/test/resources/backup-recovery/rehearsal.init.gradle");
        driverSources.append(init.getFileName()).append('\t').append(RepositoryBackupRehearsalBackupSet.sha256(init)).append('\n');
        Files.writeString(ROOT.resolve("driver-sources.tsv"), driverSources);
        var shell = new RepositoryBackupRehearsalShell(ROOT.resolve("environment.log"), List.of());
        var stores = new RepositoryBackupRehearsalStores(shell, "protomolt-rehearsal-env");
        stores.requireDocker();
        var environment = new StringBuilder();
        environment.append("java.version=").append(System.getProperty("java.version")).append('\n');
        environment.append("java.vm.name=").append(System.getProperty("java.vm.name")).append('\n');
        environment.append("os=").append(System.getProperty("os.name")).append(' ').append(System.getProperty("os.version")).append(' ').append(System.getProperty("os.arch")).append('\n');
        environment.append("docker.server=").append(shell.run(List.of("docker", "version", "--format", "{{.Server.Version}}")).stdout().strip()).append('\n');
        environment.append("postgres.image=").append(RepositoryBackupRehearsalStores.POSTGRES_IMAGE).append(' ').append(stores.imageId(RepositoryBackupRehearsalStores.POSTGRES_IMAGE)).append('\n');
        environment.append("rustfs.image=").append(RepositoryBackupRehearsalStores.RUSTFS_IMAGE).append(' ').append(stores.imageId(RepositoryBackupRehearsalStores.RUSTFS_IMAGE)).append('\n');
        environment.append("git.head=").append(shell.run(List.of("git", "rev-parse", "HEAD")).stdout().strip()).append('\n');
        environment.append("git.status=").append(shell.run(List.of("git", "status", "--short")).stdout().strip().replace('\n', ';')).append('\n');
        environment.append("load.average=").append(shell.run(List.of("cat", "/proc/loadavg")).stdout().strip()).append('\n');
        environment.append("bundle=").append(compiled.bundle()).append('\n');
        Files.writeString(ROOT.resolve("environment.txt"), environment);
        summary.put("startedAt", Instant.now().toString());
        summary.put("root", ROOT.toString());
        System.out.println("REPOSITORY_BACKUP_REHEARSAL_EVIDENCE " + ROOT);
    }

    @AfterAll void writeSummary() {
        summary.put("outcomes", outcomes);
        summary.put("finishedAt", Instant.now().toString());
        RepositoryBackupRehearsalBackupSet.writeJson(ROOT.resolve("summary.json"), summary);
    }

    // ---- positive rehearsals -------------------------------------------------------------------

    @Test @Order(1) void rehearsalRunOne() throws Exception { firstBackup = positiveRun("run-1"); }

    @Test @Order(2) void rehearsalRunTwo() throws Exception { secondBackup = positiveRun("run-2"); }

    // ---- negative rehearsals (each restores a disposable copy of run-1's sealed backup) ----------

    @Test @Order(3) void preflightRefusesDamagedSets() throws Exception {
        requirePositive();
        var run = open("negative-preflight");
        try {
            var before = RepositoryBackupRehearsalBackupSet.fingerprint(firstBackup);
            run.stores().requireDocker();
            var missing = copy(run, "missing-component");
            Files.delete(missing.resolve("provider/rustfs-data.tar"));
            expectRefusal(run, missing, "missing", "missing-component.refused_before_resources");
            var corrupt = copy(run, "corrupt-checksum");
            var dump = corrupt.resolve("sql/ledger.dump");
            var bytes = Files.readAllBytes(dump); bytes[bytes.length / 2] ^= 0x01; Files.write(dump, bytes);
            expectRefusal(run, corrupt, "checksum differs from the manifest", "corrupt-checksum.refused_before_resources");
            var unsealed = copy(run, "unsealed");
            Files.delete(unsealed.resolve("SEALED"));
            expectRefusal(run, unsealed, "not sealed", "unsealed.refused_before_resources");
            var mixed = copy(run, "mixed-captures");
            Files.copy(secondBackup.resolve("provider/rustfs-data.tar"), mixed.resolve("provider/rustfs-data.tar"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            // The other capture's archive differs in size before its digest is even compared.
            expectRefusal(run, mixed, "differs from the manifest", "mixed-captures.refused_before_resources");
            var nonempty = copy(run, "nonempty-target");
            var target = run.directory().resolve("restore-nonempty");
            Files.createDirectories(target); Files.writeString(target.resolve("existing.txt"), "previous restore state\n");
            assertThatThrownBy(() -> restore(run, target, nonempty)).hasMessageContaining("not empty");
            run.log().pass("nonempty-target.directory_refused", "restore into a nonempty directory refused; no resources created=" + run.stores().created().isEmpty());
            assertThat(Files.readString(target.resolve("existing.txt"))).isEqualTo("previous restore state\n");
            run.stores().createNetwork();
            var volume = run.stores().createVolume("occupied");
            run.shell().run(List.of("docker", "run", "--rm", "-v", volume + ":/data", RepositoryBackupRehearsalStores.POSTGRES_IMAGE, "sh", "-c", "echo occupied > /data/marker")).require("occupy");
            assertThat(run.stores().volumeEmpty(volume)).isFalse();
            run.log().pass("nonempty-target.volume_refused", "a nonempty volume is reported occupied and is never extracted into");
            assertThat(RepositoryBackupRehearsalBackupSet.fingerprint(firstBackup)).isEqualTo(before);
            run.log().pass("preflight.original_backup_untouched", "run-1 backup set unchanged");
            outcomes.put(run.id(), Map.of("passed", true));
        } catch (Throwable failure) { outcomes.put(run.id(), Map.of("passed", false, "failure", failure.toString())); throw failure; }
        finally { finish(run); }
    }

    @Test @Order(4) void inconsistentCatalogAndObjectSnapshotRefuses() throws Exception {
        requirePositive();
        negativeRestore("inconsistent-snapshot", (run, copy, identities) -> {
            // Someone "repairs" a set assembled from two captures by resealing it: preflight passes, the host must still refuse.
            Files.copy(secondBackup.resolve("provider/rustfs-data.tar"), copy.resolve("provider/rustfs-data.tar"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            var manifest = RepositoryBackupRehearsalBackupSet.readJson(RepositoryBackupRehearsalBackupSet.manifest(copy));
            var identity = new LinkedHashMap<String, Object>();
            for (String key : List.of("images", "backend", "database", "sequences", "rowCounts", "stopped", "catalog")) identity.put(key, manifest.get(key));
            RepositoryBackupRehearsalBackupSet.seal(copy, identity);
            run.shell().note("inconsistent-snapshot: run-2 provider archive substituted into run-1's set and resealed");
            run.log().pass("inconsistent-snapshot.injected", "catalog from run-1, provider volume from run-2, manifest resealed");
        }, null);
    }

    @Test @Order(5) void missingProviderVersionRefusesWithoutSubstitution() throws Exception {
        requirePositive();
        negativeRestore("missing-version", null, (run, restored, record) -> {
            var objects = RepositoryBackupRehearsalBackupSet.list(record, "documentObjects");
            var first = RepositoryBackupRehearsalBackupSet.list(record, "revisions").getFirst();
            var core = objects.stream().filter(object -> RepositoryBackupRehearsalBackupSet.number(object, "part") == 1
                    && RepositoryBackupRehearsalBackupSet.string(object, "revisionId").equals(RepositoryBackupRehearsalBackupSet.string(first, "revisionId"))).findFirst().orElseThrow();
            try (var client = s3(restored.provider())) {
                String bucket = RepositoryBackupRehearsalBackupSet.string(core, "namespace"), key = RepositoryBackupRehearsalBackupSet.string(core, "key");
                var newer = client.putObject(builder -> builder.bucket(bucket).key(key),
                        software.amazon.awssdk.core.sync.RequestBody.fromBytes("newer provider bytes must not replace the archived version".getBytes(StandardCharsets.UTF_8)));
                assertThat(newer.versionId()).isNotEqualTo(RepositoryBackupRehearsalBackupSet.string(core, "providerVersion"));
                client.deleteObject(builder -> builder.bucket(bucket).key(key).versionId(RepositoryBackupRehearsalBackupSet.string(core, "providerVersion")));
                run.shell().note("missing-version: PUT newer version " + newer.versionId() + " then DeleteObject versionId=" + core.get("providerVersion") + " of " + bucket + "/" + key);
            }
            record.put("injectedMissingVersion", core);
            run.log().pass("missing-version.injected", "recorded version " + core.get("providerVersion") + " of revision " + first.get("tag") + "'s CORE deleted on the disposable restored provider; a newer latest version exists");
        });
    }

    @Test @Order(6) void corruptRetainedDescriptorRefusesWithoutLiveSubstitution() throws Exception {
        requirePositive();
        negativeRestore("corrupt-descriptor", null, (run, restored, record) -> {
            String artifact = attachmentArtifact(record);
            try (var catalog = catalog(restored.postgres())) {
                var constraint = new String[1];
                catalog.query("SELECT conname FROM pg_constraint WHERE conrelid = 'repository_schema_artifacts'::regclass AND contype = 'c' "
                        + "AND pg_get_constraintdef(oid) LIKE '%sha256(artifact_bytes)%'", List.of(), result -> constraint[0] = result.getString(1));
                assertThat(constraint[0]).isNotNull();
                // Explicit isolated catalog corruption outside the immutable writer: the digest check and immutability guard are
                // removed on this disposable copy only, so the retained bytes no longer match their recorded identity.
                catalog.execute("ALTER TABLE repository_schema_artifacts DROP CONSTRAINT " + constraint[0]);
                catalog.execute("ALTER TABLE repository_schema_artifacts DISABLE TRIGGER ALL");
                catalog.execute("UPDATE repository_schema_artifacts SET artifact_bytes = artifact_bytes || '\\x00'::bytea WHERE encode(artifact_sha256,'hex') = '" + artifact + "'");
                catalog.execute("ALTER TABLE repository_schema_artifacts ENABLE TRIGGER ALL");
                var sizes = new ArrayList<Long>();
                catalog.query("SELECT octet_length(artifact_bytes) FROM repository_schema_artifacts WHERE encode(artifact_sha256,'hex') = '" + artifact + "'", List.of(), result -> sizes.add(result.getLong(1)));
                assertThat(sizes).hasSize(2);
                run.shell().note("corrupt-descriptor: artifact " + artifact + " bytes damaged in both accounts' catalog rows (digest check dropped on the disposable copy)");
            }
            record.put("injectedArtifact", artifact);
            run.log().pass("corrupt-descriptor.injected", "retained attachment descriptor " + artifact + " damaged on the disposable restored ledger");
        });
    }

    @Test @Order(7) void missingRetainedDescriptorRefusesWithoutLiveSubstitution() throws Exception {
        requirePositive();
        negativeRestore("missing-descriptor", null, (run, restored, record) -> {
            String artifact = attachmentArtifact(record);
            try (var catalog = catalog(restored.postgres())) {
                catalog.execute("ALTER TABLE repository_schema_artifacts DISABLE TRIGGER ALL");
                catalog.execute("DELETE FROM repository_schema_artifacts WHERE encode(artifact_sha256,'hex') = '" + artifact + "'");
                catalog.execute("ALTER TABLE repository_schema_artifacts ENABLE TRIGGER ALL");
                var remaining = new ArrayList<Long>();
                catalog.query("SELECT count(*) FROM repository_schema_artifacts WHERE encode(artifact_sha256,'hex') = '" + artifact + "'", List.of(), result -> remaining.add(result.getLong(1)));
                assertThat(remaining).containsExactly(0L);
                run.shell().note("missing-descriptor: artifact " + artifact + " rows deleted with referential and immutability triggers disabled on the disposable copy");
            }
            record.put("injectedArtifact", artifact);
            run.log().pass("missing-descriptor.injected", "retained attachment descriptor " + artifact + " absent from the disposable restored ledger; revision references still point at it");
        });
    }

    @Test @Order(8) void wrongEndpointUnderRecordedGenerationRefuses() throws Exception {
        requirePositive();
        negativeRestore("wrong-endpoint", null, null);
    }

    @Test @Order(9) void newGenerationAtRecordedEndpointRefusesHistory() throws Exception {
        requirePositive();
        negativeRestore("new-generation", null, null);
    }

    @Test @Order(10) void wrongProviderCredentialsRefuse() throws Exception {
        requirePositive();
        negativeRestore("wrong-credentials", null, null);
    }

    @Test @Order(11) void wrongDatabaseCredentialsRefuseTheHost() throws Exception {
        requirePositive();
        var run = open("negative-wrong-database-credentials");
        try {
            run.stores().requireDocker();
            var copy = copy(run, "backup-copy");
            var restored = restore(run, run.directory().resolve("restore"), copy);
            var record = RepositoryBackupRehearsalBackupSet.readJson(copy.resolve("identities/identities.json"));
            var env = hostEnvironment(run, restored.postgres(), restored.provider().endpoint(), RepositoryBackupRehearsalBackupSet.string(record, "generation"),
                    RepositoryBackupRehearsalBackupSet.string(record, "realm"));
            env.put("PROTOMOLT_REHEARSAL_DB_PASSWORD", "wrong-" + UUID.randomUUID());
            env.put("PROTOMOLT_REHEARSAL_IDENTITIES", copy.resolve("identities").toString());
            env.put("PROTOMOLT_REHEARSAL_MANIFEST", RepositoryBackupRehearsalBackupSet.manifest(copy).toString());
            env.put("PROTOMOLT_REHEARSAL_OUT", run.directory().resolve("recovered").toString());
            env.put("PROTOMOLT_REHEARSAL_MODE", "verify");
            var host = host(run, "RepositoryBackupRehearsalRecoveredHost", env, run.directory().resolve("recovered-host.log"), 300);
            assertThat(host.exitCode()).isNotZero();
            assertThat(host.stdout()).containsIgnoringCase("password authentication failed");
            assertThat(host.stdout()).doesNotContain("REPOSITORY_BACKUP_REHEARSAL_RECOVERED_VERIFY_OK");
            assertThat(Files.exists(run.directory().resolve("recovered/READY"))).isFalse();
            run.log().pass("wrong-database-credentials.refused", "recovered host exit=" + host.exitCode() + "; PostgreSQL refused the credential; no READY");
            outcomes.put(run.id(), Map.of("passed", true));
        } catch (Throwable failure) { outcomes.put(run.id(), Map.of("passed", false, "failure", failure.toString())); throw failure; }
        finally { finish(run); }
    }

    /** Provider identity is provider-issued: the recorded bytes copied into a new provider get a different version id. */
    @Test @Order(12) void copyingObjectsDoesNotPreserveProviderIdentity() throws Exception {
        requirePositive();
        var run = open("identity-gap");
        try {
            run.stores().requireDocker();
            var copy = copy(run, "backup-copy");
            var restored = restore(run, run.directory().resolve("restore"), copy);
            var record = RepositoryBackupRehearsalBackupSet.readJson(copy.resolve("identities/identities.json"));
            var object = RepositoryBackupRehearsalBackupSet.list(record, "documentObjects").getFirst();
            String bucket = RepositoryBackupRehearsalBackupSet.string(object, "namespace"), key = RepositoryBackupRehearsalBackupSet.string(object, "key");
            String recordedVersion = RepositoryBackupRehearsalBackupSet.string(object, "providerVersion"), recordedSha = RepositoryBackupRehearsalBackupSet.string(object, "sha256");
            byte[] bytes;
            try (var client = s3(restored.provider())) {
                var response = client.getObjectAsBytes(builder -> builder.bucket(bucket).key(key).versionId(recordedVersion));
                bytes = response.asByteArray();
                assertThat(response.response().versionId()).isEqualTo(recordedVersion);
                assertThat(RepositoryBackupRehearsalBackupSet.sha256(bytes)).isEqualTo(recordedSha);
                run.log().pass("identity-gap.get_recorded_version", "restored volume serves the recorded version " + recordedVersion + " with sha256 " + recordedSha);
            }
            var volume = run.stores().createVolume("fresh-rustfs");
            var fresh = run.stores().startRustFs("fresh-rustfs", volume, RepositoryBackupRehearsalStores.reservePort(), run.s3Access(), run.s3Secret());
            try (var client = s3(fresh)) {
                client.createBucket(builder -> builder.bucket(bucket));
                client.putBucketVersioning(builder -> builder.bucket(bucket).versioningConfiguration(configuration ->
                        configuration.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
                var written = client.putObject(builder -> builder.bucket(bucket).key(key).contentType(RepositoryBackupRehearsalBackupSet.string(object, "contentType")),
                        software.amazon.awssdk.core.sync.RequestBody.fromBytes(bytes));
                assertThat(written.versionId()).isNotBlank().isNotEqualTo(recordedVersion);
                var back = client.getObjectAsBytes(builder -> builder.bucket(bucket).key(key).versionId(written.versionId()));
                assertThat(RepositoryBackupRehearsalBackupSet.sha256(back.asByteArray())).isEqualTo(recordedSha);
                String absent = RepositoryBackupRehearsalContentChecksOutcome.of(() -> client.getObjectAsBytes(builder -> builder.bucket(bucket).key(key).versionId(recordedVersion)));
                assertThat(absent).doesNotStartWith("OK");
                run.log().pass("identity-gap.version_id_not_preserved", "the same bytes (sha256 " + recordedSha + ") put into a fresh provider were issued version "
                        + written.versionId() + ", not the recorded " + recordedVersion + "; a GET by the recorded version on the fresh provider -> " + absent);
                outcomes.put(run.id(), Map.of("passed", true, "recordedVersion", recordedVersion, "issuedVersion", written.versionId()));
            }
        } catch (Throwable failure) { outcomes.put(run.id(), Map.of("passed", false, "failure", failure.toString())); throw failure; }
        finally { finish(run); }
    }

    /** The outcome class of a driver-side action, for recorded refusals. */
    static final class RepositoryBackupRehearsalContentChecksOutcome {
        interface Action { void run() throws Exception; }
        static String of(Action action) {
            try { action.run(); return "OK"; }
            catch (Exception failure) { return failure.getClass().getSimpleName() + ": " + failure.getMessage(); }
        }
    }

    // ---- the positive rehearsal ----------------------------------------------------------------

    private Path positiveRun(String name) throws Exception {
        var run = open(name);
        var stores = run.stores();
        var log = run.log();
        var backup = run.directory().resolve("backup");
        String generation = "rehearsal-gen-" + UUID.randomUUID();
        String realm = "rehearsal-realm";
        try {
            Files.createDirectories(backup.resolve("sql")); Files.createDirectories(backup.resolve("provider"));
            Files.createDirectories(backup.resolve("schema")); Files.createDirectories(backup.resolve("catalog"));
            stores.requireDocker();
            stores.createNetwork();
            var sourceDbVolume = stores.createVolume("src-pg-data");
            var sourceFsVolume = stores.createVolume("src-rustfs-data");
            var sourceDb = stores.startPostgres("src-pg", sourceDbVolume, "repo", run.dbPassword(), "repo");
            int providerPort = RepositoryBackupRehearsalStores.reservePort();
            var sourceFs = stores.startRustFs("src-rustfs", sourceFsVolume, providerPort, run.s3Access(), run.s3Secret());
            log.pass("source.started", "postgres " + RepositoryBackupRehearsalStores.POSTGRES_IMAGE + " image=" + stores.imageId(RepositoryBackupRehearsalStores.POSTGRES_IMAGE)
                    + "; rustfs " + RepositoryBackupRehearsalStores.RUSTFS_IMAGE + " image=" + stores.imageId(RepositoryBackupRehearsalStores.RUSTFS_IMAGE) + " endpoint=" + sourceFs.endpoint());

            // 1. Seed through production paths; the seed host records identities into the backup set.
            var env = hostEnvironment(run, sourceDb, sourceFs.endpoint(), generation, realm);
            env.put("PROTOMOLT_REHEARSAL_IDENTITIES", backup.resolve("identities").toString());
            var seed = host(run, "RepositoryBackupRehearsalSeedHost", env, run.directory().resolve("seed-host.log"), 900);
            assertThat(seed.exitCode()).as("seed host exit; log: %s", run.directory().resolve("seed-host.log")).isZero();
            assertThat(seed.stdout()).contains("REPOSITORY_BACKUP_REHEARSAL_SEED_OK");
            log.pass("seed.exit", "seed host exit=0 with REPOSITORY_BACKUP_REHEARSAL_SEED_OK");

            // 2. QUIESCED cut: the only writer has exited; prove no backend, no active reader, no pin; fingerprint; dump; stop; archive; seal.
            Map<String, Long> sequences; Map<String, Long> counts; long level, maxXid; int xidColumns; Map<String, Object> invariants;
            TreeMap<String, String> fingerprints; TreeMap<String, Long> schemaCatalog;
            try (var catalog = catalog(sourceDb)) {
                var others = catalog.otherBackends();
                assertThat(others).as("client backends other than the capture connection").isEmpty();
                var inFlight = catalog.inFlightWork();
                assertThat(inFlight.get("activeReaderIncarnations")).isZero();
                assertThat(inFlight.get("documentReadPins")).isZero();
                assertThat(inFlight.get("stagingPartAttempts")).isZero();
                log.pass("capture.quiescent", "no other client backend, no ACTIVE reader incarnation, no read pin, no staging part attempt: " + inFlight
                        + " (the pending operation's journal rows are durable state, not process activity)");
                fingerprints = catalog.tableFingerprints(); sequences = catalog.sequences(); counts = catalog.rowCounts(); level = catalog.migrationLevel();
                maxXid = catalog.maxStoredXid(); xidColumns = catalog.xidColumns(); invariants = catalog.coverageInvariants(); schemaCatalog = catalog.schemaCatalog();
                assertThat(level).isEqualTo(119);
                for (String key : List.of("certifiedAndUnresolved", "lineageAndUnresolved", "certifiedAndLineage", "certificateDiffersFromPreparation",
                        "certificateDiffersFromSealedHeader", "lineageDiffersFromInstallEdge", "lineageDiffersFromCertifiedAnchor",
                        "lineageDepthOrPredecessorInvalid", "preparationNotInExactlyOneState", "installXidNotBelowCurrent", "successorExecutions"))
                    assertThat(invariants.get(key)).as(key).isEqualTo(0L);
                // Every managed publication journals its own preparation and certifies it (V118): three committed
                // publications plus the pending operation's generation 0.
                assertThat(invariants.get("certificates")).isEqualTo(4L);
                assertThat(invariants.get("lineage")).isEqualTo(1L);
                assertThat(invariants.get("unresolved")).isEqualTo(1L);
                assertThat(invariants.get("successorInstalls")).isEqualTo(2L);
                log.pass("capture.catalog_fingerprinted", "tables=" + fingerprints.size() + " V" + level + " xid8 columns=" + xidColumns + " maxStoredXid=" + maxXid
                        + " coverage invariants hold: " + invariants);
            }
            var catalogText = new StringBuilder("account\tsha256\tsize\n");
            schemaCatalog.forEach((key, size) -> catalogText.append(key.replace('/', '\t')).append('\t').append(size).append('\n'));
            Files.writeString(backup.resolve("schema/catalog.tsv"), catalogText, StandardCharsets.UTF_8);
            var catalogRecord = new LinkedHashMap<String, Object>();
            catalogRecord.put("tables", new LinkedHashMap<String, Object>(fingerprints));
            catalogRecord.put("sequences", new LinkedHashMap<String, Object>(sequences));
            catalogRecord.put("maxStoredXid", maxXid);
            catalogRecord.put("xidColumns", (long) xidColumns);
            catalogRecord.put("coverageInvariants", invariants);
            RepositoryBackupRehearsalBackupSet.writeJson(backup.resolve("catalog/fingerprints.json"), catalogRecord);
            var dump = stores.pgDump(sourceDb, backup.resolve("sql/ledger.dump"));
            assertThat(dump.ok() && !dump.stderr().toLowerCase().contains("error") && Files.size(backup.resolve("sql/ledger.dump")) > 0)
                    .as("pg_dump exit=%s stderr=%s", dump.exitCode(), dump.stderr()).isTrue();
            log.pass("capture.pg_dump", "exit=" + dump.exitCode() + " bytes=" + Files.size(backup.resolve("sql/ledger.dump")));
            // The cut is proven, not assumed: no session may have appeared during the dump window, and the restored
            // catalog must reproduce the fingerprints taken before the dump (checked in restore), so any write
            // landing between the quiescence check and the dump would be detected rather than silently captured.
            try (var catalog = catalog(sourceDb)) {
                var others = catalog.otherBackends();
                assertThat(others).as("client backends after the dump").isEmpty();
                assertThat(catalog.tableFingerprints()).isEqualTo(fingerprints);
                log.pass("capture.post_dump_quiescent", "no client backend after pg_dump; every table fingerprint unchanged across the dump window");
            }
            var dbStop = stores.stop(sourceDb.container());
            var fsStop = stores.stop(sourceFs.container());
            log.pass("capture.stopped", "postgres " + dbStop + "; rustfs " + fsStop);
            var archive = stores.archiveVolume(sourceFsVolume, backup.resolve("provider/rustfs-data.tar"));
            assertThat(archive.ok()).as("provider archive stderr=%s", archive.stderr()).isTrue();
            log.pass("capture.provider_archive", "stopped RustFS volume archived, bytes=" + Files.size(backup.resolve("provider/rustfs-data.tar")));
            var identity = new LinkedHashMap<String, Object>();
            identity.put("images", Map.of("postgres", Map.of("tag", RepositoryBackupRehearsalStores.POSTGRES_IMAGE, "id", stores.imageId(RepositoryBackupRehearsalStores.POSTGRES_IMAGE)),
                    "rustfs", Map.of("tag", RepositoryBackupRehearsalStores.RUSTFS_IMAGE, "id", stores.imageId(RepositoryBackupRehearsalStores.RUSTFS_IMAGE))));
            identity.put("backend", Map.of("generation", generation, "realm", realm, "endpoint", sourceFs.endpoint(), "region", "us-east-1", "pathStyle", true, "providerPort", (long) providerPort));
            identity.put("database", Map.of("name", sourceDb.database(), "user", sourceDb.user(), "migrationLevel", level));
            identity.put("sequences", new LinkedHashMap<String, Object>(sequences));
            identity.put("rowCounts", new LinkedHashMap<String, Object>(counts));
            identity.put("stopped", Map.of("postgres", dbStop, "rustfs", fsStop));
            identity.put("catalog", Map.of("tables", (long) fingerprints.size(), "maxStoredXid", maxXid, "xidColumns", (long) xidColumns));
            var manifest = RepositoryBackupRehearsalBackupSet.seal(backup, identity);
            log.pass("capture.sealed", "components=" + RepositoryBackupRehearsalBackupSet.object(manifest, "components").size() + " manifest sha256=" + RepositoryBackupRehearsalBackupSet.sha256(RepositoryBackupRehearsalBackupSet.manifest(backup)));
            RepositoryBackupRehearsalBackupSet.verify(backup);
            var sealedFingerprint = RepositoryBackupRehearsalBackupSet.fingerprint(backup);
            log.pass("capture.preflight", "sealed set verifies; " + sealedFingerprint.size() + " files fingerprinted before restore");

            // 3. The source becomes unreachable before restore; its stopped volume is digested for the untouched proof.
            stores.remove(sourceDb.container());
            stores.remove(sourceFs.container());
            assertThatThrownBy(() -> catalog(sourceDb).close()).isInstanceOf(IllegalStateException.class);
            assertThat(stores.exists(sourceDb.container()) || stores.exists(sourceFs.container())).isFalse();
            assertThat(RepositoryBackupRehearsalStores.portFree(providerPort)).isTrue();
            log.pass("source.unreachable", "source containers removed; database refuses connections; provider port " + providerPort + " free");
            String sourceFsDigest = stores.volumeDigest(sourceFsVolume);

            // 4. Restore into new stores, compare catalog identities, then verify in a fresh production-JAR host.
            var restored = restore(run, run.directory().resolve("restore"), backup);
            env = hostEnvironment(run, restored.postgres(), restored.provider().endpoint(), generation, realm);
            env.put("PROTOMOLT_REHEARSAL_IDENTITIES", backup.resolve("identities").toString());
            env.put("PROTOMOLT_REHEARSAL_MANIFEST", RepositoryBackupRehearsalBackupSet.manifest(backup).toString());
            env.put("PROTOMOLT_REHEARSAL_OUT", run.directory().resolve("recovered").toString());
            env.put("PROTOMOLT_REHEARSAL_MODE", "verify");
            var recovered = host(run, "RepositoryBackupRehearsalRecoveredHost", env, run.directory().resolve("recovered-host.log"), 900);
            assertThat(recovered.exitCode()).as("recovered host exit; log: %s", run.directory().resolve("recovered-host.log")).isZero();
            assertThat(recovered.stdout()).contains("REPOSITORY_BACKUP_REHEARSAL_RECOVERED_VERIFY_OK");
            assertThat(Files.isRegularFile(run.directory().resolve("recovered/READY"))).isTrue();
            log.pass("recovered.exit", "recovered host exit=0, READY written after verification");
            assertThat(stores.volumeDigest(sourceFsVolume)).isEqualTo(sourceFsDigest);
            log.pass("source.volume_untouched", "source provider volume digest unchanged " + sourceFsDigest);
            assertThat(RepositoryBackupRehearsalBackupSet.fingerprint(backup)).isEqualTo(sealedFingerprint);
            log.pass("backup.untouched", "every file of the sealed set, including manifest.json and SEALED, unchanged after restore and verification");
            var outcome = new LinkedHashMap<String, Object>();
            outcome.put("passed", true); outcome.put("backup", backup.toString()); outcome.put("generation", generation);
            outcome.put("providerPort", (long) providerPort);
            outcome.put("maxStoredXid", maxXid); outcome.put("tables", (long) fingerprints.size());
            outcomes.put(name, outcome);
            return backup;
        } catch (Throwable failure) {
            outcomes.put(name, Map.of("passed", false, "failure", failure.toString()));
            throw failure;
        } finally { finish(run); }
    }

    record Restored(RepositoryBackupRehearsalStores.Postgres postgres, RepositoryBackupRehearsalStores.Provider provider, Map<String, Object> manifest) {}

    /** Preflight, then new volumes and containers from the backup set; refuses anything nonempty or unverifiable. */
    private Restored restore(Run run, Path target, Path backup) throws Exception {
        var stores = run.stores();
        var log = run.log();
        var manifest = RepositoryBackupRehearsalBackupSet.verify(backup);
        if (Files.exists(target) && !isEmptyDirectory(target)) throw new IllegalStateException("Restore target is not empty: " + target);
        Files.createDirectories(target);
        var backend = RepositoryBackupRehearsalBackupSet.object(manifest, "backend");
        var database = RepositoryBackupRehearsalBackupSet.object(manifest, "database");
        int port = (int) RepositoryBackupRehearsalBackupSet.number(backend, "providerPort");
        if (!RepositoryBackupRehearsalStores.portFree(port)) throw new IllegalStateException("Recorded provider port " + port + " is still in use; the original provider is not stopped");
        if (stores.created().stream().noneMatch(resource -> resource.startsWith("network:"))) stores.createNetwork();
        var dbVolume = stores.createVolume("rst-pg-data");
        var fsVolume = stores.createVolume("rst-rustfs-data");
        if (!stores.volumeEmpty(dbVolume) || !stores.volumeEmpty(fsVolume)) throw new IllegalStateException("Restore volumes are not empty");
        var extract = stores.extractVolume(backup.resolve("provider/rustfs-data.tar"), fsVolume);
        if (!extract.ok()) throw new IllegalStateException("Provider volume extraction failed: " + extract.stderr());
        var provider = stores.startRustFs("rst-rustfs", fsVolume, port, run.s3Access(), run.s3Secret());
        assertThat(provider.endpoint()).isEqualTo(RepositoryBackupRehearsalBackupSet.string(backend, "endpoint"));
        log.pass("restore.provider_identity", "restored provider volume served at the recorded endpoint " + provider.endpoint());
        var postgres = stores.startPostgres("rst-pg", dbVolume, RepositoryBackupRehearsalBackupSet.string(database, "user"), run.dbPassword(), RepositoryBackupRehearsalBackupSet.string(database, "name"));
        var result = stores.pgRestore(postgres, backup.resolve("sql/ledger.dump"));
        if (!result.ok() || result.stderr().contains("ERROR") || result.stderr().contains("WARNING"))
            throw new IllegalStateException("pg_restore reported problems: exit=" + result.exitCode() + " stderr=" + result.stderr());
        log.pass("restore.pg_restore", "exit=" + result.exitCode() + " into new database " + postgres.container());
        var recorded = RepositoryBackupRehearsalBackupSet.readJson(backup.resolve("catalog/fingerprints.json"));
        try (var catalog = catalog(postgres)) {
            long max = catalog.maxStoredXid(), current = catalog.currentXid();
            // A restored cluster whose counter is not past every stored xid8 is refused, not repaired here:
            // advancing it is a manual pg_resetwal procedure that this rehearsal has not exercised.
            if (current <= max) throw new IllegalStateException("Restored cluster next transaction id " + current
                    + " is not past the largest stored xid8 " + max + "; refuse to start a host (see the operator guide)");
            log.pass("restore.xid_epoch", "next xid " + current + " > max stored xid8 " + max + " (no advancement needed)");
            var expectedSequences = new TreeMap<String, Long>();
            RepositoryBackupRehearsalBackupSet.object(manifest, "sequences").forEach((sequence, value) -> expectedSequences.put(sequence, RepositoryBackupRehearsalBackupSet.number(RepositoryBackupRehearsalBackupSet.object(manifest, "sequences"), sequence)));
            assertThat(catalog.sequences()).isEqualTo(expectedSequences);
            log.pass("restore.sequences", "sequences=" + catalog.sequences());
            assertThat(catalog.migrationLevel()).isEqualTo(RepositoryBackupRehearsalBackupSet.number(database, "migrationLevel"));
            log.pass("restore.migration_level", "V" + catalog.migrationLevel());
            var expectedTables = new TreeMap<String, String>();
            RepositoryBackupRehearsalBackupSet.object(recorded, "tables").forEach((table, value) -> expectedTables.put(table, value.toString()));
            var actualTables = catalog.tableFingerprints();
            var differing = new ArrayList<String>();
            for (var entry : expectedTables.entrySet()) if (!entry.getValue().equals(actualTables.get(entry.getKey()))) differing.add(entry.getKey());
            assertThat(differing).as("tables whose restored row text differs from the source fingerprint").isEmpty();
            assertThat(actualTables.keySet()).isEqualTo(expectedTables.keySet());
            log.pass("restore.catalog_identity", "every table's row fingerprint equals the source capture: tables=" + actualTables.size()
                    + " (V118 certificates/unresolved and V119 lineage/install edges included)");
            assertThat(catalog.maxStoredXid()).isEqualTo(RepositoryBackupRehearsalBackupSet.number(recorded, "maxStoredXid"));
            var invariants = catalog.coverageInvariants();
            var recordedInvariants = RepositoryBackupRehearsalBackupSet.object(recorded, "coverageInvariants");
            for (var entry : invariants.entrySet())
                assertThat(((Number) entry.getValue()).longValue()).as(entry.getKey()).isEqualTo(RepositoryBackupRehearsalBackupSet.number(recordedInvariants, entry.getKey()));
            log.pass("restore.coverage_invariants", "V118/V119 invariants equal the source: " + invariants);
        }
        return new Restored(postgres, provider, manifest);
    }

    // ---- negative-case plumbing ----------------------------------------------------------------

    interface BeforeRestore { void apply(Run run, Path copy, Path identities) throws Exception; }
    interface Injection { void apply(Run run, Restored restored, Map<String, Object> record) throws Exception; }

    private void negativeRestore(String mode, BeforeRestore beforeRestore, Injection injection) throws Exception {
        var run = open("negative-" + mode);
        try {
            var before = RepositoryBackupRehearsalBackupSet.fingerprint(firstBackup);
            run.stores().requireDocker();
            var copy = copy(run, "backup-copy");
            if (beforeRestore != null) beforeRestore.apply(run, copy, copy.resolve("identities"));
            var restored = restore(run, run.directory().resolve("restore"), copy);
            var record = RepositoryBackupRehearsalBackupSet.readJson(copy.resolve("identities/identities.json"));
            if (injection != null) {
                injection.apply(run, restored, record);
                RepositoryBackupRehearsalBackupSet.writeJson(copy.resolve("identities/identities.json"), record);
            }
            String generation = RepositoryBackupRehearsalBackupSet.string(record, "generation"), realm = RepositoryBackupRehearsalBackupSet.string(record, "realm");
            var env = hostEnvironment(run, restored.postgres(), restored.provider().endpoint(), generation, realm);
            env.put("PROTOMOLT_REHEARSAL_IDENTITIES", copy.resolve("identities").toString());
            env.put("PROTOMOLT_REHEARSAL_MANIFEST", RepositoryBackupRehearsalBackupSet.manifest(copy).toString());
            env.put("PROTOMOLT_REHEARSAL_OUT", run.directory().resolve("recovered").toString());
            env.put("PROTOMOLT_REHEARSAL_MODE", mode);
            switch (mode) {
                case "wrong-endpoint" -> env.put("PROTOMOLT_REHEARSAL_S3_ENDPOINT", "http://127.0.0.1:" + RepositoryBackupRehearsalStores.reservePort());
                case "new-generation" -> env.put("PROTOMOLT_REHEARSAL_GENERATION", "rehearsal-other-" + UUID.randomUUID());
                default -> { }
            }
            var host = host(run, "RepositoryBackupRehearsalRecoveredHost", env, run.directory().resolve("recovered-host.log"), 600);
            String marker = "REPOSITORY_BACKUP_REHEARSAL_RECOVERED_" + mode.toUpperCase().replace('-', '_') + "_OK";
            assertThat(host.exitCode()).as("recovered host (%s) exit; log: %s", mode, run.directory().resolve("recovered-host.log")).isZero();
            assertThat(host.stdout()).contains(marker);
            assertThat(Files.exists(run.directory().resolve("recovered/READY"))).isFalse();
            run.log().pass(mode + ".observed", "recovered host observed the refusal: " + marker + "; no READY advertised");
            assertThat(RepositoryBackupRehearsalBackupSet.fingerprint(firstBackup)).isEqualTo(before);
            run.log().pass(mode + ".original_backup_untouched", "run-1 backup set unchanged");
            outcomes.put(run.id(), Map.of("passed", true));
        } catch (Throwable failure) { outcomes.put(run.id(), Map.of("passed", false, "failure", failure.toString())); throw failure; }
        finally { finish(run); }
    }

    private void expectRefusal(Run run, Path copy, String expectedText, String check) {
        assertThatThrownBy(() -> restore(run, run.directory().resolve("restore-" + check), copy))
                .isInstanceOf(IllegalStateException.class).satisfies(failure -> assertThat(failure.getMessage().toLowerCase()).contains(expectedText.toLowerCase()));
        assertThat(run.stores().created()).as("resources created before a preflight refusal").isEmpty();
        run.log().pass(check, "refused before any resource was created (" + expectedText + ")");
    }

    private Path copy(Run run, String name) {
        var copy = run.directory().resolve(name);
        RepositoryBackupRehearsalBackupSet.copy(firstBackup, copy);
        return copy;
    }

    private static String attachmentArtifact(Map<String, Object> record) {
        var artifacts = RepositoryBackupRehearsalBackupSet.object(record, "artifacts");
        return artifacts.entrySet().stream().filter(entry -> entry.getKey().endsWith("Attachment")).map(entry -> entry.getValue().toString()).findFirst().orElseThrow();
    }

    private void requirePositive() {
        assertThat(firstBackup).as("a passed positive rehearsal (run-1) is required before negative cases").isNotNull();
        assertThat(secondBackup).as("a passed positive rehearsal (run-2) is required before negative cases").isNotNull();
    }

    // ---- run plumbing ---------------------------------------------------------------------------

    record Run(String id, RepositoryBackupRehearsalShell shell, RepositoryBackupRehearsalStores stores, Log log, Path directory,
            String dbPassword, String s3Access, String s3Secret, String apiToken) {}

    /** Driver observations as markers, alongside the JUnit result; every pass line is an asserted fact. */
    static final class Log {
        private final Path markers;
        Log(Path markers) { this.markers = markers; }
        void pass(String name, String detail) {
            var line = "CHECK_OK " + name + " :: " + detail + "\n";
            System.out.print(line);
            try { Files.writeString(markers, line, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND); }
            catch (java.io.IOException failure) { throw new IllegalStateException("Cannot append markers", failure); }
        }
    }

    private Run open(String name) throws Exception {
        var directory = Files.createDirectories(ROOT.resolve(name));
        String id = "protomolt-rehearsal-" + Long.toHexString(System.nanoTime()).substring(4) + "-" + UUID.randomUUID().toString().substring(0, 8);
        String dbPassword = UUID.randomUUID().toString(), s3Access = "rehearsal-" + UUID.randomUUID().toString().substring(0, 8),
                s3Secret = UUID.randomUUID().toString(), apiToken = UUID.randomUUID().toString();
        var shell = new RepositoryBackupRehearsalShell(directory.resolve("commands.log"), List.of(dbPassword, s3Secret, apiToken));
        return new Run(name, shell, new RepositoryBackupRehearsalStores(shell, id), new Log(directory.resolve("markers.log")), directory, dbPassword, s3Access, s3Secret, apiToken);
    }

    private Map<String, String> hostEnvironment(Run run, RepositoryBackupRehearsalStores.Postgres postgres, String endpoint, String generation, String realm) {
        var env = new LinkedHashMap<String, String>();
        env.put("PROTOMOLT_REHEARSAL_JDBC", postgres.jdbcUrl());
        env.put("PROTOMOLT_REHEARSAL_DB_USER", postgres.user());
        env.put("PROTOMOLT_REHEARSAL_DB_PASSWORD", postgres.password());
        env.put("PROTOMOLT_REHEARSAL_S3_ENDPOINT", endpoint);
        env.put("PROTOMOLT_REHEARSAL_S3_REGION", "us-east-1");
        env.put("PROTOMOLT_REHEARSAL_S3_ACCESS", run.s3Access());
        env.put("PROTOMOLT_REHEARSAL_S3_SECRET", run.s3Secret());
        env.put("PROTOMOLT_REHEARSAL_GENERATION", generation);
        env.put("PROTOMOLT_REHEARSAL_REALM", realm);
        env.put("PROTOMOLT_REHEARSAL_BUNDLE", compiled.bundle().toString());
        env.put("PROTOMOLT_REHEARSAL_API_TOKEN", run.apiToken());
        return env;
    }

    /** Child JVM on the production classpath plus the compiled hosts; exit code and markers are the evidence. */
    private RepositoryBackupRehearsalShell.Result host(Run run, String mainClass, Map<String, String> environment, Path log, long timeoutSeconds) throws Exception {
        var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-XX:+DisableAttachMechanism",
                "-XX:-EnableDynamicAgentLoading", "-cp", compiled.classpath() + java.io.File.pathSeparator + compiled.probe(),
                "ai.protomolt.proto.repo.container.ledger." + mainClass);
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().putAll(environment);
        var process = builder.start();
        try {
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new IllegalStateException(mainClass + " timed out; log: " + log); }
        } finally { if (process.isAlive()) process.destroyForcibly(); }
        String output = Files.readString(log, StandardCharsets.UTF_8);
        assertThat(Files.size(log)).isLessThan(4_194_304);
        run.shell().note(mainClass + " exit=" + process.exitValue() + " log=" + log.getFileName());
        return new RepositoryBackupRehearsalShell.Result(List.of(mainClass), process.exitValue(), output, "");
    }

    private static RepositoryBackupRehearsalCatalog catalog(RepositoryBackupRehearsalStores.Postgres postgres) {
        return new RepositoryBackupRehearsalCatalog(postgres.jdbcUrl(), postgres.user(), postgres.password());
    }

    private static software.amazon.awssdk.services.s3.S3Client s3(RepositoryBackupRehearsalStores.Provider provider) {
        return software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(java.net.URI.create(provider.endpoint()))
                .region(software.amazon.awssdk.regions.Region.US_EAST_1).forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(provider.accessKey(), provider.secretKey()))).build();
    }

    private static boolean isEmptyDirectory(Path path) throws java.io.IOException {
        try (var entries = Files.list(path)) { return Files.isDirectory(path) && entries.findFirst().isEmpty(); }
    }

    private void finish(Run run) {
        for (String resource : run.stores().created()) {
            if (resource.startsWith("container:")) {
                String name = resource.substring("container:".length());
                run.stores().writeLogs(name, run.directory().resolve("docker-" + name.substring(name.lastIndexOf('-') + 1) + ".log"));
            }
        }
        if (!"true".equals(System.getProperty("protomolt.test.rehearsalKeep"))) run.stores().cleanup();
    }
}
