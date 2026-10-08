package ai.protomolt.proto.repo.recovery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Offline backup and recovery rehearsal: seed a repository through production paths over
 * real PostgreSQL and pinned RustFS, stop every actor, capture a sealed backup set, make the
 * source unreachable, restore into new resources, verify in a fresh production-JAR host, then
 * run the negative cases on disposable copies. Evidence is archived under the output directory.
 */
public final class RecoveryRehearsal {
    record Options(Path out, int runs, boolean keep, boolean negatives, Path bundle, long xidBurn) {}

    public static void main(String[] args) throws Exception {
        var options = parse(args);
        Files.createDirectories(options.out());
        if (!Files.isDirectory(options.bundle().resolve("artifacts")) || !Files.isRegularFile(options.bundle().resolve("inventory.tsv")))
            throw new RehearsalFailure("Admission runtime bundle is missing: " + options.bundle());
        var summary = new LinkedHashMap<String, Object>();
        summary.put("startedAt", Instant.now().toString());
        summary.put("bundle", options.bundle().toString());
        summary.put("runtimeInventory", runtimeInventory(options.out().resolve("runtime-inventory.tsv")));
        var outcomes = new LinkedHashMap<String, Object>();
        boolean failed = false;
        Path firstBackup = null;
        for (int run = 1; run <= options.runs(); run++) {
            var directory = options.out().resolve("run-" + run);
            var outcome = new LinkedHashMap<String, Object>();
            try {
                // The second run seeds after consuming transaction ids, so the restore must take the
                // pg_resetwal branch; the first run covers the no-advancement branch.
                var backup = positiveRun(directory, options, outcome, run == 2 ? options.xidBurn() : 0);
                if (firstBackup == null) firstBackup = backup;
                outcome.put("passed", true);
            } catch (RuntimeException failure) {
                failed = true;
                outcome.put("passed", false);
                outcome.put("failure", failure.toString());
                System.out.println("RUN_FAILED run-" + run + " " + failure);
                failure.printStackTrace();
            }
            outcomes.put("run-" + run, outcome);
        }
        if (options.negatives() && firstBackup != null && !failed) {
            for (String name : List.of("missing-component", "corrupt-checksum", "unsealed", "nonempty-target", "wrong-endpoint",
                    "new-generation", "missing-version", "corrupt-descriptor")) {
                var directory = options.out().resolve("negative-" + name);
                var outcome = new LinkedHashMap<String, Object>();
                try {
                    negativeCase(name, directory, firstBackup, options, outcome);
                    outcome.put("passed", true);
                } catch (RuntimeException failure) {
                    failed = true;
                    outcome.put("passed", false);
                    outcome.put("failure", failure.toString());
                    System.out.println("NEGATIVE_FAILED " + name + " " + failure);
                    failure.printStackTrace();
                }
                outcomes.put("negative-" + name, outcome);
            }
        } else if (options.negatives()) {
            failed = true;
            outcomes.put("negatives", Map.of("passed", false, "failure", "skipped because a positive run failed"));
        }
        summary.put("outcomes", outcomes);
        summary.put("passed", !failed);
        summary.put("finishedAt", Instant.now().toString());
        Json.write(options.out().resolve("summary.json"), summary);
        System.out.println(failed ? "REPOSITORY_RECOVERY_REHEARSAL_FAILED" : "REPOSITORY_RECOVERY_REHEARSAL_OK");
        System.exit(failed ? 1 : 0);
    }

    static Options parse(String[] args) {
        Path out = null; int runs = 2; boolean keep = false; boolean negatives = true; long xidBurn = 8192;
        Path bundle = System.getProperty("protomolt.recovery.bundle") == null ? null : Path.of(System.getProperty("protomolt.recovery.bundle"));
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--out" -> out = Path.of(args[++i]);
                case "--runs" -> runs = Integer.parseInt(args[++i]);
                case "--bundle" -> bundle = Path.of(args[++i]);
                case "--keep" -> keep = true;
                case "--skip-negatives" -> negatives = false;
                case "--xid-burn" -> xidBurn = Long.parseLong(args[++i]);
                default -> throw new RehearsalFailure("Unknown option " + args[i] + " (expected --out DIR [--runs N] [--bundle DIR] [--keep] [--skip-negatives] [--xid-burn N])");
            }
        }
        if (out == null) throw new RehearsalFailure("--out DIR is required");
        if (bundle == null) throw new RehearsalFailure("--bundle DIR (admission transport runtime inventory) is required");
        if (runs < 1) throw new RehearsalFailure("--runs must be positive");
        if (xidBurn < 0 || xidBurn > 1_000_000) throw new RehearsalFailure("--xid-burn must be between 0 and 1000000");
        if (Files.exists(out) && !isEmptyDirectory(out)) throw new RehearsalFailure("Output directory must be new or empty: " + out);
        return new Options(out.toAbsolutePath(), runs, keep, negatives, bundle.toAbsolutePath(), xidBurn);
    }

    static boolean isEmptyDirectory(Path path) {
        try (var entries = Files.list(path)) { return Files.isDirectory(path) && entries.findFirst().isEmpty(); }
        catch (IOException failure) { throw new RehearsalFailure("Cannot inspect " + path, failure); }
    }

    /** Every JAR the hosts run on, by digest: build evidence for the exact candidate. */
    static List<Map<String, Object>> runtimeInventory(Path file) {
        var rows = new ArrayList<Map<String, Object>>();
        var text = new StringBuilder("protomolt-repository-recovery-runtime/v1\n");
        for (String entry : System.getProperty("java.class.path").split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            var path = Path.of(entry);
            if (!Files.isRegularFile(path)) continue;
            String digest = Digests.sha256(path);
            rows.add(Map.of("name", path.getFileName().toString(), "sha256", digest));
            text.append(path.getFileName()).append('\t').append(digest).append('\n');
        }
        try { Files.writeString(file, text, StandardCharsets.UTF_8); } catch (IOException failure) { throw new RehearsalFailure("Cannot write inventory", failure); }
        return rows;
    }

    record Run(Shell shell, Containers containers, Checks checks, Path directory, String id, String dbPassword, String s3Access, String s3Secret, String apiToken) {}

    static Run open(Path directory, String suite) {
        try { Files.createDirectories(directory); } catch (IOException failure) { throw new RehearsalFailure("Cannot create " + directory, failure); }
        String id = "protomolt-recovery-" + Long.toHexString(System.nanoTime()).substring(4) + "-" + UUID.randomUUID().toString().substring(0, 8);
        String dbPassword = UUID.randomUUID().toString(), s3Access = "recovery-" + UUID.randomUUID().toString().substring(0, 8),
                s3Secret = UUID.randomUUID().toString(), apiToken = UUID.randomUUID().toString();
        var shell = new Shell(directory.resolve("commands.log"), List.of(dbPassword, s3Secret, apiToken));
        return new Run(shell, new Containers(shell, id), new Checks(suite, directory.resolve("markers.log")), directory, id, dbPassword, s3Access, s3Secret, apiToken);
    }

    static Map<String, String> hostEnvironment(Run run, Containers.Postgres postgres, String endpoint, String generation, String realm, Options options) {
        var env = new LinkedHashMap<String, String>();
        env.put("PROTOMOLT_RECOVERY_JDBC", postgres.jdbcUrl());
        env.put("PROTOMOLT_RECOVERY_DB_USER", postgres.user());
        env.put("PROTOMOLT_RECOVERY_DB_PASSWORD", postgres.password());
        env.put("PROTOMOLT_RECOVERY_S3_ENDPOINT", endpoint);
        env.put("PROTOMOLT_RECOVERY_S3_REGION", "us-east-1");
        env.put("PROTOMOLT_RECOVERY_S3_ACCESS", run.s3Access());
        env.put("PROTOMOLT_RECOVERY_S3_SECRET", run.s3Secret());
        env.put("PROTOMOLT_RECOVERY_GENERATION", generation);
        env.put("PROTOMOLT_RECOVERY_REALM", realm);
        env.put("PROTOMOLT_RECOVERY_BUNDLE", options.bundle().toString());
        env.put(RehearsalFixture.API_TOKEN_ENV, run.apiToken());
        return env;
    }

    /** Child JVM on the production classpath only; exit code and markers are the evidence. */
    static Shell.Result host(Run run, String mainClass, Map<String, String> environment, Path log, long timeoutSeconds) {
        var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-XX:+DisableAttachMechanism",
                "-XX:-EnableDynamicAgentLoading", "-cp", System.getProperty("java.class.path"), mainClass);
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().putAll(environment);
        try {
            var process = builder.start();
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new RehearsalFailure(mainClass + " timed out; log: " + log); }
            String output = Files.readString(log, StandardCharsets.UTF_8);
            run.shell().run(List.of("true", "#", mainClass, "exit=" + process.exitValue(), "log=" + log.getFileName()));
            return new Shell.Result(List.of(mainClass), process.exitValue(), output, "");
        } catch (IOException | InterruptedException failure) { throw new RehearsalFailure("Cannot run " + mainClass, failure); }
    }

    static Path positiveRun(Path directory, Options options, Map<String, Object> outcome, long xidBurn) {
        var run = open(directory, "rehearsal");
        var checks = run.checks();
        var containers = run.containers();
        var backup = directory.resolve("backup");
        String generation = "recovery-gen-" + UUID.randomUUID();
        String realm = "recovery-realm";
        try {
            Files.createDirectories(backup.resolve("sql")); Files.createDirectories(backup.resolve("provider")); Files.createDirectories(backup.resolve("schema"));
            containers.requireDocker();
            containers.createNetwork();
            var sourceDbVolume = containers.createVolume("src-pg-data");
            var sourceFsVolume = containers.createVolume("src-rustfs-data");
            var sourceDb = containers.startPostgres("src-pg", sourceDbVolume, "repo", run.dbPassword(), "repo");
            int providerPort = Containers.reservePort();
            var sourceFs = containers.startRustFs("src-rustfs", sourceFsVolume, providerPort, run.s3Access(), run.s3Secret());
            checks.pass("source.started", "postgres " + Containers.POSTGRES_IMAGE + " image=" + containers.imageId(Containers.POSTGRES_IMAGE)
                    + "; rustfs " + Containers.RUSTFS_IMAGE + " image=" + containers.imageId(Containers.RUSTFS_IMAGE) + " endpoint=" + sourceFs.endpoint());

            if (xidBurn > 0) {
                try (var ledger = new Ledger(sourceDb.jdbcUrl(), sourceDb.user(), sourceDb.password())) {
                    long before = ledger.currentXid();
                    ledger.burnTransactions(xidBurn);
                    long after = ledger.currentXid();
                    checks.require(after >= before + xidBurn, "source.xid_burn", "consumed " + xidBurn + " transaction ids before seeding: " + before + " -> " + after);
                }
            }
            // 1. Seed through production paths; the seed host records identities into the backup set.
            var env = hostEnvironment(run, sourceDb, sourceFs.endpoint(), generation, realm, options);
            env.put("PROTOMOLT_RECOVERY_IDENTITIES", backup.resolve("identities").toString());
            var seed = host(run, SeedHost.class.getName(), env, directory.resolve("seed-host.log"), 900);
            checks.require(seed.exitCode() == 0 && seed.stdout().contains("RECOVERY_SEED_OK"), "seed.exit", "seed host exit=" + seed.exitCode());

            // 2. Quiesce and capture. The only writer has exited; prove no backend remains, dump, stop, archive, seal.
            Map<String, Long> sequences; Map<String, Long> counts; long level; Map<String, Object> profile; var catalog = new StringBuilder("account\tsha256\tsize\n");
            try (var ledger = new Ledger(sourceDb.jdbcUrl(), sourceDb.user(), sourceDb.password())) {
                var others = ledger.otherBackends();
                checks.require(others.isEmpty(), "capture.quiescent", "client backends other than the capture connection: " + others);
                sequences = ledger.sequences(); counts = ledger.rowCounts(); level = ledger.migrationLevel(); profile = ledger.backendProfile(generation);
                ledger.schemaCatalog().forEach((key, size) -> catalog.append(key.replace('/', '\t')).append('\t').append(size).append('\n'));
            }
            Files.writeString(backup.resolve("schema/catalog.tsv"), catalog, StandardCharsets.UTF_8);
            var dump = containers.pgDump(sourceDb, backup.resolve("sql/ledger.dump"));
            checks.require(dump.ok() && !dump.stderr().contains("error") && Files.size(backup.resolve("sql/ledger.dump")) > 0, "capture.pg_dump", "exit=" + dump.exitCode() + " stderr=" + dump.stderr().strip());
            var dbStop = containers.stop(sourceDb.container());
            var fsStop = containers.stop(sourceFs.container());
            checks.pass("capture.stopped", "postgres " + dbStop + "; rustfs " + fsStop);
            var archive = containers.archiveVolume(sourceFsVolume, backup.resolve("provider/rustfs-data.tar"));
            checks.require(archive.ok(), "capture.provider_archive", "exit=" + archive.exitCode() + " stderr=" + archive.stderr().strip());
            var identity = new LinkedHashMap<String, Object>();
            identity.put("images", Map.of("postgres", Map.of("tag", Containers.POSTGRES_IMAGE, "id", containers.imageId(Containers.POSTGRES_IMAGE)),
                    "rustfs", Map.of("tag", Containers.RUSTFS_IMAGE, "id", containers.imageId(Containers.RUSTFS_IMAGE)),
                    "helper", Map.of("tag", Containers.HELPER_IMAGE, "id", containers.imageId(Containers.HELPER_IMAGE))));
            identity.put("backend", Map.of("generation", generation, "realm", realm, "endpoint", sourceFs.endpoint(), "region", "us-east-1", "pathStyle", true,
                    "providerPort", (long) providerPort, "profile", profile));
            identity.put("database", Map.of("name", sourceDb.database(), "user", sourceDb.user(), "migrationLevel", level));
            identity.put("sequences", new LinkedHashMap<String, Object>(sequences));
            identity.put("rowCounts", new LinkedHashMap<String, Object>(counts));
            identity.put("stopped", Map.of("postgres", dbStop, "rustfs", fsStop));
            var manifest = BackupSet.seal(backup, identity);
            checks.pass("capture.sealed", "components=" + Json.object(manifest, "components").size() + " manifest sha256=" + Digests.sha256(BackupSet.manifest(backup)));
            BackupSet.verify(backup);
            checks.pass("capture.preflight", "sealed set verifies");

            // 3. Source resources become unreachable before restore.
            containers.remove(sourceDb.container());
            containers.remove(sourceFs.container());
            boolean dbGone;
            try (var ledger = new Ledger(sourceDb.jdbcUrl(), sourceDb.user(), sourceDb.password())) { dbGone = false; } catch (RehearsalFailure expected) { dbGone = true; }
            checks.require(!containers.exists(sourceDb.container()) && !containers.exists(sourceFs.container()) && dbGone && Containers.portFree(providerPort),
                    "source.unreachable", "source containers removed; database refuses connections; provider port " + providerPort + " free");
            String sourceFsDigest = containers.volumeDigest(sourceFsVolume);

            // 4. Restore into new resources and 5./6. verify in a fresh host.
            var restored = restore(run, directory.resolve("restore"), backup, options, checks);
            checks.require(restored.xidAdvanced() == (xidBurn > 0), "restore.xid_branch",
                    xidBurn > 0 ? "seeded xid8 values exceeded the fresh cluster; pg_resetwal advancement branch executed"
                            : "fresh cluster already past the stored xid8 values; no advancement branch");
            env = hostEnvironment(run, restored.postgres(), restored.provider().endpoint(), generation, realm, options);
            env.put("PROTOMOLT_RECOVERY_IDENTITIES", backup.resolve("identities").toString());
            env.put("PROTOMOLT_RECOVERY_MANIFEST", BackupSet.manifest(backup).toString());
            env.put("PROTOMOLT_RECOVERY_OUT", directory.resolve("recovered").toString());
            env.put("PROTOMOLT_RECOVERY_MODE", "verify");
            var recovered = host(run, RecoveredHost.class.getName(), env, directory.resolve("recovered-host.log"), 900);
            checks.require(recovered.exitCode() == 0 && recovered.stdout().contains("RECOVERED_HOST_VERIFY_OK") && Files.isRegularFile(directory.resolve("recovered/READY")),
                    "recovered.exit", "recovered host exit=" + recovered.exitCode() + " READY=" + Files.isRegularFile(directory.resolve("recovered/READY")));
            checks.require(containers.volumeDigest(sourceFsVolume).equals(sourceFsDigest), "source.volume_untouched", "source provider volume digest unchanged " + sourceFsDigest);
            checks.require(BackupSet.fingerprint(backup).equals(fingerprint(manifest, backup)), "backup.untouched", "backup components unchanged after restore and verification");
            outcome.put("backup", backup.toString());
            outcome.put("generation", generation);
            outcome.put("providerPort", (long) providerPort);
            outcome.put("xidAdvanced", restored.xidAdvanced());
            return backup;
        } catch (IOException failure) {
            throw new RehearsalFailure("Rehearsal I/O failure", failure);
        } finally {
            finish(run, options);
        }
    }

    static java.util.TreeMap<String, String> fingerprint(Map<String, Object> manifest, Path backup) {
        var expected = new java.util.TreeMap<String, String>();
        Json.object(manifest, "components").forEach((name, value) -> expected.put(name, Json.string(Json.object(Json.object(manifest, "components"), name), "sha256")));
        expected.put("manifest.json", Digests.sha256(BackupSet.manifest(backup)));
        expected.put("SEALED", Digests.sha256(BackupSet.seal(backup)));
        return expected;
    }

    record Restored(Containers.Postgres postgres, Containers.Provider provider, Map<String, Object> manifest, boolean xidAdvanced) {}

    /** Preflight, then new volumes and containers from the backup set; refuses anything nonempty or unverifiable. */
    static Restored restore(Run run, Path target, Path backup, Options options, Checks checks) {
        var containers = run.containers();
        var manifest = BackupSet.verify(backup);
        if (Files.exists(target) && !isEmptyDirectory(target)) throw new RehearsalFailure("Restore target is not empty: " + target);
        try { Files.createDirectories(target); } catch (IOException failure) { throw new RehearsalFailure("Cannot create " + target, failure); }
        var backend = Json.object(manifest, "backend");
        var database = Json.object(manifest, "database");
        int port = (int) Json.number(backend, "providerPort");
        if (!Containers.portFree(port)) throw new RehearsalFailure("Recorded provider port " + port + " is still in use; the original provider is not stopped");
        if (containers.created().isEmpty()) containers.createNetwork();
        var dbVolume = containers.createVolume("rst-pg-data");
        var fsVolume = containers.createVolume("rst-rustfs-data");
        if (!containers.volumeEmpty(dbVolume) || !containers.volumeEmpty(fsVolume)) throw new RehearsalFailure("Restore volumes are not empty");
        var extract = containers.extractVolume(backup.resolve("provider/rustfs-data.tar"), fsVolume);
        if (!extract.ok()) throw new RehearsalFailure("Provider volume extraction failed: " + extract.stderr());
        var provider = containers.startRustFs("rst-rustfs", fsVolume, port, run.s3Access(), run.s3Secret());
        checks.require(provider.endpoint().equals(Json.string(backend, "endpoint")), "restore.provider_identity", "restored provider at recorded endpoint " + provider.endpoint());
        Containers.Postgres postgres = containers.startPostgres("rst-pg", dbVolume, Json.string(database, "user"), run.dbPassword(), Json.string(database, "name"));
        var restoreResult = containers.pgRestore(postgres, backup.resolve("sql/ledger.dump"));
        if (!restoreResult.ok() || restoreResult.stderr().contains("ERROR") || restoreResult.stderr().contains("WARNING"))
            throw new RehearsalFailure("pg_restore reported problems: exit=" + restoreResult.exitCode() + " stderr=" + restoreResult.stderr());
        checks.pass("restore.pg_restore", "exit=" + restoreResult.exitCode() + " into new database " + postgres.container());
        boolean advanced = false;
        try (var ledger = new Ledger(postgres.jdbcUrl(), postgres.user(), postgres.password())) {
            long max = ledger.maxStoredXid(), current = ledger.currentXid();
            if (current <= max) {
                containers.stop(postgres.container());
                long next = max + 1, epoch = next >>> 32, xid = next & 0xFFFFFFFFL;
                var reset = containers.pgResetXid(dbVolume, epoch, Math.max(xid, 3));
                if (!reset.ok()) throw new RehearsalFailure("pg_resetwal failed: " + reset.stderr());
                run.shell().run(List.of("docker", "start", postgres.container())).require("docker start");
                // Docker may publish a different ephemeral host port after a restart; resolve it again.
                postgres = new Containers.Postgres(postgres.container(), postgres.volume(), containers.mappedPort(postgres.container(), "5432/tcp"),
                        postgres.database(), postgres.user(), postgres.password());
                containers.awaitPostgres(postgres);
                advanced = true;
            }
        }
        try (var ledger = new Ledger(postgres.jdbcUrl(), postgres.user(), postgres.password())) {
            long max = ledger.maxStoredXid(), current = ledger.currentXid();
            checks.require(current > max, "restore.xid_epoch", "next xid " + current + " > max stored xid8 " + max + (advanced ? " (advanced with pg_resetwal)" : " (no advancement needed)"));
            var expected = new java.util.TreeMap<String, Long>();
            Json.object(manifest, "sequences").forEach((name, value) -> expected.put(name, Json.number(Json.object(manifest, "sequences"), name)));
            checks.require(ledger.sequences().equals(expected), "restore.sequences", "sequences=" + ledger.sequences());
            checks.require(ledger.migrationLevel() == Json.number(database, "migrationLevel"), "restore.migration_level", "V" + ledger.migrationLevel());
        }
        return new Restored(postgres, provider, manifest, advanced);
    }

    static void negativeCase(String name, Path directory, Path original, Options options, Map<String, Object> outcome) {
        var run = open(directory, "negative-" + name);
        var checks = run.checks();
        var containers = run.containers();
        var before = BackupSet.fingerprint(original);
        var copy = directory.resolve("backup-copy");
        BackupSet.copy(original, copy);
        var manifest = Json.read(BackupSet.manifest(original));
        var record = Json.read(original.resolve("identities/identities.json"));
        String generation = Json.string(record, "generation"), realm = Json.string(record, "realm");
        try {
            containers.requireDocker();
            switch (name) {
                case "missing-component" -> {
                    Files.delete(copy.resolve("provider/rustfs-data.tar"));
                    expectRefusal(run, copy, directory, options, checks, "missing", "missing-component.refused_before_resources");
                }
                case "corrupt-checksum" -> {
                    var dump = copy.resolve("sql/ledger.dump");
                    var bytes = Files.readAllBytes(dump); bytes[bytes.length / 2] ^= 0x01; Files.write(dump, bytes);
                    expectRefusal(run, copy, directory, options, checks, "checksum differs", "corrupt-checksum.refused_before_resources");
                }
                case "unsealed" -> {
                    Files.delete(copy.resolve("SEALED"));
                    expectRefusal(run, copy, directory, options, checks, "not sealed", "unsealed.refused_before_resources");
                }
                case "nonempty-target" -> {
                    var target = directory.resolve("restore");
                    Files.createDirectories(target); Files.writeString(target.resolve("existing.txt"), "previous restore state\n");
                    expectRefusal(run, copy, directory, options, checks, "not empty", "nonempty-target.directory_refused");
                    checks.require(Files.readString(target.resolve("existing.txt")).equals("previous restore state\n"), "nonempty-target.target_untouched", "existing file intact");
                    containers.createNetwork();
                    var volume = containers.createVolume("occupied");
                    run.shell().run(List.of("docker", "run", "--rm", "-v", volume + ":/data", Containers.HELPER_IMAGE, "sh", "-c", "echo occupied > /data/marker")).require("occupy");
                    checks.require(!containers.volumeEmpty(volume), "nonempty-target.volume_refused", "a nonempty volume is reported occupied and never extracted into");
                }
                case "wrong-endpoint", "new-generation", "missing-version", "corrupt-descriptor" -> fullRestoreCase(name, run, copy, directory, options, checks, record, generation, realm);
                default -> throw new RehearsalFailure("Unknown negative case " + name);
            }
            checks.require(BackupSet.fingerprint(original).equals(before), name + ".original_backup_untouched", "original backup set unchanged");
            outcome.put("directory", directory.toString());
        } catch (IOException failure) {
            throw new RehearsalFailure("Negative case I/O failure", failure);
        } finally {
            finish(run, options);
        }
    }

    static void expectRefusal(Run run, Path copy, Path directory, Options options, Checks checks, String expectedText, String check) {
        try {
            restore(run, directory.resolve("restore"), copy, options, checks);
            checks.require(false, check, "restore did not refuse");
        } catch (RehearsalFailure refusal) {
            checks.require(refusal.getMessage().toLowerCase().contains(expectedText.toLowerCase()) && run.containers().created().isEmpty(), check,
                    "refused: " + refusal.getMessage() + "; resources created=" + run.containers().created().size());
        }
    }

    @SuppressWarnings("unchecked")
    static void fullRestoreCase(String name, Run run, Path copy, Path directory, Options options, Checks checks, Map<String, Object> record, String generation, String realm) {
        var restored = restore(run, directory.resolve("restore"), copy, options, checks);
        var env = hostEnvironment(run, restored.postgres(), restored.provider().endpoint(), generation, realm, options);
        env.put("PROTOMOLT_RECOVERY_IDENTITIES", copy.resolve("identities").toString());
        env.put("PROTOMOLT_RECOVERY_MANIFEST", BackupSet.manifest(copy).toString());
        env.put("PROTOMOLT_RECOVERY_OUT", directory.resolve("recovered").toString());
        env.put("PROTOMOLT_RECOVERY_MODE", name);
        switch (name) {
            case "wrong-endpoint" -> env.put("PROTOMOLT_RECOVERY_S3_ENDPOINT", "http://127.0.0.1:" + Containers.reservePort());
            case "new-generation" -> env.put("PROTOMOLT_RECOVERY_GENERATION", "recovery-other-" + UUID.randomUUID());
            case "missing-version" -> {
                var objects = (List<Map<String, Object>>) record.get("documentObjects");
                var core = objects.stream().filter(object -> Json.number(object, "part") == 1 && Json.string(object, "revisionId")
                        .equals(Json.string(((List<Map<String, Object>>) record.get("revisions")).getFirst(), "revisionId"))).findFirst().orElseThrow();
                deleteVersion(restored.provider(), run, Json.string(core, "namespace"), Json.string(core, "key"), Json.string(core, "providerVersion"));
                checks.pass("missing-version.injected", "deleted recorded version " + Json.string(core, "providerVersion") + " of " + Json.string(core, "key") + " on the disposable restored provider");
            }
            case "corrupt-descriptor" -> {
                String artifact = Json.string(record, "schemaArtifactSha256");
                try (var ledger = new Ledger(restored.postgres().jdbcUrl(), restored.postgres().user(), restored.postgres().password())) {
                    ledger.execute("ALTER TABLE repository_schema_artifacts DISABLE TRIGGER ALL");
                    ledger.execute("UPDATE repository_schema_artifacts SET artifact_bytes = sha256(artifact_bytes) || artifact_bytes, artifact_sha256 = sha256(sha256(artifact_bytes) || artifact_bytes) WHERE encode(artifact_sha256,'hex') = '" + artifact + "'");
                    ledger.execute("ALTER TABLE repository_schema_artifacts ENABLE TRIGGER ALL");
                    checks.require(ledger.schemaCatalog().keySet().stream().noneMatch(key -> key.endsWith("/" + artifact)), "corrupt-descriptor.injected",
                            "retained descriptor artifact " + artifact + " replaced by damaged bytes on the disposable restored ledger");
                }
            }
            default -> throw new IllegalStateException(name);
        }
        var recovered = host(run, RecoveredHost.class.getName(), env, directory.resolve("recovered-host.log"), 600);
        String marker = "RECOVERED_HOST_" + name.toUpperCase().replace('-', '_') + "_OK";
        checks.require(recovered.exitCode() == 0 && recovered.stdout().contains(marker), name + ".observed", "recovered host exit=" + recovered.exitCode() + " marker=" + marker);
        checks.require(!Files.exists(directory.resolve("recovered/READY")), name + ".not_ready", "no READY advertised");
    }

    static void deleteVersion(Containers.Provider provider, Run run, String bucket, String key, String version) {
        try (var client = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(java.net.URI.create(provider.endpoint()))
                .region(software.amazon.awssdk.regions.Region.US_EAST_1).forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(run.s3Access(), run.s3Secret()))).build()) {
            client.deleteObject(builder -> builder.bucket(bucket).key(key).versionId(version));
            run.shell().run(List.of("true", "#", "s3 DeleteObject", bucket, key, "versionId=" + version));
        }
    }

    static void finish(Run run, Options options) {
        for (String container : run.containers().created()) {
            if (container.startsWith("container:")) {
                String name = container.substring("container:".length());
                run.containers().writeLogs(name, run.directory().resolve("docker-" + name.substring(name.lastIndexOf("-", name.lastIndexOf("-") - 1) + 1) + ".log"));
            }
        }
        run.checks().writeXml(run.directory().resolve("results.xml"));
        if (!options.keep()) run.containers().cleanup();
    }
}
