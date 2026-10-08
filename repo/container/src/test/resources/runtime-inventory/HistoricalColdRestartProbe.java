package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Recovery receives request bytes only; retained metadata and historical bytes come from storage. */
public final class HistoricalColdRestartProbe {
    static void checkpointAndHalt(Tx tx, DocumentPublicationCommand command, RepositoryCaller caller,
            Map<Integer, ByteString> fragments, PayloadBudget budget) throws Exception {
        require(!caller.processAuthority(), "writer uses scoped credential");
        String application = tx.readOnly(em -> (String) em.createNativeQuery("SELECT current_setting('application_name')").getSingleResult());
        require(application.equals(System.getenv("PROTOMOLT_TEST_COLD_WRITER_APP")), "writer SQL sessions have unique host identity");
        String phase = phase();
        if (!phase.equals("initial")) {
            waitExpired(tx, command);
            var coordinator = new RepositoryCaller(caller.principalName(), true);
            var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
            var timeouts = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
            var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, timeouts)
                    .inspect(coordinator, key, command.sha256(), RepositoryReadControl.NONE);
            require(observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND, "writer predecessor expired");
            var modes = Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
            var bodies = new HashMap<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.codec.PartObject>();
            var member = command.intent().getMembers(0);
            for (int i = 0; i < member.getPartsCount(); i++) {
                var part = member.getParts(i);
                if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key(member.getMemberId(), i),
                        new ai.protomolt.proto.repo.codec.PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                fragments.get(i).toByteArray(), part.getUpload().getSha256()));
            }
            // These live objects are intentionally abandoned by the process crash below.
            var attempts = new RepositoryInstalledHistoricalAttempts(tx, budget, new DriveLedger(tx), 1);
            var attempt = attempts.beginColdProposed(caller, command, modes, observed, Duration.ofSeconds(10), timeouts);
            require(attempt.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                    == RepositoryHistoricalAttemptPreparation.Phase.RESERVED, "writer committed recovery reservation");
            if (phase.equals("installed")) require(attempt.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                    == RepositoryHistoricalAttemptPreparation.Phase.INSTALLED, "writer committed unactivated installation");
        }
        var state = new Properties();
        state.setProperty("command", Base64.getEncoder().encodeToString(command.intent().toByteArray()));
        var parts = command.intent().getMembers(0).getPartsList();
        for (int i = 0; i < parts.size(); i++) if (parts.get(i).hasUpload())
            state.setProperty("upload." + i, Base64.getEncoder().encodeToString(fragments.get(i).toByteArray()));
        Path file = Path.of(System.getenv("PROTOMOLT_TEST_COLD_REQUEST"));
        Files.createFile(file, java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        try (var out = Files.newOutputStream(file)) { state.store(out, "Caller request and resubmitted upload bytes"); }
        System.out.println("HISTORICAL_COLD_WRITER_CHECKPOINT_OK");
        System.out.println("HISTORICAL_COLD_WRITER_PHASE_" + phase);
        System.out.flush();
        Runtime.getRuntime().halt(23); // No finally blocks, reader drain, or graceful owner shutdown.
        throw new AssertionError("Writer halt returned");
    }

    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.junit.jupiter.api.Test");
            throw new AssertionError("Test framework leaked into restart host");
        } catch (ClassNotFoundException expected) { }
        Path file = Path.of(System.getenv("PROTOMOLT_TEST_COLD_REQUEST"));
        require(Files.size(file) <= 2_000_000, "bounded caller request");
        var state = new Properties();
        try (var input = Files.newInputStream(file)) { state.load(input); }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(
                Base64.getDecoder().decode(Objects.requireNonNull(state.getProperty("command")))));
        require(command.intent().getMembersCount() == 1, "one mixed member");
        var member = command.intent().getMembers(0);
        var expectedKeys = new HashSet<String>(Set.of("command"));
        var uploads = new HashMap<Integer, ByteString>();
        for (int i = 0; i < member.getPartsCount(); i++) if (member.getParts(i).hasUpload()) {
            String key = "upload." + i;
            expectedKeys.add(key);
            uploads.put(i, ByteString.copyFrom(Base64.getDecoder().decode(Objects.requireNonNull(state.getProperty(key)))));
        }
        require(state.stringPropertyNames().equals(expectedKeys), "handoff contains no retained metadata or authority");
        require(uploads.size() == 1 && member.getPartsList().stream().anyMatch(DocumentPublicationPart::hasHistoricalReuse),
                "request combines resubmitted and historical data");
        // Synthetic authentication binding is injected by the trusted test host, independently of request data.
        var credential = new RepositoryCredentialBinding("historical-create",
                UUID.fromString(System.getenv("PROTOMOLT_TEST_COLD_CREDENTIAL")), 1);
        var caller = new RepositoryCaller("scoped-create", false, Set.of("account"), Set.of(), Optional.of(credential));
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {});
        try (var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")));
             var provider = new AssessmentProviderProbe(false)) {
            var tx = new Tx(database.entityManagerFactory());
            tx.inTransaction(em -> { RepositoryCredentialAuthorities.requireLive(em, caller); });
            var replay = new DocumentPublicationReplay(tx).observe(caller, command);
            require(replay.result().isEmpty() && replay.rejection().isEmpty(), "writer did not publish or reject");
            long captures = count(tx, "repository_preparation_history_sets", command.operationId());
            require(captures == 1 && count(tx, "repository_publication_assessment_starts", command.operationId()) == 1,
                    "writer committed initial capture lineage and START");
            String phase = phase();
            require(count(tx, "repository_successor_installs", command.operationId()) == (phase.equals("installed") ? 1 : 0),
                    "writer installation count matches crash boundary");
            require(count(tx, "repository_coordinator_expirations", command.operationId()) == (phase.equals("initial") ? 0 : 1),
                    "writer reservation count matches crash boundary");
            require(count(tx, "repository_historical_activations", command.operationId()) == 0, "writer activated no successor");
            waitExpired(tx, command);
            var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
            var expected = switch (phase) {
                case "initial" -> RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND;
                case "reserved" -> RepositoryCoordinatorRecoveryDiscovery.Status.RESERVED_NOT_INSTALLED;
                case "installed" -> RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED;
                default -> throw new AssertionError("Unknown phase");
            };
            var discovered = new RepositoryCoordinatorRecoveryDiscovery(tx, new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5)))
                    .inspect(coordinator, key, command.sha256(), RepositoryReadControl.NONE);
            require(discovered.status() == expected, "fresh process discovers exact persisted phase");
            var budget = new PayloadBudget(128_000_000);
            try (var prepared = HistoricalInstalledOwnerProbe.prepareCold(tx, caller, coordinator, command, uploads, budget)) {
                var selector = member.getPartsList().stream().filter(DocumentPublicationPart::hasHistoricalReuse)
                        .findFirst().orElseThrow().getHistoricalReuse();
                var reads = new DocumentReadLedger(tx, UUID.randomUUID());
                var history = reads.captureHistorical(caller, selector.getSource(), UUID.fromString(selector.getRevisionId()));
                try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                     var accepted = sources.work()) {
                    var fragments = new HashMap<Integer, ByteString>(uploads);
                    int providerReads = 0;
                    try (var use = history.use()) {
                        for (var entry : use.plan().entries()) {
                            var part = entry.part();
                            require(part.binding().generation().equals("assessment-s3") && part.binding().profile().equals(provider.profile()),
                                    "source uses exact retained backend identity");
                            for (int i = 0; i < member.getPartsCount(); i++) {
                                var declaration = member.getParts(i);
                                if (!declaration.hasHistoricalReuse() || !declaration.getHistoricalReuse().getObject().getObjectId()
                                        .equals(entry.objectId().toString())) continue;
                                var bytes = provider.store().getBounded(part.binding().namespace(), part.part().key(),
                                        part.part().providerVersion(), Math.toIntExact(part.part().size())).data();
                                require(bytes.length == part.part().size() && ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(bytes)
                                        .equals(part.part().sha256()), "fresh GET matches retained version checksum");
                                fragments.put(i, ByteString.copyFrom(bytes)); providerReads++;
                            }
                        }
                    }
                    require(providerReads > 0 && fragments.size() == member.getPartsCount(), "all historical bytes read after restart");
                    var resolutions = new AtomicInteger();
                    var freshDefinition = ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
                    DocumentPublicationCandidate.Resolver resolver = (selected, occurrence) -> {
                        require(selected.getParts(occurrence.ordinal()).hasUpload(), "historical schemas must use retained descriptors");
                        resolutions.incrementAndGet();
                        return freshDefinition;
                    };
                    HistoricalInstalledOwnerProbe.run(tx, provider, caller, coordinator, prepared.plan().previous(), prepared,
                            sources, accepted, new DocumentSchemaPolicies(tx).read("account", () -> {}), fragments,
                            Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())), resolver,
                            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), budget, observation,
                            null, database.dataSource(), HistoricalInstalledOwnerProbe.Check.COLD);
                    require(resolutions.get() == 1, "only the resubmitted upload resolves a fresh schema");
                } finally {
                    history.close();
                    require(history.awaitDrained(Duration.ofSeconds(1)), "fresh history worker drains");
                    history.release(); reads.fence(); reads.attestLocalQuiescence();
                    require(reads.outstandingReads() == 0, "fresh process releases its reads");
                }
            }
            require(budget.reservedBytes() == 0, "fresh process releases metadata");
            require(new DocumentPublicationReplay(tx).observe(caller, command).result().isPresent(), "durable restart receipt");
            require(count(tx, "repository_successor_installs", command.operationId()) == (phase.equals("installed") ? 2 : 1),
                    "fresh process installs exactly one successor");
            require(count(tx, "repository_coordinator_supersessions", command.operationId()) == (phase.equals("initial") ? 0 : 1),
                    "unactivated crash recovery supersedes exactly once");
            require(count(tx, "repository_historical_activations", command.operationId()) == 1, "only the fresh process activates");
            observation.identity(() -> {});
        }
        System.out.println("HISTORICAL_COLD_PROCESS_RESTART_OK");
    }

    private static long count(Tx tx, String table, UUID operation) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                .setParameter("op", operation).getSingleResult()).longValue());
    }
    private static String phase() {
        String phase = System.getenv("PROTOMOLT_TEST_COLD_PHASE");
        require(Set.of("initial", "reserved", "installed").contains(phase), "known crash phase");
        return phase;
    }
    private static void waitExpired(Tx tx, DocumentPublicationCommand command) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:op
                """).setParameter("op", command.operationId()).getSingleResult());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
