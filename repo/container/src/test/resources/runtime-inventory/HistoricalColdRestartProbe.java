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
    static DocumentReadLedger writerReads(Tx tx) {
        var host = UUID.fromString(System.getenv("PROTOMOLT_TEST_COLD_HOST_EXECUTION"));
        ReaderHostExecutions.register(tx, host, "cold-restart-driver", System.getenv("PROTOMOLT_TEST_COLD_HOST_BOOT"));
        return new DocumentReadLedger(tx, UUID.randomUUID(), host);
    }

    static void checkpointAndHalt
(Tx tx, DocumentPublicationCommand command, RepositoryCaller caller,
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
            if ("true".equals(System.getenv("PROTOMOLT_TEST_COLD_MANAGED_DISPATCH"))) {
                ManagedHistoricalColdDispatchProbe.run(tx, provider, caller, command, uploads);
                reclaimOriginalWriter(tx, coordinator, command, budget);
            } else if ("true".equals(System.getenv("PROTOMOLT_TEST_COLD_PUBLIC_DISPATCH"))) {
                HistoricalPublicColdDispatchProbe.run(tx, provider, caller, command, uploads, budget);
                reclaimOriginalWriter(tx, coordinator, command, budget);
            } else {
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
                    reclaimOriginalWriter(tx, coordinator, prepared.plan().previous().command(), budget);
                }
            }
            require(budget.reservedBytes() == 0, "fresh process releases metadata");
            require(new DocumentPublicationReplay(tx).observe(caller, command).result().isPresent(), "durable restart receipt");
            require(count(tx, "document_revision_commits", command.operationId()) == 1, "exactly one cold recovery publication");
            require(count(tx, "document_assessment_owners", command.operationId()) == 1, "exactly one cold recovery assessment");
            require(count(tx, "repository_publication_assessment_starts", command.operationId()) == 2,
                    "original and recovered generations each retain one START");
            require(count(tx, "repository_successor_installs", command.operationId()) == (phase.equals("installed") ? 2 : 1),
                    "fresh process installs exactly one successor");
            require(count(tx, "repository_coordinator_supersessions", command.operationId()) == (phase.equals("initial") ? 0 : 1),
                    "unactivated crash recovery supersedes exactly once");
            require(count(tx, "repository_historical_activations", command.operationId()) == 1, "only the fresh process activates");
            observation.identity(() -> {});
        }
        System.out.println("HISTORICAL_COLD_PROCESS_RESTART_OK");
    }

    private static void reclaimOriginalWriter(Tx tx, RepositoryCaller coordinator,
            DocumentPublicationCommand command, PayloadBudget budget) {
        // Fixture-only archive read after publication: the reservation loader is no
        // longer usable after terminal retirement. Decode the immutable original
        // preparation with the same digest/command/nonce checks as runtime loading.
        try (var scratch = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES)) {
            var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), coordinator.principalName(), command.operationId());
            var rows = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT p.preparation_bytes,p.preparation_sha256,p.owner_nonce,p.command_sha256,p.predecessor_generation
                    FROM repository_publication_preparations p JOIN repository_preparation_pin_batches b
                    USING(account_id,principal,operation_id,predecessor_generation)
                    WHERE p.account_id=:account AND p.principal=:principal AND p.operation_id=:op
                    AND b.initial_capture AND b.sealed AND octet_length(p.preparation_bytes) BETWEEN 1 AND :maximum
                    """).setParameter("account", key.account()).setParameter("principal", key.principal())
                    .setParameter("op", key.operationId()).setParameter("maximum", DocumentPublicationPreparationCodec.MAX_BYTES)
                    .setMaxResults(2).getResultList());
            require(rows.size() == 1, "exact original retained preparation from SQL");
            var row = (Object[]) rows.getFirst();
            var original = DocumentPublicationPreparationJournal.decode(row, ((byte[]) row[0]).length, key,
                    command.sha256(), ((Number) row[4]).longValue());
            require(original.predecessorGeneration() == 0, "fixture original retention is generation zero");
            reclaimWriter(tx, coordinator, original, budget);
        }
    }

    private static void reclaimWriter(Tx tx, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord record, PayloadBudget budget) {
        var host = UUID.fromString(System.getenv("PROTOMOLT_TEST_COLD_HOST_EXECUTION"));
        var identities = tx.readOnly(em -> em.createNativeQuery("""
                SELECT r.incarnation,r.registration_nonce,t.receipt_id FROM repository_reader_incarnations r
                JOIN repository_reader_host_terminations t ON t.execution=r.host_execution
                WHERE r.host_execution=:host AND r.state='ACTIVE'
                """).setParameter("host", host).getResultList());
        require(identities.size() == 1, "exact crashed reader remains protected after successor publication");
        var identity = (Object[]) identities.getFirst();
        UUID reader = (UUID) identity[0];
        long pinned = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_read_pins WHERE reader_incarnation=:reader")
                .setParameter("reader", reader).getSingleResult()).longValue());
        require(pinned > 0, "crashed writer still owns native pins");
        long captured = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_preparation_source_pins p JOIN repository_preparation_pin_batches b
                USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                WHERE p.account_id=:account AND p.principal=:principal AND p.operation_id=:op
                AND b.initial_capture AND b.sealed AND p.reader_incarnation=:reader
                """).setParameter("account", record.key().account()).setParameter("principal", record.key().principal())
                .setParameter("op", record.key().operationId()).setParameter("reader", reader).getSingleResult()).longValue());
        require(captured == pinned, "original sealed source pins belong to exact crashed reader");

        try {
            DocumentPreparationRootReleases.release(tx, budget, coordinator, record, RepositoryReadControl.NONE);
            throw new AssertionError("Undrained writer allowed preparation root release");
        } catch (RepositoryException expected) {
            require(expected.getMessage().contains("has not drained"), "root release failed for exact undrained capture");
        }
        long recovered = 0;
        boolean drained = false;
        var recovery = new ReaderHostRecovery(tx);
        for (int batch = 0; batch < 10001; batch++) {
            var page = recovery.recoverPage(host, (UUID) identity[2], Optional.empty(), 1, 1, RepositoryReadControl.NONE);
            require(page.recovered().size() == 1 && page.recovered().getFirst().reader().equals(reader),
                    "supervisor discovers only the exact crashed reader");
            var result = page.recovered().getFirst();
            require(result.archivePins() == 0 && result.assessmentSessions() == 0, "crashed writer owns native document pins only");
            if (result.readerResourcesDrained()) { drained = true; break; }
            recovered += result.documentPins();
        }
        require(drained && recovered == pinned, "bounded supervisor drains exactly the crashed reader pins");
        var quiescence = new ReaderExternalQuiescence(tx);
        var receipt = quiescence.quiesce(reader, (UUID) identity[1], host, (UUID) identity[2]);
        require(quiescence.quiesce(reader, (UUID) identity[1], host, (UUID) identity[2]).equals(receipt), "reader receipt replays");
        var captures = tx.readOnly(em -> em.createNativeQuery("""
                SELECT b.predecessor_generation,b.pins_sha256,o.claim_epoch,o.claim_token,o.incarnation
                FROM repository_preparation_pin_batches b JOIN repository_preparation_pin_owners o
                USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                WHERE b.account_id=:account AND b.principal=:principal AND b.operation_id=:op AND b.initial_capture AND b.sealed
                """).setParameter("account", record.key().account()).setParameter("principal", record.key().principal()).setParameter("op", record.key().operationId()).getResultList());
        require(captures.size() == 1, "one sealed original capture");
        var capture = (Object[]) captures.getFirst();
        var drainIdentity = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(
                record.key(), record.command().sha256(), ((Number) capture[2]).longValue(), (UUID) capture[3], (UUID) capture[4]),
                ((Number) capture[0]).longValue(), HexFormat.of().formatHex((byte[]) capture[1]));
        require(DocumentPreparationCaptureDrain.recover(tx, coordinator, drainIdentity, RepositoryReadControl.NONE).kind().equals("QUIESCED"),
                "original capture records proven quiescence after pin recovery");
        var released = DocumentPreparationRootReleases.release(tx, budget, coordinator, record, RepositoryReadControl.NONE);
        require(DocumentPreparationRootReleases.release(tx, budget, coordinator, record, RepositoryReadControl.NONE).equals(released),
                "root release receipt replays");
        require(count(tx, "repository_preparation_history_roots", record.key().operationId()) == 0, "historical preparation roots released");
        require(new DocumentPublicationReplay(tx).observe(coordinator, record.command()).result().isPresent(), "publication receipt remains after orphan cleanup");
        System.out.println("HISTORICAL_COLD_ORPHAN_CAPTURE_RECLAIMED_OK");
        System.out.println("HISTORICAL_COLD_HOST_SUPERVISOR_OK");
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
