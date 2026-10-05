package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** Test-only request file, never a credential format or a public repository API. */
public final class AssessmentRestartProbe {
    static void persist(Path file, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String, DocumentAssessmentRetainedSlots.UploadSelection> selections, DocumentAssessmentCreation.Created retained) throws Exception {
        var fields = new Properties();
        fields.setProperty("command", Base64.getEncoder().encodeToString(command.intent().toByteArray()));
        fields.setProperty("account", owner.key().account());
        fields.setProperty("principal", owner.key().principal());
        fields.setProperty("operation", owner.key().operationId().toString());
        fields.setProperty("generation", Long.toString(owner.generation()));
        fields.setProperty("nonce", owner.token().toString());
        fields.setProperty("leaseUntil", owner.leaseUntil().toString());
        fields.setProperty("assessment", retained.assessment().toString());
        fields.setProperty("manifest", retained.manifestSha256());
        fields.setProperty("deadline", retained.retainUntil().toString());
        fields.setProperty("selectionCount", Integer.toString(selections.size()));
        int index = 0;
        for (var entry : new java.util.TreeMap<>(selections).entrySet()) {
            String prefix = "selection." + index++ + ".";
            fields.setProperty(prefix + "key", entry.getKey());
            fields.setProperty(prefix + "member", entry.getValue().member());
            fields.setProperty(prefix + "revision", Long.toString(entry.getValue().revision()));
            fields.setProperty(prefix + "attempt", entry.getValue().attempt().toString());
        }
        Files.createFile(file, java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        try (var output = Files.newOutputStream(file)) { fields.store(output, "Internal restart request fixture"); }
    }

    public static void main(String[] args) throws Exception {
        var file = Path.of(args[0]);
        if (Files.size(file) > 1_048_576) throw new AssertionError("Oversized restart fixture");
        var fields = new Properties();
        try (var input = Files.newInputStream(file)) { fields.load(input); }
        // The parent test waits for the exact writer Process to exit before launch.
        // Looking up its PID here would race operating-system PID reuse.
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(
                Base64.getDecoder().decode(required(fields, "command"))));
        var owner = new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key(required(fields, "account"),
                required(fields, "principal"), UUID.fromString(required(fields, "operation"))),
                Long.parseLong(required(fields, "generation")), UUID.fromString(required(fields, "nonce")),
                Instant.parse(required(fields, "leaseUntil")));
        var selections = new HashMap<String, DocumentAssessmentRetainedSlots.UploadSelection>();
        int count = Integer.parseInt(required(fields, "selectionCount"));
        if (count < 1 || count > 10000) throw new AssertionError("Invalid selection count");
        for (int index = 0; index < count; index++) {
            String prefix = "selection." + index + ".";
            var selection = new DocumentAssessmentRetainedSlots.UploadSelection(required(fields, prefix + "member"),
                    Long.parseLong(required(fields, prefix + "revision")), UUID.fromString(required(fields, prefix + "attempt")));
            if (selections.put(required(fields, prefix + "key"), selection) != null) throw new AssertionError("Duplicate selection");
        }
        var retained = new DocumentAssessmentCreation.Created(UUID.fromString(required(fields, "assessment")),
                required(fields, "manifest"), Instant.parse(required(fields, "deadline")));
        // Trusted host identity is supplied independently of the persisted request.
        var caller = new RepositoryCaller("principal", true);
        var budget = new PayloadBudget(64_000_000);
        try (var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")))) {
            var tx = new Tx(database.entityManagerFactory());
            var actual = new DocumentAssessmentReconciliation(tx).observeRetained(caller, owner, command, selections,
                    retained.assessment(), retained.manifestSha256(), retained.retainUntil(), budget, () -> {}).orElseThrow();
            if (!actual.equals(retained)) throw new AssertionError("Restart changed assessment identity");
            long publications = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                    .setParameter("op", command.operationId()).getSingleResult()).longValue());
            if (publications != 0) throw new AssertionError("Acknowledgement published candidate");
            verifyRevokedAccess(tx, owner, command, selections, retained, budget);
            verifyExpiredOwner(tx, owner, command, selections, retained, budget);
        }
        if (budget.reservedBytes() != 0) throw new AssertionError("Restart leaked payload reservations");
        System.out.println("RESTARTED_ASSESSMENT_ACK_OK");
    }

    private static void verifyExpiredOwner(Tx tx, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String, DocumentAssessmentRetainedSlots.UploadSelection> selections, DocumentAssessmentCreation.Created retained,
            PayloadBudget budget) throws Exception {
        // Observe real database time. Never rewrite a lease or disable fencing triggers.
        long stop = System.nanoTime() + java.time.Duration.ofSeconds(65).toNanos();
        while (!tx.readOnly(em -> (Boolean) em.createNativeQuery("""
                SELECT lease_until<=clock_timestamp() FROM repository_operation_owners
                WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).getSingleResult())) {
            if (System.nanoTime() >= stop) throw new AssertionError("Owner did not expire within fixture bound");
            Thread.sleep(100);
        }
        var caller = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
        var handler = new DocumentAssessmentReconciliation(tx);
        expectFenced(handler, caller, owner, command, selections, retained, budget);
        var replacement = new RepositoryOperationLedger(tx).takeOver(owner.key(), command, owner.generation(),
                UUID.randomUUID(), java.time.Duration.ofMinutes(1));
        if (replacement.generation() != owner.generation() + 1) throw new AssertionError("Takeover did not advance generation");
        expectFenced(handler, caller, owner, command, selections, retained, budget);
        if (handler.observeRetained(caller, replacement, command, selections, retained.assessment(), retained.manifestSha256(),
                retained.retainUntil(), budget, () -> {}).isPresent())
            throw new AssertionError("New generation adopted old assessment");
        Object[] stored = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT o.owner_generation, o.release_xid IS NULL, o.retain_until>clock_timestamp(),
                    (SELECT count(*) FROM document_assessment_slot_snapshots s WHERE s.assessment_id=o.assessment_id),
                    (SELECT count(*) FROM document_assessment_objects r WHERE r.assessment_id=o.assessment_id), o.sealed,
                    (SELECT count(*) FROM document_assessment_objects a JOIN repository_object_references r
                        ON r.object_id=a.object_id AND r.owner_kind='ASSESSMENT' AND r.owner_id=a.assessment_id
                        AND r.owner_revision=1 WHERE a.assessment_id=o.assessment_id)
                FROM document_assessment_owners o WHERE o.assessment_id=:id
                """).setParameter("id", retained.assessment()).getSingleResult());
        if (((Number) stored[0]).longValue() != owner.generation() || !Boolean.TRUE.equals(stored[1])
                || !Boolean.TRUE.equals(stored[2]) || ((Number) stored[3]).intValue() != 1 || ((Number) stored[4]).intValue() < 1
                || !Boolean.TRUE.equals(stored[5]) || ((Number) stored[6]).intValue() != ((Number) stored[4]).intValue())
            throw new AssertionError("Expiry or takeover changed retained ownership");
        Object[] outcomes = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT (SELECT count(*) FROM document_revision_commits WHERE operation_id=:op),
                       (SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op)
                """).setParameter("op", command.operationId()).getSingleResult());
        if (((Number) outcomes[0]).longValue() != 0 || ((Number) outcomes[1]).longValue() != 0)
            throw new AssertionError("Expiry or takeover created a publication or terminal rejection");
        if (budget.reservedBytes() != 0) throw new AssertionError("Owner fencing leaked reservations");
        System.out.println("RESTARTED_ASSESSMENT_OWNER_FENCE_OK");
    }

    private static void expectFenced(DocumentAssessmentReconciliation handler, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String, DocumentAssessmentRetainedSlots.UploadSelection> selections, DocumentAssessmentCreation.Created retained,
            PayloadBudget budget) {
        try {
            handler.observeRetained(caller, owner, command, selections, retained.assessment(), retained.manifestSha256(),
                    retained.retainUntil(), budget, () -> {});
            throw new AssertionError("Expired or replaced owner acknowledged assessment");
        } catch (RepositoryOperationLedger.OwnerFencedException expected) { /* exact fence failure */ }
    }

    static java.util.List<ai.protomolt.proto.repo.v1.DocumentPublicationMember> seedDestinations(Tx tx,
            java.util.List<ai.protomolt.proto.repo.v1.DocumentPublicationMember> members) {
        String readPolicy = "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}";
        // Controlled SQL authorization fixture, not publication of the staged candidate.
        // These current destination rows allow a non-process caller to be checked.
        return tx.inTransaction(em -> {
            var sampled = new java.util.ArrayList<ai.protomolt.proto.repo.v1.DocumentPublicationMember>();
            for (var member : members) {
                var address = member.getDestination().getAddress();
                var row = new DocumentRecord();
                row.nodeId = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address);
                row.accountId = address.getAccountId(); row.docId = address.getDocId();
                row.graphId = address.getGraphId(); row.graphAddressId = address.getGraphAddressId();
                row.rowKind = DocumentRowKind.PIPELINE; row.datasourceId = member.getOwnership().getDatasourceId();
                row.checksum = "authorization-fixture"; row.driveName = "creation-" + member.getDriveId();
                row.etag = "authorization-fixture"; row.sizeBytes = 0L;
                row.objectKey = "authorization-fixture/" + row.nodeId;
                row.security = readPolicy;
                em.persist(row);
                em.flush(); em.refresh(row);
                sampled.add(member.toBuilder().setDestination(member.getDestination().toBuilder()
                        .setExpectedMutationRevision(row.mutationRevision)).build());
            }
            return java.util.List.copyOf(sampled);
        });
    }

    private static void verifyRevokedAccess(Tx tx, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String, DocumentAssessmentRetainedSlots.UploadSelection> selections, DocumentAssessmentCreation.Created retained,
            PayloadBudget budget) throws Exception {
        String readPolicy = "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}";
        var caller = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
        var handler = new DocumentAssessmentReconciliation(tx);
        if (!handler.observeRetained(caller, owner, command, selections, retained.assessment(), retained.manifestSha256(),
                retained.retainUntil(), budget, () -> {}).orElseThrow().equals(retained)) throw new AssertionError("Granted read failed");
        var denied = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(command.intent().getMembers(1).getDestination().getAddress());
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_DENY\"}]}")
                    .setParameter("node", denied).executeUpdate();
            if (changed != 1) throw new AssertionError("Revocation fixture missed destination");
        });
        for (String digest : java.util.List.of(retained.manifestSha256(), "00".repeat(32))) {
            try {
                handler.observeRetained(caller, owner, command, selections, retained.assessment(), digest,
                        retained.retainUntil(), budget, () -> {});
                throw new AssertionError("Revoked caller received assessment");
            } catch (ai.protomolt.proto.repo.spi.RepositoryException expected) {
                if (expected.code() != ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND)
                    throw new AssertionError("Revocation disclosed assessment identity", expected);
            }
        }
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", readPolicy).setParameter("node", denied).executeUpdate();
            if (changed != 1) throw new AssertionError("Read grant missed destination");
        });
        if (!handler.observeRetained(caller, owner, command, selections, retained.assessment(), retained.manifestSha256(),
                retained.retainUntil(), budget, () -> {}).orElseThrow().equals(retained))
            throw new AssertionError("Revocation damaged retained evidence");
        if (budget.reservedBytes() != 0) throw new AssertionError("Authorization path leaked reservations");
        verifyCaptureRevocationRace(tx, caller, owner, command, selections, retained, budget, denied, readPolicy);
        System.out.println("RESTARTED_ASSESSMENT_REVOCATION_OK");
    }

    private static void verifyCaptureRevocationRace(Tx tx, RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Map<String, DocumentAssessmentRetainedSlots.UploadSelection> selections,
            DocumentAssessmentCreation.Created retained, PayloadBudget budget, UUID denied, String readPolicy) throws Exception {
        UUID reader = UUID.randomUUID();
        var ledger = new DocumentReadLedger(tx, reader, 1);
        var pending = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.Future<DocumentReadLedger.PinnedAssessment>>();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            // Keep the policy update uncommitted until capture demonstrably waits on it.
            tx.inTransaction(em -> {
                int holder = ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                        .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_DENY\"}]}")
                        .setParameter("node", denied).executeUpdate();
                if (changed != 1) throw new AssertionError("Capture revocation missed destination");
                pending.set(executor.submit(() -> ledger.captureAssessment(caller, owner, command, selections,
                        retained.assessment(), retained.manifestSha256(), retained.retainUntil(), budget, () -> {})));
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
                while (!tx.readOnly(observer -> !observer.createNativeQuery("""
                        SELECT pid FROM pg_stat_activity WHERE :holder=ANY(pg_blocking_pids(pid))
                            AND wait_event_type='Lock'
                        """).setParameter("holder", holder).getResultList().isEmpty())) {
                    if (System.nanoTime() >= deadline) throw new AssertionError("Capture did not wait on policy update");
                    try { Thread.sleep(10); }
                    catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted);
                    }
                }
            });
            try {
                pending.get().get(10, java.util.concurrent.TimeUnit.SECONDS).close();
                throw new AssertionError("Capture accepted a concurrently revoked destination");
            } catch (java.util.concurrent.ExecutionException failure) {
                if (!(failure.getCause() instanceof ai.protomolt.proto.repo.spi.RepositoryException rejected)
                        || rejected.code() != ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND)
                    throw new AssertionError("Capture revocation must refuse without disclosing evidence", failure);
            }
            if (ledger.outstandingReads() != 0 || budget.reservedBytes() != 0)
                throw new AssertionError("Revoked capture leaked capacity or verification memory");
            long sessions = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_read_sessions WHERE reader_incarnation=:reader")
                    .setParameter("reader", reader).getSingleResult()).longValue());
            if (sessions != 0) throw new AssertionError("Revoked capture retained a session");
        } finally {
            tx.inTransaction(em -> {
                int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                        .setParameter("policy", readPolicy).setParameter("node", denied).executeUpdate();
                if (changed != 1) throw new AssertionError("Capture race restoration missed destination");
            });
        }
        System.out.println("RESTARTED_ASSESSMENT_CAPTURE_REVOCATION_OK");
    }

    private static String required(Properties fields, String name) {
        var value = fields.getProperty(name);
        if (value == null || value.isBlank()) throw new AssertionError("Missing restart field: " + name);
        return value;
    }
}
