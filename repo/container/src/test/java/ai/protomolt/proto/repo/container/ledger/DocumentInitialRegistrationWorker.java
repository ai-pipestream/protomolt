package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import com.zaxxer.hikari.*;
import java.nio.file.*;
import java.util.*;

/** Test-only SQL capability inspection; never a production recovery endpoint. */
public final class DocumentInitialRegistrationWorker {
    private static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of(
            "member-0", DocumentPublicationCandidate.Mode.TYPED, "member-1", DocumentPublicationCandidate.Mode.OPAQUE);
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        require(Set.of("write-before", "write-after", "read-before", "read-after",
                "write-modes-before", "write-modes-after", "read-modes-before", "read-modes-after",
                "write-owner-before", "write-owner-after", "read-owner-before", "read-owner-after").contains(mode), "known worker mode");
        boolean ownerAdmission = mode.contains("-owner-");
        boolean modeBinding = mode.contains("-modes-") || ownerAdmission;
        boolean committedModes = mode.equals("read-modes-after") || ownerAdmission;
        boolean committedOwner = mode.equals("read-owner-after");
        var key = new RepositoryOperationLedger.Key("account", "principal", UUID.fromString(args[1]));
        var config = new HikariConfig(); var env = System.getenv();
        config.setJdbcUrl(env.get("TEST_DB_URL")); config.setUsername(env.get("TEST_DB_USER"));
        config.setPassword(env.get("TEST_DB_PASSWORD")); config.setSchema(env.get("TEST_DB_SCHEMA"));
        config.setMaximumPoolSize(2);
        try (var pool = new HikariDataSource(config)) {
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            javax.sql.DataSource source = pool;
            if (mode.startsWith("write-")) {
                source = DocumentJdbcFaults.beforeCommit(DocumentJdbcFaults.afterCommit(pool, () -> {
                    if (armed.get()) Runtime.getRuntime().halt(82);
                }), connection -> {
                    try (var query = connection.prepareStatement("""
                            SELECT (SELECT count(*) FROM repository_execution_claims WHERE operation_id=?),
                                   (SELECT count(*) FROM repository_publication_preparations WHERE operation_id=?),
                                   (SELECT count(*) FROM repository_publication_modes WHERE operation_id=?),
                                   (SELECT count(*) FROM repository_operation_owners WHERE operation_id=?),
                                   (SELECT count(*) FROM repository_operations WHERE operation_id=?),
                                   (SELECT lease_until FROM repository_operation_owners WHERE operation_id=?)
                            """)) {
                        for (int i = 1; i <= 6; i++) query.setObject(i, key.operationId());
                        try (var rows = query.executeQuery()) {
                            require(rows.next(), "registration counts");
                            if (rows.getInt(1) == 1 && rows.getInt(2) == 1 && (!modeBinding || rows.getInt(3) == 1)
                                    && (!ownerAdmission || (rows.getInt(4) == 1 && rows.getInt(5) == 1))) {
                                if (ownerAdmission) {
                                    System.out.println("OWNER_COMMIT_LEASE|" + rows.getObject(6, java.time.OffsetDateTime.class).toInstant());
                                    System.out.flush();
                                }
                                armed.set(true);
                                if (mode.endsWith("-before")) Runtime.getRuntime().halt(81);
                            }
                        }
                    }
                });
            }
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var tx = new Tx(emf); var budget = new PayloadBudget(64_000_000);
                var journal = new DocumentPublicationPreparationJournal(tx, budget);
                var caller = new RepositoryCaller("principal", true);
                if (mode.startsWith("write-")) {
                    var record = DocumentPublicationPreparationCodec.decode(ByteString.copyFrom(Files.readAllBytes(Path.of(args[2]))), key, args[3]);
                    var claim = journal.acquireInitial(caller, record, UUID.fromString(args[4]), RepositoryReadControl.NONE);
                    if (modeBinding) {
                        System.out.println("INITIAL_LEASE|" + claim.leaseUntil()); System.out.flush();
                        new DocumentPublicationModesJournal(tx, budget).bind(caller, claim, 0, MODES, RepositoryReadControl.NONE);
                    }
                    if (ownerAdmission) new RepositoryOperationLedger(tx).admit(key, record.command(),
                            record.seeds().ownerNonce(), record.lease(), claim);
                    throw new AssertionError("Target commit hook did not halt");
                }
                require(args.length == 2, "Reader receives only mode and operation ID, no preparation or claim token");
                var claims = tx.readOnly(em -> em.createNativeQuery("""
                        SELECT encode(command_sha256,'hex'), claim_epoch, claim_token, lease_until
                        FROM repository_execution_claims WHERE operation_id=:id
                        """).setParameter("id", key.operationId()).getResultList());
                if (mode.equals("read-before")) {
                    require(claims.isEmpty(), "precommit death leaves no claim");
                    require(journal.readCommand(caller, key, 0, RepositoryReadControl.NONE).isEmpty(), "precommit death leaves no preparation");
                    System.out.println("REGISTRATION_ABSENT_OK");
                } else {
                    require(claims.size() == 1, "committed claim survives death");
                    var row = (Object[]) claims.getFirst();
                    var lease = (java.time.Instant) row[3];
                    var claim = new RepositoryExecutionClaimLedger.Claim(key, (String) row[0], ((Number) row[1]).longValue(), (UUID) row[2], lease);
                    System.out.println("RETAINED_LEASE|" + claim.leaseUntil());
                    var command = journal.readCommand(caller, key, 0, RepositoryReadControl.NONE).orElseThrow();
                    try (var loaded = journal.load(caller, claim, 0, RepositoryReadControl.NONE).orElseThrow()) {
                        require(command.intent().equals(loaded.record().command().intent()), "command/preparation agreement");
                        String digest = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                                .digest(DocumentPublicationPreparationCodec.encode(loaded.record()).toByteArray()));
                        require(DocumentPublicationRegistrationInspection.inspect(tx, budget, caller, claim, RepositoryReadControl.NONE)
                                == (committedOwner ? DocumentPublicationRegistrationInspection.Phase.OWNER_ADMITTED
                                        : committedModes ? DocumentPublicationRegistrationInspection.Phase.MODES_BOUND
                                        : DocumentPublicationRegistrationInspection.Phase.PREPARATION_ONLY), "exact registration phase");
                        var modeJournal = new DocumentPublicationModesJournal(tx, budget);
                        var storedModes = modeJournal.load(caller, claim, 0, RepositoryReadControl.NONE);
                        require(storedModes.equals(committedModes ? Optional.of(MODES) : Optional.empty()), "exact durable mode choices");
                        if (committedModes) {
                            // Retry only the map actually loaded from SQL; never invent missing choices.
                            modeJournal.bind(caller, claim, 0, storedModes.orElseThrow(), RepositoryReadControl.NONE);
                            require(modeJournal.load(caller, claim, 0, RepositoryReadControl.NONE).equals(storedModes), "mode retry preserves choices");
                        }
                        require(journal.acquireInitial(caller, loaded.record(), claim.token(), RepositoryReadControl.NONE).equals(claim),
                                "exact retry preserves claim epoch token and lease");
                        if (committedOwner) {
                            var ownerRow = (Object[]) tx.readOnly(em -> em.createNativeQuery("""
                                    SELECT owner_generation,owner_token,lease_until FROM repository_operation_owners WHERE operation_id=:id
                                    """).setParameter("id", key.operationId()).getSingleResult());
                            var originalOwner = new RepositoryOperationLedger.Owner(key, ((Number) ownerRow[0]).longValue(),
                                    (UUID) ownerRow[1], (java.time.Instant) ownerRow[2], Optional.of(claim));
                            require(originalOwner.generation() == 1 && originalOwner.token().equals(loaded.record().seeds().ownerNonce()),
                                    "owner binds original preparation nonce and first generation");
                            require(new DocumentOperationCommands(tx).load(caller, key.account(), key.operationId(), RepositoryReadControl.NONE)
                                    .orElseThrow().intent().equals(command.intent()), "admitted command is exactly the journaled command");
                            var retried = new RepositoryOperationLedger(tx).admit(key, command, loaded.record().seeds().ownerNonce(),
                                    loaded.record().lease(), claim).owner().orElseThrow();
                            require(retried.equals(originalOwner), "owner retry does not renew replace or advance ownership");
                            System.out.println("OWNER_RETAINED_LEASE|" + retried.leaseUntil());
                        }
                        System.out.println("REGISTRATION_RETAINED_OK|" + command.sha256() + "|" + digest);
                        if (modeBinding) System.out.println(committedModes ? "MODES_BOUND_RETAINED_OK" : "MODES_ABSENT_PREPARATION_RETAINED_OK");
                        if (ownerAdmission) System.out.println(committedOwner ? "OWNER_RETAINED_OK" : "OWNER_ABSENT_MODES_RETAINED_OK");
                    }
                }
                for (String table : List.of("repository_publication_modes", "repository_operation_owners", "repository_operations",
                        "repository_publication_assessment_starts", "repository_operation_success", "repository_operation_rejection",
                        "document_part_attempts", "document_operation_selection_attempts")) {
                    long count = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                            .setParameter("id", key.operationId()).getSingleResult()).longValue());
                    int expected = (committedModes && table.equals("repository_publication_modes"))
                            || (committedOwner && Set.of("repository_operation_owners", "repository_operations").contains(table)) ? 1 : 0;
                    require(count == expected,
                            "inspection must not advance " + table);
                }
                require(budget.reservedBytes() == 0, "all inspection reservations released");
            }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
