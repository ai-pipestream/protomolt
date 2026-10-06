package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import com.zaxxer.hikari.*;
import java.nio.file.*;
import java.util.*;

/** Test-only SQL capability inspection; never a production recovery endpoint. */
public final class DocumentInitialRegistrationWorker {
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        require(Set.of("write-before", "write-after", "read-before", "read-after").contains(mode), "known worker mode");
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
                                   (SELECT count(*) FROM repository_publication_preparations WHERE operation_id=?)
                            """)) {
                        query.setObject(1, key.operationId()); query.setObject(2, key.operationId());
                        try (var rows = query.executeQuery()) {
                            require(rows.next(), "registration counts");
                            if (rows.getInt(1) == 1 && rows.getInt(2) == 1) {
                                armed.set(true);
                                if (mode.equals("write-before")) Runtime.getRuntime().halt(81);
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
                    journal.acquireInitial(caller, record, UUID.fromString(args[4]), RepositoryReadControl.NONE);
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
                    var command = journal.readCommand(caller, key, 0, RepositoryReadControl.NONE).orElseThrow();
                    try (var loaded = journal.load(caller, claim, 0, RepositoryReadControl.NONE).orElseThrow()) {
                        require(command.intent().equals(loaded.record().command().intent()), "command/preparation agreement");
                        String digest = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                                .digest(DocumentPublicationPreparationCodec.encode(loaded.record()).toByteArray()));
                        require(DocumentPublicationRegistrationInspection.inspect(tx, budget, caller, claim, RepositoryReadControl.NONE)
                                == DocumentPublicationRegistrationInspection.Phase.PREPARATION_ONLY, "only preparation is registered");
                        require(journal.acquireInitial(caller, loaded.record(), claim.token(), RepositoryReadControl.NONE).equals(claim),
                                "exact retry preserves claim epoch token and lease");
                        System.out.println("REGISTRATION_RETAINED_OK|" + command.sha256() + "|" + digest);
                    }
                }
                for (String table : List.of("repository_publication_modes", "repository_operation_owners", "repository_operations",
                        "repository_publication_assessment_starts", "repository_operation_success", "repository_operation_rejection",
                        "document_part_attempts", "document_operation_selection_attempts")) {
                    long count = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                            .setParameter("id", key.operationId()).getSingleResult()).longValue());
                    require(count == 0, "inspection must not advance " + table);
                }
                require(budget.reservedBytes() == 0, "all inspection reservations released");
            }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
