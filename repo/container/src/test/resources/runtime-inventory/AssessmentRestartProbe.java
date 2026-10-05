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
        }
        if (budget.reservedBytes() != 0) throw new AssertionError("Restart leaked payload reservations");
        System.out.println("RESTARTED_ASSESSMENT_ACK_OK");
    }

    private static String required(Properties fields, String name) {
        var value = fields.getProperty(name);
        if (value == null || value.isBlank()) throw new AssertionError("Missing restart field: " + name);
        return value;
    }
}
