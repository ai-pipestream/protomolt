package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Internal restart input; command reconstruction grants neither ownership nor document access. */
final class DocumentOperationCommands {
    private final Tx tx;
    DocumentOperationCommands(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    Optional<DocumentPublicationCommand> load(RepositoryCaller caller, String account, UUID operation,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(account, caller.principalName(), operation);
        DocumentAdmissionAuthorization.requireCaller(caller, key, account);
        var rows = tx.readOnly(em -> em.createNativeQuery("""
                SELECT command_codec,command_version,command,command_sha256 FROM repository_operations
                WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                """).setParameter("account", account).setParameter("principal", key.principal())
                .setParameter("operation", operation).getResultList());
        control.check();
        if (rows.isEmpty()) return Optional.empty();
        var row = (Object[]) rows.getFirst();
        if (!DocumentPublicationCommand.CODEC.equals(row[0])
                || ((Number) row[1]).intValue() != DocumentPublicationCommand.ENCODING_VERSION)
            throw new RepositoryException(RepositoryException.Code.UNSUPPORTED, "Stored publication command encoding is unsupported");
        var bytes = (byte[]) row[2];
        var digest = (byte[]) row[3];
        if (bytes.length == 0 || bytes.length > DocumentPublicationCommand.MAX_COMMAND_BYTES || digest.length != 32)
            throw corrupt(null);
        try {
            var semantic = DocumentPublicationIntent.parseFrom(bytes);
            if (!semantic.getOperationId().isEmpty()) throw corrupt(null);
            var command = new DocumentPublicationCommand(semantic.toBuilder().setOperationId(operation.toString()).build());
            if (!command.intent().getAccountId().equals(account)
                    || !command.canonical().equals(ByteString.copyFrom(bytes))
                    || !command.sha256().equals(HexFormat.of().formatHex(digest))) throw corrupt(null);
            control.check();
            return Optional.of(command);
        } catch (InvalidProtocolBufferException | IllegalArgumentException invalid) {
            throw corrupt(invalid);
        }
    }

    private static RepositoryException corrupt(Throwable cause) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Stored publication command is invalid", cause);
    }
}
