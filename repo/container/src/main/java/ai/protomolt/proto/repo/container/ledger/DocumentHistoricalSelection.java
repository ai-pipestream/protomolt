package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.PublicationHistoricalReuse;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Message;
import java.util.Objects;

/** Exact selector comparison against an authorized, pinned native revision. No current-source fallback. */
final class DocumentHistoricalSelection {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private DocumentHistoricalSelection() {}

    static DocumentHistoricalReadPlan.Entry select(DocumentHistoricalReadPlan plan, PublicationHistoricalReuse selection) {
        Objects.requireNonNull(selection);
        if (selection.getSerializedSize() > DocumentPublicationCommand.MAX_COMMAND_BYTES)
            throw new IllegalArgumentException("Historical selector exceeds command byte bound");
        rejectUnknown(selection);
        if (!VALIDATOR.validate(selection).valid())
            throw new IllegalArgumentException("Invalid historical selector");
        if (!plan.address().equals(selection.getSource()) || !plan.revision().toString().equals(selection.getRevisionId()))
            throw mismatch();
        var entry = entryAt(plan, selection.getRevisionOrdinal());
        var slot = plan.manifest().getParts(selection.getRevisionOrdinal());
        if (slot.getState() != ai.protomolt.proto.repo.v1.PartState.PART_STATE_PRESENT
                || slot.getPart() != selection.getSourceSlot().getPart()
                || !slot.getSubKey().equals(selection.getSourceSlot().getSubKey())) throw mismatch();
        var expected = selection.getObject();
        var part = entry.part().part();
        var binding = entry.part().binding();
        if (part.part() != selection.getSourceSlot().getPart() || !part.subKey().equals(selection.getSourceSlot().getSubKey())
                || !entry.objectId().toString().equals(expected.getObjectId())
                || !binding.generation().equals(expected.getBackendGeneration())
                || !binding.profile().storageRealm().equals(expected.getStorageRealm())
                || !binding.namespace().equals(expected.getNamespace())
                || !part.key().equals(expected.getObjectKey())
                || !Objects.equals(part.providerVersion(), expected.hasProviderVersion() ? expected.getProviderVersion() : null)
                || part.size() != expected.getSizeBytes() || !part.sha256().equals(expected.getSha256())
                || !part.contentType().equals(expected.getContentType())) throw mismatch();
        return entry;
    }

    private static DocumentHistoricalReadPlan.Entry entryAt(DocumentHistoricalReadPlan plan, int ordinal) {
        // SQL capture orders full ordinals. Binary search avoids quadratic work
        // when a restore selects every part; compact indexes cannot represent gaps.
        int low = 0, high = plan.entries().size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            var entry = plan.entries().get(middle);
            if (entry.revisionOrdinal() == ordinal) return entry;
            if (entry.revisionOrdinal() < ordinal) low = middle + 1;
            else high = middle - 1;
        }
        throw mismatch();
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("Unknown historical selector fields are unsupported");
        // The selector and its nested messages contain no repeated fields.
        for (var value : message.getAllFields().values()) {
            if (value instanceof Message nested) rejectUnknown(nested);
        }
    }

    private static RepositoryException mismatch() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Historical selector differs from retained revision binding");
    }
}
