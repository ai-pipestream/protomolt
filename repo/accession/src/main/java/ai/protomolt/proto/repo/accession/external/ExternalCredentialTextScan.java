package ai.protomolt.proto.repo.accession.external;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Finds text that reads as a credential in a message bound for the ledger: an operation's intent
 * before it is first saved, and provider evidence before it is accepted. The ledger is readable by
 * anyone who may read the source, and a valid signature proves only who wrote a text, not that it
 * is safe to keep. Every string field is checked, at any depth, through the message descriptors;
 * bytes fields are not text and are not read.
 */
final class ExternalCredentialTextScan {

    private ExternalCredentialTextScan() {
    }

    /**
     * Returns the path of the first string field whose value carries credential material, or
     * empty when none does. The path names the field, never the value.
     *
     * @param message the message to scan
     * @param rootName the name the returned path starts with
     */
    static Optional<String> firstCredentialField(Message message, String rootName) {
        for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            String path = rootName + "." + field.getKey().getName();
            Optional<String> found = field.getKey().isRepeated()
                    ? scanRepeated(field.getKey(), (List<?>) field.getValue(), path)
                    : scanValue(field.getKey(), field.getValue(), path);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private static Optional<String> scanRepeated(FieldDescriptor descriptor, List<?> values, String path) {
        for (int i = 0; i < values.size(); i++) {
            Optional<String> found = scanValue(descriptor, values.get(i), path + "[" + i + "]");
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private static Optional<String> scanValue(FieldDescriptor descriptor, Object value, String path) {
        return switch (descriptor.getJavaType()) {
            case STRING -> ExternalPublicationCoordinates.containsCredentialMaterial((String) value)
                    ? Optional.of(path)
                    : Optional.empty();
            case MESSAGE -> firstCredentialField((Message) value, path);
            default -> Optional.empty();
        };
    }
}
