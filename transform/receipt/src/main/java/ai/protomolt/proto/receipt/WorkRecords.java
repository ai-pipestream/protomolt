package ai.protomolt.proto.receipt;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The canonical-byte discipline: a manifest's bytes are the deterministic
 * serialization of a map-free message, and the record's identity is the
 * SHA-256 of exactly those bytes. Verification never re-canonicalizes —
 * it hashes and checks bytes as given — so canonicality is enforced as a
 * named check (reserialization equality), not assumed.
 */
public final class WorkRecords {

    /** The manifest format version this build writes and verifies. */
    public static final int MANIFEST_VERSION = 1;

    /** The v1 subject kind: a workflow run's evidence. */
    public static final String SUBJECT_KIND_WORKFLOW_RUN = "workflow-run";

    /** The size bound on a signed record, matching run evidence at rest. */
    public static final int MAX_RECORD_BYTES = 4 * 1024 * 1024;

    private WorkRecords() {
    }

    /** Serializes a manifest into its canonical bytes. */
    public static byte[] canonicalBytes(WorkRecord manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest must not be null");
        }
        return deterministicBytes(manifest);
    }

    /** Deterministic protobuf bytes, shared with protocol identity checks. */
    public static byte[] deterministicBytes(Message message) {
        return ai.protomolt.proto.descriptors.MessageFingerprints.deterministicBytes(message);
    }
    /** SHA-256 of the message's deterministic serialization. */
    public static String fingerprint(Message message) {
        return ai.protomolt.proto.descriptors.MessageFingerprints.fingerprint(message);
    }
    /** SHA-256 of the given bytes as lowercase hex. */
    public static String sha256Hex(byte[] bytes) {
        return ai.protomolt.proto.descriptors.MessageFingerprints.sha256Hex(bytes);
    }

    /**
     * Paths of every unknown field in the message and its nested messages.
     * The strict profile refuses a record carrying any: unknown fields
     * survive reserialization, so reserialization equality alone cannot
     * catch them.
     */
    static List<String> unknownFieldPaths(Message message) {
        List<String> paths = new ArrayList<>();
        collectUnknown(message, "", paths);
        return paths;
    }

    private static void collectUnknown(Message message, String prefix, List<String> paths) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            paths.add(prefix.isEmpty() ? "(root)" : prefix);
        }
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            if (field.isRepeated()) {
                List<?> values = (List<?>) entry.getValue();
                for (int i = 0; i < values.size(); i++) {
                    collectUnknown((Message) values.get(i), path + "[" + i + "]", paths);
                }
            } else {
                collectUnknown((Message) entry.getValue(), path, paths);
            }
        }
    }
}
