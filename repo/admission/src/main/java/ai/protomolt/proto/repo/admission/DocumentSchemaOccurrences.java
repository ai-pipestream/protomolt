package ai.protomolt.proto.repo.admission;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;

/** Internal decoded identities. Not a wire encoding, authorization or retention proof. */
final class DocumentSchemaOccurrences {
    record Limits(int maxOccurrences, long maxPathSteps, long maxTextBytes) {
        static final Limits DEFAULT = new Limits(4096, 65536, 1048576);
        Limits {
            if (maxOccurrences < 1 || maxPathSteps < 1 || maxTextBytes < 1)
                throw new IllegalArgumentException("positive occurrence evidence limits required");
        }
    }

    sealed interface Step permits Field, Index, MapKey, Boundary {}
    record Field(int number) implements Step {}
    record Index(int index) implements Step {}
    // The scalar type is an in-memory enum, never its ordinal on a wire.
    record MapKey(FieldDescriptor.Type type, Object value) implements Step {
        MapKey {
            boolean valid = switch (type) {
                case STRING -> value instanceof String;
                case BOOL -> value instanceof Boolean;
                case INT32, SINT32, SFIXED32, UINT32, FIXED32 -> value instanceof Integer;
                case INT64, SINT64, SFIXED64, UINT64, FIXED64 -> value instanceof Long;
                default -> false;
            };
            if (!valid) throw new IllegalArgumentException("invalid protobuf map key");
        }
    }
    record Boundary(String typeUrl, String valueSha256, String artifactSha256) implements Step {}
    record Occurrence(List<Step> path) {
        Occurrence { path = List.copyOf(path); }
    }

    private final Limits limits;
    private final ArrayList<Step> path = new ArrayList<>();
    private final ArrayList<Long> textSizes = new ArrayList<>();
    private final ArrayList<Occurrence> occurrences = new ArrayList<>();
    private final HashSet<Occurrence> distinct = new HashSet<>();
    private long pathTextBytes;
    private long retainedSteps;
    private long retainedTextBytes;

    DocumentSchemaOccurrences(Limits limits) { this.limits = java.util.Objects.requireNonNull(limits); }

    void push(Step step) {
        long text = step instanceof MapKey key && key.value() instanceof String value ? utf8(value)
                : step instanceof Boundary boundary ? utf8(boundary.typeUrl()) : 0;
        if (text > limits.maxTextBytes() - pathTextBytes)
            throw new IllegalArgumentException("occurrence path text limit exceeded");
        if (path.size() >= limits.maxPathSteps())
            throw new IllegalArgumentException("occurrence path step limit exceeded");
        path.add(step);
        textSizes.add(text);
        pathTextBytes += text;
    }

    void pop() {
        path.remove(path.size() - 1);
        pathTextBytes -= textSizes.remove(textSizes.size() - 1);
    }

    void boundary(String url, ByteString bytes, DocumentSchemaBinding schema, Runnable control) {
        long text = utf8(url);
        if (occurrences.size() >= limits.maxOccurrences())
            throw new IllegalArgumentException("occurrence count limit exceeded");
        if (path.size() + 1L > limits.maxPathSteps() - retainedSteps)
            throw new IllegalArgumentException("aggregate occurrence path step limit exceeded");
        if (pathTextBytes > limits.maxTextBytes() - retainedTextBytes
                || text > limits.maxTextBytes() - retainedTextBytes - pathTextBytes)
            throw new IllegalArgumentException("aggregate occurrence text limit exceeded");
        push(new Boundary(url, sha256(bytes, control), schema.artifactSha256()));
        var occurrence = new Occurrence(path);
        if (!distinct.add(occurrence)) throw new IllegalArgumentException("duplicate schema occurrence path");
        occurrences.add(occurrence);
        retainedSteps += path.size();
        retainedTextBytes += pathTextBytes;
    }

    /** Called only after the enclosing candidate check succeeds. Order is not identity. */
    List<Occurrence> result() { return List.copyOf(occurrences); }

    private static long utf8(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }

    private static String sha256(ByteString bytes, Runnable control) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) {
                control.run();
                digest.update(buffer);
            }
            control.run();
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
