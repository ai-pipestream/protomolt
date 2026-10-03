import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import java.util.Arrays;
import java.util.List;

/** Runs from published artifacts, with no project dependency substitution. */
public final class RepositoryConsumer {
    public static void main(String[] args) throws Exception {
        Document original = Document.newBuilder().setDocId("published-consumer")
                .setStructuredData(Any.pack(StringValue.of("retained payload"))).build();
        List<PartObject> parts = DocumentPartCodec.split(original, PartLayouts.document());
        Document restored = DocumentPartCodec.assemble(parts.stream().map(PartObject::bytes).toList(),
                Document.getDefaultInstance());
        if (!Arrays.equals(original.toByteArray(), restored.toByteArray())) {
            throw new AssertionError("Typed assembly changed persisted bytes");
        }
        if (!restored.getStructuredData().unpack(StringValue.class).getValue().equals("retained payload")) {
            throw new AssertionError("Any payload changed");
        }
        if (BlobStore.MAX_CONDITIONAL_BYTES != 9 * 1024 * 1024) {
            throw new AssertionError("Conditional byte limit changed");
        }
        try {
            new BlobStore.WriteCondition(true, "\"etag\"");
            throw new AssertionError("Conflicting alternatives accepted");
        } catch (IllegalArgumentException expected) {
            // Both alternatives must remain rejected through the published API.
        }
    }
}
