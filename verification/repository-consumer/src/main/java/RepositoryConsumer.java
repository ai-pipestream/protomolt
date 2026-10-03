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
    /** Compile the archive API using published metadata without any storage implementation. */
    static ai.protomolt.proto.repo.archive.v1.GetEntryResponse readArchive(
            ai.protomolt.proto.repo.spi.ArchiveRepository repository,
            ai.protomolt.proto.repo.spi.RepositoryCaller caller,
            ai.protomolt.proto.repo.archive.v1.GetEntryRequest request) {
        return repository.getEntry(caller, request);
    }

    public static void main(String[] args) throws Exception {
        var caller = new ai.protomolt.proto.repo.spi.RepositoryCaller("consumer", false);
        if (caller.processAuthority()) throw new AssertionError("Unexpected process authority");
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
