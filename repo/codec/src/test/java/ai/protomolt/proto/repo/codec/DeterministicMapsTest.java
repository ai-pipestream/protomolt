package ai.protomolt.proto.repo.codec;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

/** Deterministic fragments sort map entries for generated and dynamic messages alike. */
class DeterministicMapsTest {
    @Test void dynamicAndGeneratedFragmentsAgreeWhateverTheEntryOrder() throws Exception {
        var first = document("alpha", "beta", "gamma");
        var second = document("gamma", "alpha", "beta");
        assertThat(first.toByteArray()).isNotEqualTo(second.toByteArray());
        var generated = DocumentPartCodec.deterministic(part(first), Document.getDefaultInstance());
        var prototype = DynamicMessage.getDefaultInstance(Document.getDescriptor());
        for (var document : new Document[] {first, second}) {
            var dynamic = DynamicMessage.parseFrom(Document.getDescriptor(), document.toByteString());
            var fragment = DocumentPartCodec.deterministic(part(dynamic), prototype);
            assertThat(fragment.bytes()).isEqualTo(generated.bytes());
            assertThat(DocumentPartCodec.deterministic(part(document), Document.getDefaultInstance()).bytes())
                    .isEqualTo(generated.bytes());
        }
    }

    private static PartObject part(com.google.protobuf.Message message) {
        var bytes = message.toByteArray();
        return new PartObject(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE, "", bytes,
                DocumentPartCodec.sha256Hex(bytes));
    }

    private static Document document(String... keys) {
        var struct = Struct.newBuilder();
        for (var key : keys) struct.putFields(key, Value.newBuilder().setStringValue("value of " + key).build());
        return Document.newBuilder().setDocId("d").setSearchMetadata(SearchMetadata.newBuilder().setCustomFields(struct)).build();
    }
}
