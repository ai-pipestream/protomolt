package ai.pipestream.proto.search.index.protobuf;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.indexing.testdata.AnyEnvelope;
import ai.pipestream.proto.indexing.testdata.ValidatedPayload;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The NDJSON facade's {@code google.protobuf.Any} payload gate: a
 * {@link ProtobufIndexer} whose writer carries a registry unpacks every packed payload
 * on the write path and enforces its declared rules — the same gate the engine write
 * path applies during mapping expansion, here covering repeated Anys and map values too.
 */
class ProtobufIndexerAnyGateTest {

    @Test
    void validPayloadRendersWithItsTypeUrl() throws Exception {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = envelope(payload("Opinion", 12));

        String line = indexer.toNdjsonLine(envelope);

        assertThat(line)
                .contains("\"doc_id\":\"doc-1\"")
                .contains("ValidatedPayload")
                .contains("\"title\":\"Opinion\"");
    }

    @Test
    void invalidPayloadFailsBeforeAnythingIsWritten() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = envelope(payload("x", -1));

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.title")
                .hasMessageContaining("payload.page_count");
    }

    @Test
    void invalidRepeatedPayloadFailsWithItsElementPath() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .addAttachments(Any.pack(payload("Fine", 1)))
                .addAttachments(Any.pack(payload("x", -1)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("attachments[1].title");
    }

    @Test
    void invalidMapPayloadFailsWithItsKeyedPath() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .putExtras("cover", Any.pack(payload("x", -1)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("extras[cover].title");
    }

    @Test
    void skipWhenEscapeHatchSuspendsTheDeclaredRules() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = envelope(ValidatedPayload.newBuilder()
                .setTitle("x")
                .setDraft(true)
                .build());

        assertThatCode(() -> indexer.toNdjsonLine(envelope)).doesNotThrowAnyException();
    }

    @Test
    void unknownTypeUrlFailsWithRegistryGuidance() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload")
                .hasMessageContaining("register the packed type");
    }

    @Test
    void bulkWriteRunsTheSameGate() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = envelope(payload("x", -1));

        StringBuilder out = new StringBuilder();
        assertThatThrownBy(() -> indexer.writeBulkIndex(out, "docs", "doc-1", envelope))
                .isInstanceOf(ValidationResult.ValidationException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void validatePayloadsFalseOnTheSchemaRendersWithoutRunningTheRules() throws Exception {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setUnchecked(Any.pack(payload("x", -1)))
                .build();

        String line = indexer.toNdjsonLine(envelope);

        assertThat(line).contains("\"unchecked\"").contains("\"title\":\"x\"");
    }

    @Test
    void validatePayloadsFalseDoesNotLeakToSiblingFields() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setUnchecked(Any.pack(payload("x", -1)))
                .setPayload(Any.pack(payload("y", -2)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.title");
    }

    @Test
    void registryLessFacadeRunsNoGateAndCannotRenderPackedAnys() {
        ProtobufIndexer indexer = ProtobufIndexer.defaults(null);
        AnyEnvelope envelope = envelope(payload("x", -1));

        // Without a registry the invalid payload is not gated; the writer itself
        // rejects the unresolvable type URL at render time instead.
        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to encode");
    }

    private static ProtobufIndexer indexer() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(ValidatedPayload.getDescriptor());
        return ProtobufIndexer.defaults(null, registry);
    }

    private static ValidatedPayload payload(String title, int pageCount) {
        return ValidatedPayload.newBuilder()
                .setTitle(title)
                .setPageCount(pageCount)
                .build();
    }

    private static AnyEnvelope envelope(ValidatedPayload payload) {
        return AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(payload))
                .build();
    }
}
