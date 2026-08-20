package ai.pipestream.proto.search.index.protobuf;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.indexing.testdata.AnyEnvelope;
import ai.pipestream.proto.indexing.testdata.GatedEnvelope;
import ai.pipestream.proto.indexing.testdata.ValidatedPayload;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

/**
 * Adversarial edge cases for the NDJSON facade's {@code google.protobuf.Any} gate:
 * output atomicity on gate failure, ordering against the top-level validator, root-level
 * and doubly-nested payloads, hostile type URLs, and the paths
 * {@link DeclaredRulesAnyPayloadValidator} prefixes onto payload violations.
 */
class ProtobufIndexerAnyGateEdgeCaseTest {

    @Test
    void writeNdjsonLineEmitsNothingWhenTheGateRejectsThePayload() {
        ProtobufIndexer indexer = indexer();
        StringBuilder out = new StringBuilder();

        assertThatThrownBy(() -> indexer.writeNdjsonLine(out, envelope(payload("x", -1))))
                .isInstanceOf(ValidationResult.ValidationException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void writeNdjsonLineEmitsNothingWhenTheTypeUrlIsUnknown() {
        ProtobufIndexer indexer = indexer();
        StringBuilder out = new StringBuilder();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> indexer.writeNdjsonLine(out, envelope))
                .isInstanceOf(MappingException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void aPriorLineSurvivesAFailedGateOnTheNextMessage() throws Exception {
        ProtobufIndexer indexer = indexer();
        StringBuilder out = new StringBuilder();

        indexer.writeNdjsonLine(out, envelope(payload("Opinion", 12)));
        assertThatThrownBy(() -> indexer.writeNdjsonLine(out, envelope(payload("x", -1))))
                .isInstanceOf(ValidationResult.ValidationException.class);

        assertThat(out.toString()).endsWith("\n").hasLineCount(1);
    }

    @Test
    void topLevelValidationFailsBeforeTheAnyPayloadGateRuns() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(ValidatedPayload.getDescriptor());
        ProtobufIndexer indexer = ProtobufIndexer.defaults(
                ProtoValidator.forMessageType(GatedEnvelope.getDescriptor()), registry);
        // Both the envelope (blank doc_id) and the packed payload violate their rules.
        GatedEnvelope envelope = GatedEnvelope.newBuilder()
                .setPayload(Any.pack(payload("x", -1)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .extracting(e -> ((ValidationResult.ValidationException) e).result().violations())
                .asInstanceOf(list(ValidationResult.Violation.class))
                .extracting(ValidationResult.Violation::path)
                .containsOnly("doc_id");
    }

    @Test
    void aRootMessageThatIsItselfAnAnyReportsUnprefixedViolationPaths() {
        ProtobufIndexer indexer = indexer();

        assertThatThrownBy(() -> indexer.toNdjsonLine(Any.pack(payload("x", -1))))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .extracting(e -> ((ValidationResult.ValidationException) e).result().violations())
                .asInstanceOf(list(ValidationResult.Violation.class))
                .extracting(ValidationResult.Violation::path)
                .contains("title", "page_count");
    }

    @Test
    void aPayloadNestedInsideAPackedEnvelopeFailsWithItsFullPath() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(ValidatedPayload.getDescriptor());
        registry.register(AnyEnvelope.getDescriptor());
        ProtobufIndexer indexer = ProtobufIndexer.defaults(null, registry);
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("outer")
                .setPayload(Any.pack(AnyEnvelope.newBuilder()
                        .setDocId("inner")
                        .setPayload(Any.pack(payload("x", -1)))
                        .build()))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.payload.title");
    }

    @Test
    void anEmptyMapKeySurvivesIntoTheViolationPath() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .putExtras("", Any.pack(payload("x", -1)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("extras[].title");
    }

    @Test
    void aWellKnownTimestampPayloadPassesTheGateAndRenders() throws Exception {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(Timestamp.newBuilder().setSeconds(42).build()))
                .build();

        assertThat(indexer.toNdjsonLine(envelope))
                .contains("type.googleapis.com/google.protobuf.Timestamp")
                .contains("1970-01-01T00:00:42Z");
    }

    @Test
    void aNonStandardTypeUrlPrefixIsBothGatedAndRendered() throws Exception {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.newBuilder()
                        .setTypeUrl("types.example.com/"
                                + ValidatedPayload.getDescriptor().getFullName())
                        .setValue(payload("Opinion", 12).toByteString())
                        .build())
                .build();

        assertThat(indexer.toNdjsonLine(envelope))
                .contains("types.example.com/")
                .contains("\"title\":\"Opinion\"");
    }

    @Test
    void anInvalidPayloadUnderANonStandardTypeUrlPrefixIsStillGated() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.newBuilder()
                        .setTypeUrl("types.example.com/"
                                + ValidatedPayload.getDescriptor().getFullName())
                        .setValue(payload("x", -1).toByteString())
                        .build())
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.title");
    }

    @Test
    void aTypeUrlWithNoSlashIsRejectedByTheGateRatherThanTheEncoder() {
        // JsonFormat's Any printer rejects any type URL without '/', so the gate must fail
        // such a document by path first instead of letting it die in the encoder.
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.newBuilder()
                        .setTypeUrl(ValidatedPayload.getDescriptor().getFullName())
                        .setValue(payload("Opinion", 12).toByteString())
                        .build())
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void aGateFailureLeavesNoDanglingBulkActionLine() {
        ProtobufIndexer indexer = indexer();
        StringBuilder out = new StringBuilder();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .addAttachments(Any.pack(payload("Fine", 1)))
                .addAttachments(Any.pack(payload("x", -1)))
                .build();

        assertThatThrownBy(() -> indexer.writeBulkIndex(out, "docs", "doc-1", envelope))
                .isInstanceOf(ValidationResult.ValidationException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void anEmptyAnyOnAGatedFieldRendersAsAnEmptyObject() throws Exception {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.getDefaultInstance())
                .build();

        assertThat(indexer.toNdjsonLine(envelope)).contains("\"payload\":{}");
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
