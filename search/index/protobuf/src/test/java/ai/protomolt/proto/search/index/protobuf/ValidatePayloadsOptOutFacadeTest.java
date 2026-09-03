package ai.protomolt.proto.search.index.protobuf;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.ndjson.NdjsonOptions;
import ai.protomolt.proto.search.index.ndjson.ProtoNdjsonWriter;
import ai.protomolt.proto.search.index.spi.AnyIndexing;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.indexing.testdata.AnyEnvelope;
import ai.protomolt.proto.indexing.testdata.NestedAnyPayload;
import ai.protomolt.proto.indexing.testdata.OptedOutEnvelope;
import ai.protomolt.proto.indexing.testdata.ValidatedPayload;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code validate_payloads} opt-out end to end through the facade, on schemas whose
 * annotation is compiled by protoc rather than hand-built: repeated and map Any fields,
 * opt-outs carried on an unpacked payload's own fields, catalog precedence over the
 * schema, and what the opt-out deliberately does not turn off.
 */
class ValidatePayloadsOptOutFacadeTest {

    @Test
    void everyElementOfAnOptedOutRepeatedAnyRendersWithoutTheRules() throws Exception {
        ProtobufIndexer indexer = indexer();
        OptedOutEnvelope envelope = OptedOutEnvelope.newBuilder()
                .setDocId("doc-1")
                .addAttachments(Any.pack(payload("x", -1)))
                .addAttachments(Any.pack(payload("y", -2)))
                .build();

        String line = indexer.toNdjsonLine(envelope);

        assertThat(line).contains("\"title\":\"x\"").contains("\"title\":\"y\"");
    }

    @Test
    void anOptedOutMapValueRendersWithoutTheRules() throws Exception {
        ProtobufIndexer indexer = indexer();
        OptedOutEnvelope envelope = OptedOutEnvelope.newBuilder()
                .setDocId("doc-1")
                .putExtras("cover", Any.pack(payload("x", -1)))
                .build();

        String line = indexer.toNdjsonLine(envelope);

        assertThat(line).contains("\"cover\"").contains("\"title\":\"x\"");
    }

    @Test
    void aRepeatedOrMapOptOutDoesNotReachTheSingularSibling() {
        ProtobufIndexer indexer = indexer();
        OptedOutEnvelope envelope = OptedOutEnvelope.newBuilder()
                .setDocId("doc-1")
                .addAttachments(Any.pack(payload("x", -1)))
                .putExtras("cover", Any.pack(payload("y", -2)))
                .setPayload(Any.pack(payload("z", -3)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.title")
                .hasMessageNotContaining("attachments")
                .hasMessageNotContaining("extras");
    }

    @Test
    void theGateResolvesTheOptOutOnTheUnpackedPayloadsOwnField() throws Exception {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(NestedAnyPayload.newBuilder()
                        .setTitle("outer")
                        .setUncheckedInner(Any.pack(payload("x", -1)))
                        .build()))
                .build();

        String line = indexer.toNdjsonLine(envelope);

        assertThat(line).contains("\"title\":\"x\"");
    }

    @Test
    void anUnannotatedAnyInsideAPayloadIsStillGatedUnderItsNestedPath() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(NestedAnyPayload.newBuilder()
                        .setTitle("outer")
                        .setUncheckedInner(Any.pack(payload("x", -1)))
                        .setCheckedInner(Any.pack(payload("y", -2)))
                        .build()))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.checked_inner.title")
                .hasMessageNotContaining("payload.unchecked_inner");
    }

    @Test
    void aCatalogHintWithoutTheOptOutOverridesTheSchemaOptOut() {
        // Whole-hint precedence: catalog → proto options → inference resolves ONE source per
        // field, so a catalog tag that says nothing about validate_payloads still replaces the
        // schema's opt-out with the default.
        DescriptorRegistry registry = registry();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(AnyEnvelope.getDescriptor().getFullName(), "unchecked",
                        ResolvedFieldHint.of(IndexFieldKind.ANY));
        ProtobufIndexer indexer = new ProtobufIndexer(
                IndexMappingFactory.defaults(catalog),
                new ProtoNdjsonWriter(NdjsonOptions.defaults(), registry));
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setUnchecked(Any.pack(payload("x", -1)))
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("unchecked.title");
    }

    @Test
    void aCatalogHintCarryingTheOptOutSuppressesTheGateOnAnUnannotatedField() throws Exception {
        DescriptorRegistry registry = registry();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(AnyEnvelope.getDescriptor().getFullName(), "payload",
                        ResolvedFieldHint.builder(IndexFieldKind.ANY).validatePayloads(false).build());
        ProtobufIndexer indexer = new ProtobufIndexer(
                IndexMappingFactory.defaults(catalog),
                new ProtoNdjsonWriter(NdjsonOptions.defaults(), registry));
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(payload("x", -1)))
                .build();

        String line = indexer.toNdjsonLine(envelope);

        assertThat(line).contains("\"title\":\"x\"");
    }

    @Test
    void anUnknownTypeUrlOnTheOptedOutFieldStillFails() {
        ProtobufIndexer indexer = indexer();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setUnchecked(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> indexer.toNdjsonLine(envelope))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("unchecked")
                .hasMessageContaining("register the packed type");
    }

    @Test
    void theMappingKeepsAnAnyEntryForEveryOptedOutField() {
        ProtobufIndexer indexer = indexer();

        IndexMapping mapping = indexer.mapping(AnyEnvelope.getDescriptor());

        assertThat(mapping.find("unchecked")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.ANY);
        assertThat(mapping.find("unchecked")).get()
                .extracting(field -> field.hint().validatePayloads())
                .isEqualTo(false);
        assertThat(mapping.find("payload")).get()
                .extracting(field -> field.hint().validatePayloads())
                .isEqualTo(true);
        assertThat(indexer.mapping(OptedOutEnvelope.getDescriptor()).find("attachments")).get()
                .extracting(field -> field.hint().validatePayloads())
                .isEqualTo(false);
    }

    @Test
    void theExpansionPathSeesTheOptOutOnAPayloadsOwnField() throws Exception {
        IndexMappingFactory factory = IndexMappingFactory.defaults(new CatalogIndexingHintSource());
        AnyIndexing anyIndexing = new AnyIndexing(registry(), factory);
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(NestedAnyPayload.newBuilder()
                        .setTitle("outer")
                        .setUncheckedInner(Any.pack(payload("x", -1)))
                        .build()))
                .build();

        IndexMapping expanded =
                anyIndexing.expand(envelope, factory.create(AnyEnvelope.getDescriptor()));

        assertThat(expanded.find("payload.unchecked_inner.title")).isPresent();
    }

    private static ProtobufIndexer indexer() {
        return ProtobufIndexer.defaults(null, registry());
    }

    private static DescriptorRegistry registry() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(ValidatedPayload.getDescriptor());
        registry.register(NestedAnyPayload.getDescriptor());
        return registry;
    }

    private static ValidatedPayload payload(String title, int pageCount) {
        return ValidatedPayload.newBuilder()
                .setTitle(title)
                .setPageCount(pageCount)
                .build();
    }
}
