package ai.protomolt.proto.search.index.protobuf;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.AnyIndexing;
import ai.protomolt.proto.search.index.spi.AnyPayloadValidator;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.indexing.testdata.AnyEnvelope;
import ai.protomolt.proto.indexing.testdata.PageSpan;
import ai.protomolt.proto.indexing.testdata.ValidatedPayload;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The declared-rules gate on unpacked {@code google.protobuf.Any} payloads: this module's
 * {@link DeclaredRulesAnyPayloadValidator} is discovered via {@code ServiceLoader}, so any
 * {@link AnyIndexing} built without an explicit validator list enforces the payload's
 * declared rules during expansion.
 */
class AnyPayloadValidationTest {

    @Test
    void serviceLoaderDiscoversTheDeclaredRulesGate() {
        assertThat(ServiceLoader.load(AnyPayloadValidator.class))
                .hasAtLeastOneElementOfType(DeclaredRulesAnyPayloadValidator.class);
    }

    @Test
    void validPayloadExpandsIntoInnerFields() throws Exception {
        AnyIndexing anyIndexing = anyIndexing();
        AnyEnvelope envelope = envelope(ValidatedPayload.newBuilder()
                .setTitle("Opinion")
                .setPageCount(12)
                .build());

        IndexMapping expanded = anyIndexing.expand(envelope, mapping());

        assertThat(expanded.find("payload.title")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(expanded.find("payload.title")).get()
                .extracting(IndexMapping.IndexedField::fieldName)
                .isEqualTo("payload_title");
    }

    @Test
    void invalidPayloadFailsTheDocumentWithPrefixedViolationPaths() {
        AnyIndexing anyIndexing = anyIndexing();
        AnyEnvelope envelope = envelope(ValidatedPayload.newBuilder()
                .setTitle("x")
                .setPageCount(-1)
                .build());

        assertThatThrownBy(() -> anyIndexing.expand(envelope, mapping()))
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("payload.title")
                .hasMessageContaining("payload.page_count");
    }

    @Test
    void skipWhenEscapeHatchSuspendsTheDeclaredRules() {
        AnyIndexing anyIndexing = anyIndexing();
        AnyEnvelope envelope = envelope(ValidatedPayload.newBuilder()
                .setTitle("x")
                .setDraft(true)
                .build());

        assertThatCode(() -> anyIndexing.expand(envelope, mapping())).doesNotThrowAnyException();
    }

    @Test
    void schemaOptOutSuspendsTheGateOnTheExpansionPathToo() throws Exception {
        AnyIndexing anyIndexing = anyIndexing();
        AnyEnvelope envelope = AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setUnchecked(Any.pack(ValidatedPayload.newBuilder()
                        .setTitle("x")
                        .setPageCount(-1)
                        .build()))
                .build();

        // An invalid payload behind validate_payloads: false expands instead of failing.
        IndexMapping expanded = anyIndexing.expand(envelope, mapping());

        assertThat(expanded.find("unchecked.title")).isPresent();
    }

    @Test
    void payloadTypesWithoutRulesPassUntouched() {
        // The gate itself: a payload type declaring no rules validates clean.
        assertThatCode(() -> new DeclaredRulesAnyPayloadValidator()
                .validate(PageSpan.newBuilder().setGte(1).setLte(2).build(), "payload"))
                .doesNotThrowAnyException();
    }

    @Test
    void violationsCarryThePrefixedPathStructurally() {
        DeclaredRulesAnyPayloadValidator gate = new DeclaredRulesAnyPayloadValidator();
        ValidatedPayload invalid = ValidatedPayload.newBuilder().setTitle("x").build();

        assertThatThrownBy(() -> gate.validate(invalid, "outer.payload"))
                .isInstanceOfSatisfying(ValidationResult.ValidationException.class, e ->
                        assertThat(e.result().violations())
                                .extracting(ValidationResult.Violation::path)
                                .allMatch(path -> path.startsWith("outer.payload")));
    }

    private static AnyIndexing anyIndexing() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(ValidatedPayload.getDescriptor());
        return new AnyIndexing(registry, IndexMappingFactory.defaults(new CatalogIndexingHintSource()));
    }

    private static IndexMapping mapping() {
        return IndexMappingFactory.defaults(new CatalogIndexingHintSource())
                .create(AnyEnvelope.getDescriptor());
    }

    private static AnyEnvelope envelope(ValidatedPayload payload) {
        return AnyEnvelope.newBuilder()
                .setDocId("doc-1")
                .setPayload(Any.pack(payload))
                .build();
    }
}
