package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentRevisionMetadataContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    @Test void explicitStateAndNullableFieldsRetainTheirMeaning() throws Exception {
        var known = snapshot().build();
        check(HistoricalDocumentMetadata.newBuilder().setKnown(known).build(), true);
        check(HistoricalDocumentMetadata.getDefaultInstance(), false);
        check(HistoricalDocumentMetadata.newBuilder().setUnknown(HistoricalMetadataUnknown.HISTORICAL_METADATA_UNKNOWN_LEGACY_UNRECORDED).build(), true);
        check(HistoricalDocumentMetadata.newBuilder().setUnknownValue(0).build(), false);
        check(HistoricalDocumentMetadata.newBuilder().setUnknownValue(99).build(), false);
        check(known.toBuilder().setConnectorId("").setFilename("").setContentType("").setCrawlId("").build(), true);
        assertThat(known.hasConnectorId()).isFalse();
        assertThat(known.toBuilder().setConnectorId("").build().hasConnectorId()).isTrue();
        check(known.toBuilder().setCreatedAtEpochMicros(Long.MIN_VALUE).setUpdatedAtEpochMicros(Long.MAX_VALUE).build(), true);
    }
    @Test void validatesVersionBoundsAndPolicyAlternatives() throws Exception {
        check(snapshot().setEncodingVersion(2).build(), false);
        check(snapshot().clearRowKind().build(), false);
        check(snapshot().clearSecurity().build(), false);
        check(snapshot().setFilename("x".repeat(501)).build(), true);
        check(snapshot().setDeleteSourceBlobsOnSettle(true).build(), false);
        check(snapshot().setSourceBlobDeleteReason("cleanup").build(), false);
        check(snapshot().setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_INTAKE)
                .setDeleteSourceBlobsOnSettle(true).setSourceBlobDeleteReason("cleanup").build(), true);
        check(snapshot().setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_INTAKE).setClusterId("cluster").build(), false);
    }
    private static DocumentRevisionMetadata.Builder snapshot() {
        return DocumentRevisionMetadata.newBuilder().setEncodingVersion(1)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE).setAccountId("account")
                .setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance());
    }
    private static void check(Message message, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(message).valid()).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray())).valid()).isEqualTo(valid);
    }
}
