package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalPartDigest;
import ai.protomolt.proto.repo.external.v1.ExternalProcessingIdentity;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExternalOperationKeys}: deterministic canonical intent SHA-256,
 * order-insensitive set canonicalization, UUIDv5 derivation, and sensitivity to every
 * intent field mutation.
 */
class ExternalOperationKeysTest {

    private static final String SHA_A = "a".repeat(64);
    private static final String SHA_B = "b".repeat(64);
    private static final String SHA_C = "c".repeat(64);
    private static final String SHA_D = "d".repeat(64);
    private static final String SHA_E = "e".repeat(64);

    private static NodeAddress sourceAddress() {
        return NodeAddress.newBuilder()
                .setAccountId("tenant-alpha")
                .setGraphId("intake:tenant-alpha")
                .setGraphAddressId("ds-1")
                .setDocId("doc-100")
                .build();
    }

    private static ExternalPublicationIntent basePublishIntent() {
        ExternalPartDigest coreDigest = ExternalPartDigest.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_CORE)
                .setSubKey("")
                .setSha256(SHA_A)
                .setSizeBytes(120)
                .build();
        ExternalPartDigest parsedDigest = ExternalPartDigest.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_PARSED)
                .setSubKey("")
                .setSha256(SHA_B)
                .setSizeBytes(450)
                .build();

        return ExternalPublicationIntent.newBuilder()
                .setEncodingVersion(1)
                .setAccountId("tenant-alpha")
                .setSourceAddress(sourceAddress())
                .setSourceRevision(3L)
                .setBindingId("search-primary")
                .setBindingRevision(2L)
                .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                .setExternalObjectId("ext-obj-100")
                .setDisplayUrl("https://search.example.com/items/ext-obj-100")
                .setProcessing(ExternalProcessingIdentity.newBuilder()
                        .addSourcePartDigests(coreDigest)
                        .addSourcePartDigests(parsedDigest)
                        .addRetainedSchemaSha256S(SHA_C)
                        .addRetainedSchemaSha256S(SHA_D)
                        .setMappingId("search-projection-v2")
                        .setGenerationDigest(SHA_E)
                        .setNlpInputSha256(SHA_A)
                        .setNlpResultSha256(SHA_B)
                        .setNlpReceiptSha256(SHA_C)
                        .build())
                .build();
    }

    @Test
    void intentDigestAndDerivedOperationIdAreOrderInvariantOnPartAndSchemaSets() {
        ExternalPublicationIntent first = basePublishIntent();
        ExternalProcessingIdentity reversedProcessing = first.getProcessing().toBuilder()
                .clearSourcePartDigests()
                .addSourcePartDigests(first.getProcessing().getSourcePartDigests(1))
                .addSourcePartDigests(first.getProcessing().getSourcePartDigests(0))
                .clearRetainedSchemaSha256S()
                .addRetainedSchemaSha256S(SHA_D)
                .addRetainedSchemaSha256S(SHA_C)
                .build();
        ExternalPublicationIntent reordered = first.toBuilder()
                .setProcessing(reversedProcessing)
                .build();

        assertThat(ExternalOperationKeys.intentSha256(first))
                .isEqualTo(ExternalOperationKeys.intentSha256(reordered));
        String opId = ExternalOperationKeys.deriveOperationId(first);
        assertThat(opId).isEqualTo(ExternalOperationKeys.deriveOperationId(reordered));
        assertThat(UUID.fromString(opId).version()).isEqualTo(5);
    }

    @Test
    void mutatingAnySemanticFieldChangesIntentSha256AndOperationId() {
        ExternalPublicationIntent base = basePublishIntent();
        String baseSha = ExternalOperationKeys.intentSha256(base);
        String baseOpId = ExternalOperationKeys.deriveOperationId(base);

        ExternalPublicationIntent[] mutations = new ExternalPublicationIntent[] {
            base.toBuilder().setSourceRevision(4L).build(),
            base.toBuilder().setBindingId("search-secondary").build(),
            base.toBuilder().setBindingRevision(3L).build(),
            base.toBuilder().setExternalObjectId("ext-obj-101").build(),
            base.toBuilder().clearDisplayUrl().build(),
            base.toBuilder().setDisplayUrl("https://search.example.com/items/other").build(),
            base.toBuilder()
                    .setSourceAddress(sourceAddress().toBuilder().setDocId("doc-101").build())
                    .build(),
            base.toBuilder()
                    .setProcessing(base.getProcessing().toBuilder().setMappingId("search-projection-v3"))
                    .build(),
            base.toBuilder()
                    .setProcessing(base.getProcessing().toBuilder().setGenerationDigest(SHA_A))
                    .build(),
            base.toBuilder()
                    .setProcessing(base.getProcessing().toBuilder().setNlpReceiptSha256(SHA_E))
                    .build(),
            ExternalPublicationIntent.newBuilder()
                    .setEncodingVersion(1)
                    .setAccountId("tenant-alpha")
                    .setSourceAddress(sourceAddress())
                    .setSourceRevision(3L)
                    .setBindingId("search-primary")
                    .setBindingRevision(2L)
                    .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE)
                    .setExternalObjectId("ext-obj-100")
                    .build(),
            ExternalPublicationIntent.newBuilder()
                    .setEncodingVersion(1)
                    .setAccountId("tenant-alpha")
                    .setSourceAddress(sourceAddress())
                    .setSourceRevision(0L)
                    .setPurgeId("purge-corr-1")
                    .setBindingId("search-primary")
                    .setBindingRevision(2L)
                    .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE)
                    .setExternalObjectId("ext-obj-100")
                    .build()
        };

        for (ExternalPublicationIntent mutated : mutations) {
            assertThat(ExternalOperationKeys.intentSha256(mutated))
                    .as("Mutated intent must change SHA-256: %s", mutated)
                    .isNotEqualTo(baseSha);
            assertThat(ExternalOperationKeys.deriveOperationId(mutated))
                    .isNotEqualTo(baseOpId);
        }
    }

    @Test
    void validateIntentRejectsPublishAtRevisionZeroAndBearerDisplayUrl() {
        ExternalPublicationIntent invalidPublishAtZero = basePublishIntent().toBuilder()
                .setSourceRevision(0L)
                .setPurgeId("purge-1")
                .build();
        assertThatThrownBy(() -> ExternalOperationKeys.intentSha256(invalidPublishAtZero))
                .isInstanceOf(IllegalArgumentException.class);

        ExternalPublicationIntent bearerUrlIntent = basePublishIntent().toBuilder()
                .setDisplayUrl("https://search.example.com/items/1?token=secret")
                .build();
        assertThatThrownBy(() -> ExternalOperationKeys.intentSha256(bearerUrlIntent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid ExternalPublicationIntent");

        ExternalPublicationIntent percentEncodedTokenIntent = basePublishIntent().toBuilder()
                .setDisplayUrl("https://search.example.com/items/1?%74%6F%6B%65%6E=secret")
                .build();
        assertThatThrownBy(() -> ExternalOperationKeys.intentSha256(percentEncodedTokenIntent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Forbidden credential/token");
    }
}
