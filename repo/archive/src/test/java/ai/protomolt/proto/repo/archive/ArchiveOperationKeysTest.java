package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.archive.v1.ArchiveOperationKey;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The derived operation ID. The three golden vectors come from
 * {@code src/test/python/archive_operation_key_vectors.py}, which encodes the key
 * by hand with no protobuf library; the Java encoding and UUIDv5 must match it
 * byte for byte.
 */
class ArchiveOperationKeysTest {

    private static String sha256(String text) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "archive.put-entry|acct-1|reports|report-2026.pdf|0|vector one"
                + "|0a11617263686976652e7075742d656e74727912220a06616363742d3112077265706f7274731a0f7265706f72742d323032362e706466224062316362336264656561633033353332643534343935336262646135393566393831666632336432373366643731353939363061383831323464646538363537"
                + "|29e386c6-6c34-5f51-833a-9fc276633b0f",
        "archive.upload-rendition|acct-1|photos|café/ü 東京|0|vector two"
                + "|0a18617263686976652e75706c6f61642d72656e646974696f6e12210a06616363742d31120670686f746f731a0f636166c3a92fc3bc20e69db1e4baac224061623232383066326264373633643137363462316538653838323761323063666262623464633061616431636165363461653362343363373733323532386565"
                + "|9d1d100d-33f9-538c-92c5-bf45217a1fe7",
        "archive.put-entry|acct-2|contracts|s3://bucket/key:with/slashes?and=query|7|vector three"
                + "|0a11617263686976652e7075742d656e747279123b0a06616363742d321209636f6e7472616374731a2673333a2f2f6275636b65742f6b65793a776974682f736c61736865733f616e643d71756572791807224063376635333934646436326365366237333730616565396361373939356164373137383331356264616430303833373061366661643731656136623465666333"
                + "|e7dc798f-1fc6-52e0-a881-a69269ecf097",
    })
    void goldenVectorsMatchTheIndependentEncoder(String kind, String account, String archive, String entryId,
            long expectedVersion, String contentSeed, String encodedHex, String uuid) throws Exception {
        ArchiveOperationKey key = ArchiveOperationKey.newBuilder()
                .setOperationKind(kind)
                .setAddress(EntryAddress.newBuilder().setAccountId(account).setArchive(archive).setEntryId(entryId))
                .setExpectedVersion(expectedVersion)
                .setContentSha256(sha256(contentSeed))
                .build();
        assertThat(HexFormat.of().formatHex(key.toByteArray())).isEqualTo(encodedHex);
        UUID derived = ArchiveOperationKeys.derive(key);
        assertThat(derived).isEqualTo(UUID.fromString(uuid));
        assertThat(derived.version()).isEqualTo(5);
        assertThat(derived.variant()).isEqualTo(2);
    }

    private static PutEntryRequest.Builder put() {
        return PutEntryRequest.newBuilder()
                .setAddress(EntryAddress.newBuilder().setAccountId("acct").setArchive("reports").setEntryId("e1"))
                .setTitle("Title")
                .addRenditions(rendition("original", "", "pdf bytes"))
                .addRenditions(rendition("page", "0002", "page two"))
                .addRenditions(rendition("page", "0001", "page one"))
                .putMetadata("z", "last")
                .putMetadata("￿", "bmp high")
                .putMetadata("😀", "supplementary")
                .putMetadata("a", "first");
    }

    private static RenditionContent rendition(String name, String subKey, String data) {
        return RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name).setSubKey(subKey))
                .setData(ByteString.copyFromUtf8(data))
                .build();
    }

    @Test
    void theContentDigestIgnoresRenditionAndMetadataOrderAndTheRetryFields() {
        PutEntryRequest request = put().build();
        String digest = ArchiveOperationKeys.putEntryContentSha256(request);
        PutEntryRequest reordered = put()
                .clearRenditions()
                .addRenditions(rendition("page", "0001", "page one"))
                .addRenditions(rendition("original", "", "pdf bytes"))
                .addRenditions(rendition("page", "0002", "page two"))
                .clearMetadata()
                .putMetadata("a", "first")
                .putMetadata("😀", "supplementary")
                .putMetadata("￿", "bmp high")
                .putMetadata("z", "last")
                .setOperationId("caller-op")
                .setExpectedVersion(4)
                .build();
        assertThat(ArchiveOperationKeys.putEntryContentSha256(reordered)).isEqualTo(digest);
        assertThat(ArchiveOperationKeys.putEntryContentSha256(put().setTitle("Other").build())).isNotEqualTo(digest);
        assertThat(ArchiveOperationKeys.putEntryContentSha256(put().putMetadata("a", "changed").build()))
                .isNotEqualTo(digest);
        assertThat(ArchiveOperationKeys.putEntryContentSha256(put()
                .setRenditions(0, rendition("original", "", "other bytes")).build())).isNotEqualTo(digest);
    }

    @Test
    void theContentDigestMatchesTheIndependentEncoder() {
        // put_entry_content_sha256() in archive_operation_key_vectors.py encodes the
        // same request by hand. Changing this value changes every derived archive ID.
        assertThat(ArchiveOperationKeys.putEntryContentSha256(put().build()))
                .isEqualTo("4026ab1b39d5375d286b54ebee39651b3f7bb3571f6a4eedee563f7932f2c948");
    }

    @Test
    void theRequestedExpectedVersionIsPartOfTheKeyAndARetryDerivesTheSameId() {
        PutEntryRequest request = put().build();
        String digest = ArchiveOperationKeys.putEntryContentSha256(request);
        String id = ArchiveOperationKeys.operationId(request, digest);
        assertThat(ArchiveOperationKeys.operationId(request, digest)).isEqualTo(id);
        assertThat(ArchiveOperationKeys.operationId(put().setExpectedVersion(3).build(), digest)).isNotEqualTo(id);
        assertThat(ArchiveOperationKeys.operationId(put().setOperationId("caller-op").build(), digest))
                .isEqualTo("caller-op");
    }

    @Test
    void anUploadWithNeitherIdNorDigestIsRefusedNamingBoth() {
        UploadRenditionHeader header = UploadRenditionHeader.newBuilder()
                .setAddress(EntryAddress.newBuilder().setAccountId("acct").setArchive("reports").setEntryId("e1"))
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setSizeBytes(10)
                .build();
        String digest = ArchiveOperationKeys.uploadContentSha256(header);
        assertThatThrownBy(() -> ArchiveOperationKeys.operationId(header, digest))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(failure.getStatus().getDescription()).contains("operation_id, expected_sha256");
                });
        UploadRenditionHeader declared = header.toBuilder().setExpectedSha256("a".repeat(64)).build();
        String declaredDigest = ArchiveOperationKeys.uploadContentSha256(declared);
        assertThat(ArchiveOperationKeys.operationId(declared, declaredDigest))
                .isEqualTo(ArchiveOperationKeys.derive(ArchiveOperationKeys.uploadKey(declared, declaredDigest)).toString());
        assertThat(ArchiveOperationKeys.uploadContentSha256(declared.toBuilder().setOperationId("x").build()))
                .isEqualTo(declaredDigest);
    }

    @Test
    void anUploadKeyHoldsTheHeadersExpectedVersionOutsideItsContentDigest() {
        UploadRenditionHeader header = UploadRenditionHeader.newBuilder()
                .setAddress(EntryAddress.newBuilder().setAccountId("acct").setArchive("reports").setEntryId("e1"))
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setSizeBytes(10)
                .setExpectedSha256("a".repeat(64))
                .build();
        UploadRenditionHeader pinned = header.toBuilder().setExpectedVersion(4).build();
        String digest = ArchiveOperationKeys.uploadContentSha256(header);
        assertThat(ArchiveOperationKeys.uploadContentSha256(pinned)).isEqualTo(digest);
        assertThat(ArchiveOperationKeys.uploadKey(pinned, digest).getExpectedVersion()).isEqualTo(4);
        assertThat(ArchiveOperationKeys.operationId(pinned, digest))
                .isNotEqualTo(ArchiveOperationKeys.operationId(header, digest));
    }
}
