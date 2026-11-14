package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.NodeAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExternalPublicationCoordinates}: bijective binary source keys,
 * collision-free ledger document IDs, circular-loop graph guards, and zero-regex
 * display URL / non-secret label sanitizers.
 */
class ExternalPublicationCoordinatesTest {

    @Test
    void logicalSourceKeyIsBijectiveAcrossDelimitersUnicodeAndNuls() {
        List<NodeAddress> samples = List.of(
                NodeAddress.newBuilder()
                        .setAccountId("tenant-alpha")
                        .setGraphId("intake:tenant-alpha")
                        .setGraphAddressId("ds-1")
                        .setDocId("doc-1")
                        .build(),
                NodeAddress.newBuilder()
                        .setAccountId("a:b/c")
                        .setGraphId("g/1:2")
                        .setGraphAddressId("n:3/4")
                        .setDocId("d/5:6?x=1#frag")
                        .build(),
                NodeAddress.newBuilder()
                        .setAccountId("tenant-日本語")
                        .setGraphId("graph-🚀")
                        .setGraphAddressId("node-\u00e9\u0301")
                        .setDocId("doc-Δ-001")
                        .build(),
                NodeAddress.newBuilder()
                        .setAccountId("")
                        .setGraphId("")
                        .setGraphAddressId("")
                        .setDocId("")
                        .build());

        for (NodeAddress address : samples) {
            byte[] encoded = ExternalPublicationCoordinates.logicalSourceKey(address);
            assertThat(encoded[0]).isEqualTo(ExternalPublicationCoordinates.LOGICAL_SOURCE_KEY_VERSION);
            NodeAddress roundTrip = ExternalPublicationCoordinates.decodeLogicalSourceKey(encoded);
            assertThat(roundTrip).isEqualTo(address);
        }
    }

    @Test
    void ledgerDocIdSeparatesShiftedFieldBoundariesWithoutCollision() {
        NodeAddress left = NodeAddress.newBuilder()
                .setAccountId("acct")
                .setGraphId("ab:c")
                .setGraphAddressId("de")
                .setDocId("fg")
                .build();
        NodeAddress right = NodeAddress.newBuilder()
                .setAccountId("acct")
                .setGraphId("ab")
                .setGraphAddressId("c:de")
                .setDocId("fg")
                .build();

        assertThat(ExternalPublicationCoordinates.logicalSourceKey(left))
                .isNotEqualTo(ExternalPublicationCoordinates.logicalSourceKey(right));
        assertThat(ExternalPublicationCoordinates.ledgerDocId(left))
                .isNotEqualTo(ExternalPublicationCoordinates.ledgerDocId(right));
        assertThat(ExternalPublicationCoordinates.ledgerAddress("acct", "search-prod", left))
                .isNotEqualTo(ExternalPublicationCoordinates.ledgerAddress("acct", "search-prod", right));
    }

    @Test
    void decodeLogicalSourceKeyRejectsCorruptVersionShortAndTrailingBytes() {
        assertThatThrownBy(() -> ExternalPublicationCoordinates.decodeLogicalSourceKey(new byte[10]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short");

        NodeAddress valid = NodeAddress.newBuilder()
                .setAccountId("acct")
                .setGraphId("intake:acct")
                .setGraphAddressId("ds")
                .setDocId("doc")
                .build();
        byte[] encoded = ExternalPublicationCoordinates.logicalSourceKey(valid);

        byte[] wrongVersion = encoded.clone();
        wrongVersion[0] = 2;
        assertThatThrownBy(() -> ExternalPublicationCoordinates.decodeLogicalSourceKey(wrongVersion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");

        byte[] trailing = new byte[encoded.length + 1];
        System.arraycopy(encoded, 0, trailing, 0, encoded.length);
        trailing[encoded.length] = 0x7f;
        assertThatThrownBy(() -> ExternalPublicationCoordinates.decodeLogicalSourceKey(trailing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");

        ByteBuffer badLen = ByteBuffer.allocate(17);
        badLen.put((byte) 1);
        badLen.putInt(-1);
        badLen.putInt(0);
        badLen.putInt(0);
        badLen.putInt(0);
        assertThatThrownBy(() -> ExternalPublicationCoordinates.decodeLogicalSourceKey(badLen.array()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment length");
    }

    @Test
    void internalExternalGraphPredicatesRejectPublishingInternalLedgersOrBindings() {
        NodeAddress bindingAddr = ExternalPublicationCoordinates.bindingAddress("tenant-1", "search-v1");
        NodeAddress sourceAddr = NodeAddress.newBuilder()
                .setAccountId("tenant-1")
                .setGraphId("intake:tenant-1")
                .setGraphAddressId("ds-1")
                .setDocId("doc-42")
                .build();
        NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                "tenant-1", "search-v1", sourceAddr);

        assertThat(ExternalPublicationCoordinates.isInternalExternalAddress(bindingAddr)).isTrue();
        assertThat(ExternalPublicationCoordinates.isInternalExternalAddress(ledgerAddr)).isTrue();
        assertThat(ExternalPublicationCoordinates.isInternalExternalAddress(sourceAddr)).isFalse();

        assertThatThrownBy(() -> ExternalPublicationCoordinates.requireSourceAddress(bindingAddr))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Internal external-binding");
        assertThatThrownBy(() -> ExternalPublicationCoordinates.requireSourceAddress(ledgerAddr))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Internal external-binding");
    }

    @Test
    void validateDisplayUrlAcceptsCleanHttpHttpsAndRejectsDangerousOrSecretUrls() {
        assertThatCode(() -> ExternalPublicationCoordinates.validateDisplayUrl(
                "https://search.example.com/collections/docs/items/doc-1?view=summary#top"))
                .doesNotThrowAnyException();
        assertThatCode(() -> ExternalPublicationCoordinates.validateDisplayUrl(
                "http://[::1]:8080/items/42"))
                .doesNotThrowAnyException();

        List<String> rejected = List.of(
                "javascript:alert(1)",
                "data:text/html;base64,PHNjcmlwdD4=",
                "ftp://files.example.com/item",
                "https://user:pass@search.example.com/item",
                "https://token@search.example.com/item",
                "https://search.example.com/item?access_token=secret123",
                "https://search.example.com/item?a=1&Api_Key=secret123",
                "https://search.example.com/item?%74%6F%6B%65%6E=secret123",
                "https://search.example.com/item?X-Amz-Signature=deadbeef",
                "https://search.example.com/item with space",
                "https:///missing-host",
                "https://host:99999/item");

        for (String bad : rejected) {
            assertThatThrownBy(() -> ExternalPublicationCoordinates.validateDisplayUrl(bad))
                    .as("Expected displayUrl refusal for: %s", bad)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void validateNonSecretLabelsRejectsCredentialKeysAndBearerMaterial() {
        assertThatCode(() -> ExternalPublicationCoordinates.validateNonSecretLabels(
                Map.of("environment", "production", "region", "us-east-1")))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> ExternalPublicationCoordinates.validateNonSecretLabels(
                Map.of("api_key", "abc")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Secret-bearing");

        assertThatThrownBy(() -> ExternalPublicationCoordinates.validateNonSecretLabels(
                Map.of("endpoint_hint", "Bearer eyJhbGciOiJIUzI1NiJ9.payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credential or key material");

        assertThatThrownBy(() -> ExternalPublicationCoordinates.requireNonSecretText(
                "-----BEGIN PRIVATE KEY-----", "namespace", 256))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credential or key material");
    }

    @Test
    void configDocumentNameIsRegistrySafeAndCollisionFreeAcrossBoundaries() {
        String left = ExternalPublicationCoordinates.configDocumentName("acct-a", "b-search");
        String right = ExternalPublicationCoordinates.configDocumentName("acct", "a-b-search");
        String unicodeAcct = ExternalPublicationCoordinates.configDocumentName("org:tenant/alpha", "search_v1.prod");

        assertThat(left).isNotEqualTo(right);
        assertThat(left).startsWith("external-binding.");
        assertThat(right).startsWith("external-binding.");
        assertThat(unicodeAcct).startsWith("external-binding.");
        for (String candidate : List.of(left, right, unicodeAcct)) {
            for (int i = 0; i < candidate.length(); i++) {
                char c = candidate.charAt(i);
                boolean allowed = (c >= 'A' && c <= 'Z')
                        || (c >= 'a' && c <= 'z')
                        || (c >= '0' && c <= '9')
                        || c == '.'
                        || c == '_'
                        || c == '-';
                assertThat(allowed)
                        .as("Character '%s' at index %d in %s must match [A-Za-z0-9._-]+", c, i, candidate)
                        .isTrue();
            }
        }

        assertThatThrownBy(() -> ExternalPublicationCoordinates.configDocumentName("acct", "bad/slash"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bindingId");
    }
}
