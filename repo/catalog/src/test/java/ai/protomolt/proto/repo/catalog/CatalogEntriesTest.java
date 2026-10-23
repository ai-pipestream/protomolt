package ai.protomolt.proto.repo.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The catalog's byte bounds count UTF-8 bytes, including the encoder's unpaired-surrogate replacement. */
class CatalogEntriesTest {

    @Test
    void titleBoundCountsUtf8Bytes() {
        assertThat(CatalogEntries.unstorable(title("é".repeat(1000)))).isEmpty();
        assertThat(CatalogEntries.unstorable(title("é".repeat(1000) + "a")))
                .extracting(CatalogEntries.Unstorable::message)
                .containsExactly("exceeds " + CatalogEntries.MAX_TITLE_BYTES + " bytes");
        assertThat(CatalogEntries.unstorable(title("😀".repeat(500)))).isEmpty();
        assertThat(CatalogEntries.unstorable(title("😀".repeat(501)))).isNotEmpty();
        assertThat(CatalogEntries.unstorable(title("水".repeat(666)))).isEmpty();
        assertThat(CatalogEntries.unstorable(title("水".repeat(667)))).isNotEmpty();
    }

    @Test
    void unpairedSurrogateCountsAsTheUtf8ReplacementByte() {
        String lone = "\uD83D".repeat(CatalogEntries.MAX_TITLE_BYTES);
        assertThat(lone.getBytes(StandardCharsets.UTF_8)).hasSize(CatalogEntries.MAX_TITLE_BYTES);
        assertThat(CatalogEntries.unstorable(title(lone))).isEmpty();
        assertThat(CatalogEntries.unstorable(title(lone + "a"))).isNotEmpty();
    }

    @Test
    void nulAndMetadataKeyBoundsStayInPlace() {
        assertThat(CatalogEntries.unstorable(title("a\u0000b")))
                .extracting(CatalogEntries.Unstorable::message)
                .containsExactly("contains a NUL character");
        assertThat(CatalogEntries.unstorable(metadata("k".repeat(CatalogEntries.MAX_METADATA_KEY_BYTES)))).isEmpty();
        assertThat(CatalogEntries.unstorable(metadata("水".repeat(67))))
                .extracting(CatalogEntries.Unstorable::message)
                .containsExactly("key exceeds " + CatalogEntries.MAX_METADATA_KEY_BYTES + " bytes");
    }

    @Test
    void linkFieldPathAndKeyBoundsCountUtf8Bytes() {
        assertThat(CatalogEntries.unstorableLink("é".repeat(512), "source", Optional.empty())).isEmpty();
        assertThat(CatalogEntries.unstorableLink("é".repeat(512) + "a", "source", Optional.empty()))
                .contains("field path exceeds " + CatalogEntries.MAX_LINK_FIELD_PATH_BYTES + " bytes");
        assertThat(CatalogEntries.unstorableLink("path\u0000", "source", Optional.empty()))
                .contains("field path contains a NUL character");
        var target = NodeAddress.newBuilder().setAccountId("a").setGraphId("g").setGraphAddressId("ga").setDocId("d").build();
        assertThat(CatalogEntries.unstorableLink("path", "水".repeat(682), Optional.of(target)))
                .contains("relation and target call number exceed " + CatalogEntries.MAX_LINK_KEY_BYTES + " bytes");
    }

    private static DocumentLookup title(String title) {
        return DocumentLookup.newBuilder().setTitle(title).build();
    }

    private static DocumentLookup metadata(String key) {
        return DocumentLookup.newBuilder().putMetadata(key, "v").build();
    }
}
