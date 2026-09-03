package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The snapshot family's parse: the bucket is the switch, partial families
 * refuse naming the missing variable, and nothing is ever assumed.
 */
class SearchSnapshotConfigTest {

    @Test
    void anEmptyFamilyMeansSnapshotsAreOff() {
        assertThat(SearchSnapshotConfig.fromEnvironment(Map.of())).isNull();
        assertThat(SearchSnapshotConfig.fromEnvironment(
                Map.of("DOCUMENT_PLATFORM_SEARCH_INDEX_DIR", "/data"))).isNull();
    }

    @Test
    void theFullFamilyParses() {
        SearchSnapshotConfig config = SearchSnapshotConfig.fromEnvironment(Map.of(
                SearchSnapshotConfig.ENV_BUCKET, "search-snaps",
                SearchSnapshotConfig.ENV_REGION, "us-east-1",
                SearchSnapshotConfig.ENV_ENDPOINT, "http://localhost:9000",
                SearchSnapshotConfig.ENV_ACCESS_KEY, "ak",
                SearchSnapshotConfig.ENV_SECRET_KEY, "sk"));
        assertThat(config.bucket()).isEqualTo("search-snaps");
        assertThat(config.prefix()).isEqualTo(SearchSnapshotConfig.DEFAULT_PREFIX);
        assertThat(config.endpoint()).isEqualTo("http://localhost:9000");
    }

    @Test
    void aFamilyMemberWithoutTheBucketRefusesByName() {
        assertThatThrownBy(() -> SearchSnapshotConfig.fromEnvironment(Map.of(
                SearchSnapshotConfig.ENV_REGION, "us-east-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SearchSnapshotConfig.ENV_REGION)
                .hasMessageContaining(SearchSnapshotConfig.ENV_BUCKET);
    }

    @Test
    void aBucketWithoutARegionRefusesByName() {
        assertThatThrownBy(() -> SearchSnapshotConfig.fromEnvironment(Map.of(
                SearchSnapshotConfig.ENV_BUCKET, "search-snaps")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SearchSnapshotConfig.ENV_REGION);
    }

    @Test
    void theReadOnlyFlagParsesStrictly() {
        assertThat(DocumentPlatform.searchReadOnly(Map.of())).isFalse();
        assertThat(DocumentPlatform.searchReadOnly(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "true"))).isTrue();
        assertThat(DocumentPlatform.searchReadOnly(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "false"))).isFalse();
        assertThatThrownBy(() -> DocumentPlatform.searchReadOnly(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "yes")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_SEARCH_READ_ONLY);
    }

    @Test
    void theRefreshIntervalParsesStrictly() {
        assertThat(DocumentPlatform.searchRefreshSeconds(Map.of())).isZero();
        assertThat(DocumentPlatform.searchRefreshSeconds(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS, "30"))).isEqualTo(30L);
        assertThatThrownBy(() -> DocumentPlatform.searchRefreshSeconds(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS, "soon")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS);
        assertThatThrownBy(() -> DocumentPlatform.searchRefreshSeconds(Map.of(
                DocumentPlatformConfig.ENV_SEARCH_REFRESH_SECONDS, "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unset it for restart-only");
    }

    @Test
    void aLoneStaticCredentialRefusesByName() {
        assertThatThrownBy(() -> SearchSnapshotConfig.fromEnvironment(Map.of(
                SearchSnapshotConfig.ENV_BUCKET, "search-snaps",
                SearchSnapshotConfig.ENV_REGION, "us-east-1",
                SearchSnapshotConfig.ENV_ACCESS_KEY, "ak")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SearchSnapshotConfig.ENV_SECRET_KEY);
    }
}
