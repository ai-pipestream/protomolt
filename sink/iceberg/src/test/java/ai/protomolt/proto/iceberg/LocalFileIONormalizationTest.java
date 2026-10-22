package ai.protomolt.proto.iceberg;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free {@code file:} URI normalization, checked against the
 * {@code replaceFirst("^file:/+", "file:///")} it replaced.
 */
class LocalFileIONormalizationTest {

    @Test
    void normalizationMatchesTheFormerReplaceFirst() {
        for (String location : new String[]{
                "file:/tmp/a", "file://tmp/a", "file:///tmp/a", "file:////tmp/a",
                "file:/", "file://", "file:///", "file:",
                "file:tmp/a", "/tmp/a", "", "FILE:/tmp/a", "file:/a/file:/b"}) {
            assertThat(LocalFileIO.normalizeFileUri(location))
                    .as("normalization of '%s'", location)
                    .isEqualTo(location.replaceFirst("^file:/+", "file:///"));
        }
    }
}
