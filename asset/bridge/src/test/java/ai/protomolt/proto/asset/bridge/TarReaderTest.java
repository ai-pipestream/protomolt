package ai.protomolt.proto.asset.bridge;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bounded tests for the pax record walk: every payload must terminate
 * (the timeout turns a non-advancing loop into a failure, not a hang) and
 * yield the {@code path} record's value or null exactly as the pre-scan
 * split-based walk did.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class TarReaderTest {

    @Test
    void metadataOnlyPaxHeaderWithTrailingNewlineHasNoPath() {
        assertThat(TarReader.paxPath("13 mtime=1234\n")).isNull();
        assertThat(TarReader.paxPath("13 mtime=1234")).isNull();
    }

    @Test
    void emptyPaxPayloadHasNoPath() {
        assertThat(TarReader.paxPath("")).isNull();
        assertThat(TarReader.paxPath("\n")).isNull();
    }

    @Test
    void malformedLinesBeforeAPathRecordAreSkipped() {
        assertThat(TarReader.paxPath("\ninvalid\n16 path=foo/bar\n")).isEqualTo("foo/bar");
        assertThat(TarReader.paxPath("noequals\n=leading\n16 path=foo/bar")).isEqualTo("foo/bar");
    }

    @Test
    void pathRecordValueRunsToTheLineEnd() {
        assertThat(TarReader.paxPath("21 path=a/b=c d.txt\n13 mtime=1\n"))
                .isEqualTo("a/b=c d.txt");
        assertThat(TarReader.paxPath("16 path=foo/bar")).isEqualTo("foo/bar");
    }
}
