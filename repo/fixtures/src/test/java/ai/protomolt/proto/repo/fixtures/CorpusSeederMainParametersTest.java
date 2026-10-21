package ai.protomolt.proto.repo.fixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The seeder's environment parsing, without a store. */
class CorpusSeederMainParametersTest {

    @Test void unsetOrBlankLargeBlobBytesKeepsTheDefault() {
        assertThat(CorpusSeederMain.parameters(Map.of()).largeBlobBytes())
                .isEqualTo(RepositoryCorpus.DEFAULT_LARGE_BLOB_BYTES);
        assertThat(CorpusSeederMain.parameters(Map.of(CorpusSeederMain.ENV_LARGE_BLOB_BYTES, "  "))
                .largeBlobBytes()).isEqualTo(RepositoryCorpus.DEFAULT_LARGE_BLOB_BYTES);
    }

    @Test void validLargeBlobBytesParseIncludingTheCap() {
        assertThat(CorpusSeederMain.parameters(Map.of(CorpusSeederMain.ENV_LARGE_BLOB_BYTES, "1"))
                .largeBlobBytes()).isEqualTo(1);
        assertThat(CorpusSeederMain.parameters(Map.of(CorpusSeederMain.ENV_LARGE_BLOB_BYTES, " 4096 "))
                .largeBlobBytes()).isEqualTo(4096);
        assertThat(CorpusSeederMain.parameters(Map.of(CorpusSeederMain.ENV_LARGE_BLOB_BYTES,
                Long.toString(CorpusSeederMain.MAX_LARGE_BLOB_BYTES)))
                .largeBlobBytes()).isEqualTo(CorpusSeederMain.MAX_LARGE_BLOB_BYTES);
    }

    @Test void malformedNonPositiveAndOverCapValuesAreRefusedByName() {
        for (String bad : new String[] {"abc", "4k", "0", "-1",
                Long.toString(CorpusSeederMain.MAX_LARGE_BLOB_BYTES + 1), "999999999999999"}) {
            assertThatThrownBy(() -> CorpusSeederMain.parameters(
                    Map.of(CorpusSeederMain.ENV_LARGE_BLOB_BYTES, bad)))
                    .as("value '%s'", bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("PROTOMOLT_CORPUS_LARGE_BLOB_BYTES");
        }
    }
}
