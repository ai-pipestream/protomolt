package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** The pre-size hint for buffered parts never trusts the catalog's recorded length. */
class DocumentStreamsTest {

    @Test
    void preSizeHintIsCappedAtOneChunkForOversizedOrCorruptCatalogLengths() {
        assertThat(DocumentStreams.boundedPreSize(Long.MAX_VALUE)).isEqualTo(DocumentStreams.CHUNK_SIZE);
        assertThat(DocumentStreams.boundedPreSize(2L * Integer.MAX_VALUE)).isEqualTo(DocumentStreams.CHUNK_SIZE);
        assertThat(DocumentStreams.boundedPreSize(DocumentStreams.CHUNK_SIZE + 1L)).isEqualTo(DocumentStreams.CHUNK_SIZE);
    }

    @Test
    void preSizeHintPassesHonestSmallLengthsThroughAndClampsNegativeOnes() {
        assertThat(DocumentStreams.boundedPreSize(0)).isZero();
        assertThat(DocumentStreams.boundedPreSize(17)).isEqualTo(17);
        assertThat(DocumentStreams.boundedPreSize(DocumentStreams.CHUNK_SIZE)).isEqualTo(DocumentStreams.CHUNK_SIZE);
        assertThat(DocumentStreams.boundedPreSize(-1)).isZero();
        assertThat(DocumentStreams.boundedPreSize(Long.MIN_VALUE)).isZero();
    }
}
