package ai.protomolt.proto.repo.container.lifecycle;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CoherenceProbe.ProbeReport} arithmetic and the tombstone reason the
 * probe stamps on manifest entries (a stored contract — readers attribute
 * missing parts by it). The probe's row-repair behavior against a real
 * ledger and bucket is covered by CoherenceProbeIT.
 */
class CoherenceProbeTest {

    @Test
    void totalMissingSumsAcrossParts() {
        CoherenceProbe.ProbeReport report = new CoherenceProbe.ProbeReport(
                7, 12, Map.of("DOCUMENT_PART_CORE", 1, "DOCUMENT_PART_CHUNKS", 3));

        assertThat(report.rowsExamined()).isEqualTo(7);
        assertThat(report.objectsChecked()).isEqualTo(12);
        assertThat(report.totalMissing()).isEqualTo(4);
    }

    @Test
    void aCleanProbeReportsZeroMissing() {
        CoherenceProbe.ProbeReport report = new CoherenceProbe.ProbeReport(3, 9, Map.of());

        assertThat(report.totalMissing()).isZero();
    }

    @Test
    void theTombstoneReasonIsStable() {
        // Stamped into manifest entries' deleted_reason; changing it breaks
        // missing-part attribution for readers.
        assertThat(CoherenceProbe.DELETED_REASON).isEqualTo("COHERENCE_PROBE");
    }
}
