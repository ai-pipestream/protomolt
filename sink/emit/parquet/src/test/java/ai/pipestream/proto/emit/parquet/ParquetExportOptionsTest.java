package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.meta.SensitivityMasker;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The export options' own invariants: a masking request must carry a strategy, and the key array
 * must not leak the record's default identity semantics into equality or into {@code toString}.
 */
class ParquetExportOptionsTest {

    private static final byte[] KEY =
            "0123456789abcdef".getBytes(StandardCharsets.UTF_8);

    /**
     * Silently not masking is the worst outcome: the caller named the sensitive classes and would
     * have got them written in the clear.
     */
    @Test
    void maskClassesWithoutAStrategyAreRejected() {
        assertThatThrownBy(() -> ParquetExportOptions.masking(Set.of("pii"), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pii")
                .hasMessageContaining("without a masking strategy");

        assertThatThrownBy(() -> new ParquetExportOptions(Set.of(), Set.of("secret"), null, KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secret");
    }

    @Test
    void noMaskClassesWithoutAStrategyIsTheNoMaskingCase() {
        assertThat(ParquetExportOptions.NONE.masks()).isFalse();
        assertThat(ParquetExportOptions.project(Set.of("id")).masks()).isFalse();
        assertThat(ParquetExportOptions.masking(Set.of("pii"),
                SensitivityMasker.Strategy.REDACT, null).masks()).isTrue();
    }

    @Test
    void optionsWithEqualKeysAreEqual() {
        ParquetExportOptions one = ParquetExportOptions.masking(Set.of("pii"),
                SensitivityMasker.Strategy.ENCRYPT, KEY.clone());
        ParquetExportOptions two = ParquetExportOptions.masking(Set.of("pii"),
                SensitivityMasker.Strategy.ENCRYPT, KEY.clone());
        ParquetExportOptions other = ParquetExportOptions.masking(Set.of("pii"),
                SensitivityMasker.Strategy.ENCRYPT,
                "fedcba9876543210".getBytes(StandardCharsets.UTF_8));

        assertThat(one).isEqualTo(two).hasSameHashCodeAs(two);
        assertThat(one).isNotEqualTo(other);
        assertThat(ParquetExportOptions.NONE)
                .isEqualTo(new ParquetExportOptions(Set.of(), Set.of(), null, null));
    }

    @Test
    void toStringSummarizesTheKeyWithoutPrintingIt() {
        String text = ParquetExportOptions.masking(Set.of("pii"),
                SensitivityMasker.Strategy.ENCRYPT, KEY).toString();
        assertThat(text).contains("16 bytes").doesNotContain("0123456789abcdef");
        assertThat(ParquetExportOptions.NONE.toString()).contains("maskKey=none");
    }
}
