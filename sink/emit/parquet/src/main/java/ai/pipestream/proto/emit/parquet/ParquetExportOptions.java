package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.meta.SensitivityMasker;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * What to leave out of a Parquet export and what to obscure in it. Two independent tools:
 *
 * <ul>
 *   <li><b>Projection</b> drops columns from the file entirely: only the named top-level fields
 *       are written, and the file schema has no others.</li>
 *   <li><b>Masking</b> keeps every projected column but runs each message through
 *       {@link SensitivityMasker} first, so fields carrying one of {@code maskClasses} are
 *       redacted, encrypted, or cleared before they are written.</li>
 * </ul>
 *
 * <p>They compose. Note the distinction that matters for truly dropping a sensitive value: a
 * proto3 plain scalar is a Parquet {@code required} column, so {@code REMOVE} only clears it to
 * its default (a zero still writes) - to keep a sensitive column out of the file, <em>project it
 * out</em>. {@code REDACT}/{@code ENCRYPT} keep the column and obscure the value. Masking reads the
 * {@code sensitivity} field option, so the descriptor must have been compiled with ProtoMolt's
 * metadata extensions or nothing is masked.</p>
 *
 * <p>Independently of both, every message is validated against the constraint and CEL rules its
 * descriptor declares (see {@code ai.pipestream.proto.validate.ProtoValidator}) before it is
 * written, and the write fails on the first offending message. {@link #withoutValidation()}
 * opts out; nothing else here affects validation.</p>
 */
public record ParquetExportOptions(Set<String> columns, Set<String> maskClasses,
                                   SensitivityMasker.Strategy maskStrategy, byte[] maskKey,
                                   boolean skipValidation) {

    /** Neither projection nor masking: write every column, unchanged (validation still runs). */
    public static final ParquetExportOptions NONE =
            new ParquetExportOptions(Set.of(), Set.of(), null, null, false);

    /**
     * Projection/masking-only form, validation on. Equivalent to the canonical constructor
     * with {@code skipValidation} {@code false}.
     */
    public ParquetExportOptions(Set<String> columns, Set<String> maskClasses,
                                SensitivityMasker.Strategy maskStrategy, byte[] maskKey) {
        this(columns, maskClasses, maskStrategy, maskKey, false);
    }

    public ParquetExportOptions {
        columns = columns == null ? Set.of() : Set.copyOf(columns);
        maskClasses = maskClasses == null ? Set.of() : Set.copyOf(maskClasses);
        // Named classes with no strategy would write every sensitive column in the clear, which
        // is the opposite of what the caller asked for; refuse rather than silently not mask.
        if (!maskClasses.isEmpty() && maskStrategy == null) {
            throw new IllegalArgumentException("Mask classes " + maskClasses
                    + " were requested without a masking strategy");
        }
        maskKey = maskKey == null ? null : maskKey.clone();
    }

    /** Keep only these top-level columns; mask nothing. */
    public static ParquetExportOptions project(Set<String> columns) {
        return new ParquetExportOptions(columns, Set.of(), null, null);
    }

    /**
     * Mask fields in these sensitivity classes with {@code strategy}; keep every column.
     *
     * @throws IllegalArgumentException if {@code maskClasses} is non-empty and
     *         {@code strategy} is {@code null}
     */
    public static ParquetExportOptions masking(Set<String> maskClasses,
                                               SensitivityMasker.Strategy strategy, byte[] key) {
        return new ParquetExportOptions(Set.of(), maskClasses, strategy, key);
    }

    /**
     * A copy of these options with validate-on-write turned off. Skipping validation is for
     * replaying files whose rows predate the rules, or for bulk loads that already validated
     * upstream - the write stops checking the descriptor's constraint and CEL rules entirely,
     * so invalid rows land in the file without a word.
     */
    public ParquetExportOptions withoutValidation() {
        return new ParquetExportOptions(columns, maskClasses, maskStrategy, maskKey, true);
    }

    boolean masks() {
        return !maskClasses.isEmpty();
    }

    @Override
    public byte[] maskKey() {
        return maskKey == null ? null : maskKey.clone();
    }

    // The record's generated members compare and print the key array by identity, so two
    // options built from equal keys would be unequal and toString would leak nothing useful.

    @Override
    public boolean equals(Object o) {
        return o instanceof ParquetExportOptions other
                && columns.equals(other.columns)
                && maskClasses.equals(other.maskClasses)
                && maskStrategy == other.maskStrategy
                && Arrays.equals(maskKey, other.maskKey)
                && skipValidation == other.skipValidation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, maskClasses, maskStrategy, Arrays.hashCode(maskKey),
                skipValidation);
    }

    /** The key is summarized by length only; its bytes are secret. */
    @Override
    public String toString() {
        return "ParquetExportOptions[columns=" + columns
                + ", maskClasses=" + maskClasses
                + ", maskStrategy=" + maskStrategy
                + ", maskKey=" + (maskKey == null ? "none" : maskKey.length + " bytes")
                + ", skipValidation=" + skipValidation + "]";
    }
}
