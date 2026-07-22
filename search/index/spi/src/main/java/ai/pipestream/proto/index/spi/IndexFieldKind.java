package ai.pipestream.proto.index.spi;

/**
 * Lucene-aligned field kinds shared by all search-engine plugins.
 * Mirrors {@code IndexFieldType} in {@code indexing_hints.proto}.
 */
public enum IndexFieldKind {
    UNSPECIFIED,
    TEXT,
    KEYWORD,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    DATE,
    BINARY,
    VECTOR,
    OBJECT,
    NESTED,
    SKIP,
    INT_RANGE,
    LONG_RANGE,
    FLOAT_RANGE,
    DOUBLE_RANGE,
    DATE_RANGE;

    /** True for the range kinds, which apply to singular message fields with bound pairs. */
    public boolean isRange() {
        return switch (this) {
            case INT_RANGE, LONG_RANGE, FLOAT_RANGE, DOUBLE_RANGE, DATE_RANGE -> true;
            default -> false;
        };
    }
}
