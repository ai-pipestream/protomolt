package ai.protomolt.proto.search.index.spi;

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
    /**
     * {@code google.protobuf.Any}: not a silent {@link #OBJECT}. Mapping time keeps a single
     * entry; write time unpacks through the {@code DescriptorRegistry} and indexes the
     * packed message's fields. Schema generators emit no inner mappings for this kind.
     */
    ANY,
    INT_RANGE,
    LONG_RANGE,
    FLOAT_RANGE,
    DOUBLE_RANGE,
    DATE_RANGE,
    /**
     * Taxonomy path over a message with a repeated string {@code segments} field
     * (canonically {@code ai.protomolt.proto.types.v1.TreePath}). Engines emit the
     * ancestor chain ("a", "a/b", "a/b/c") as keyword terms for hierarchical
     * drill-down facets and path-prefix filters.
     */
    TREE_PATH;

    /** True for the range kinds, which apply to singular message fields with bound pairs. */
    public boolean isRange() {
        return switch (this) {
            case INT_RANGE, LONG_RANGE, FLOAT_RANGE, DOUBLE_RANGE, DATE_RANGE -> true;
            default -> false;
        };
    }
}
