package ai.pipestream.proto.index.ndjson;

/**
 * Options for protobuf → NDJSON encoding (OpenSearch bulk–friendly).
 *
 * <p>{@code omitWhitespace} must stay {@code true} for line-oriented output:
 * {@link ProtoNdjsonWriter} rejects {@code omitWhitespace(false)} because pretty-printed
 * multi-line JSON is structurally invalid NDJSON / bulk output.
 */
public record NdjsonOptions(
        boolean preservingProtoFieldNames,
        boolean includingDefaultValueFields,
        boolean omitWhitespace) {

    public static NdjsonOptions defaults() {
        // Match opensearch-manager indexing: snake_case proto field names, compact lines.
        return new NdjsonOptions(true, false, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean preservingProtoFieldNames = true;
        private boolean includingDefaultValueFields = false;
        private boolean omitWhitespace = true;

        public Builder preservingProtoFieldNames(boolean value) {
            this.preservingProtoFieldNames = value;
            return this;
        }

        public Builder includingDefaultValueFields(boolean value) {
            this.includingDefaultValueFields = value;
            return this;
        }

        public Builder omitWhitespace(boolean value) {
            this.omitWhitespace = value;
            return this;
        }

        public NdjsonOptions build() {
            return new NdjsonOptions(preservingProtoFieldNames, includingDefaultValueFields, omitWhitespace);
        }
    }
}
