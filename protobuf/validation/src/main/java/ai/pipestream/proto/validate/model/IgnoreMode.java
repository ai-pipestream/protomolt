package ai.pipestream.proto.validate.model;

/**
 * When a field's rules are skipped, independent of the dialect that expressed it.
 *
 * <p>Mirrors protovalidate's {@code Ignore} semantics: {@link #ALWAYS} never validates the field;
 * {@link #IF_ZERO_VALUE} skips it when unpopulated (unset, or equal to its zero value for
 * implicit-presence fields); {@link #UNSPECIFIED} applies rules normally, which — for fields that do
 * not track presence — means zero values are still validated.
 */
public enum IgnoreMode {
    UNSPECIFIED,
    IF_ZERO_VALUE,
    ALWAYS
}
