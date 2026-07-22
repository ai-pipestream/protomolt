package ai.pipestream.proto.mapper;

/**
 * A parsed text mapping rule.
 *
 * <pre>
 *   target = source     // assign
 *   target += source    // append
 *   -target             // clear
 * </pre>
 */
public record TextMappingRule(String targetPath, String sourcePath, Operation operation, String originalRule) {

    public enum Operation { ASSIGN, APPEND, CLEAR }
}
