package ai.pipestream.proto.cel;

import java.util.List;
import java.util.Objects;

/**
 * Mapping instruction optionally gated by a CEL filter and populated by a CEL selector.
 *
 * @param filterExpression optional boolean CEL expression
 * @param selectorExpression optional CEL value expression
 * @param targetPath protobuf target path
 * @param textRuleFallback optional text mapping rules used when no selector is present
 */
public record CelMappingRule(
        String filterExpression,
        String selectorExpression,
        String targetPath,
        List<String> textRuleFallback) {

    public CelMappingRule {
        Objects.requireNonNull(targetPath, "targetPath");
        textRuleFallback = textRuleFallback == null ? List.of() : List.copyOf(textRuleFallback);
    }

    public CelMappingRule(String filterExpression, String selectorExpression, String targetPath) {
        this(filterExpression, selectorExpression, targetPath, List.of());
    }
}
