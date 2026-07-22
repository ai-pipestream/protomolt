package ai.pipestream.proto.cel;

import dev.cel.bundle.Cel;
import dev.cel.common.CelIssue;

import java.util.List;
import java.util.Objects;

/** CEL compilation validation with optional advisory type diagnostics. */
public final class CelValidation {
    private CelValidation() {
    }

    public static Result validate(Cel cel, String expression) {
        Objects.requireNonNull(cel, "cel");
        if (expression == null || expression.isBlank()) {
            return new Result(false, List.of("Expression must not be blank"), List.of());
        }
        var result = cel.compile(expression);
        List<String> errors = result.getErrors().stream().map(CelIssue::getMessage).toList();
        List<String> warnings = result.getAllIssues().stream()
                .filter(issue -> !result.getErrors().contains(issue))
                .map(CelIssue::getMessage)
                .toList();
        return new Result(!result.hasError(), errors, warnings);
    }

    public record Result(boolean valid, List<String> errors, List<String> warnings) {
    }
}
