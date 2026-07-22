package ai.pipestream.proto.cel;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CelValidationTest {
    private final dev.cel.bundle.Cel cel = CelEnvironmentFactory.builder().addVar("input").build();

    @Test void acceptsValidBoolean() { assertTrue(CelValidation.validate(cel, "true").valid()); }
    @Test void rejectsSyntaxError() { assertFalse(CelValidation.validate(cel, "input.").valid()); }
    @Test void rejectsBlankExpression() { assertFalse(CelValidation.validate(cel, " ").valid()); }
    @Test void acceptsNonBooleanValueContext() { assertTrue(CelValidation.validate(cel, "'value'").valid()); }
    @Test void evaluatorAndValidatorAgreeOnCompilableExpression() {
        String expression = "1 + 2";
        assertTrue(CelValidation.validate(cel, expression).valid());
        assertEquals(3L, new CelEvaluator(cel).evaluateValue(expression, Map.of()));
    }
    @Test void dynTypoIsValidOrReportedAsAdvisory() {
        var result = CelValidation.validate(cel, "input.misspelled");
        assertTrue(result.valid() || !result.warnings().isEmpty(),
                "DYN field access should compile or be advisory-only");
    }
}
