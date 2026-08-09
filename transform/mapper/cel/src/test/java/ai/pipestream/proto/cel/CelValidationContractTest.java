package ai.pipestream.proto.cel;

import dev.cel.bundle.Cel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link CelValidation} result contract: blank input, error contents, typed diagnostics. */
class CelValidationContractTest {

    private final Cel cel = CelEnvironmentFactory.builder().addVar("input").build();

    @Test
    void nullExpressionIsInvalidWithABlankMessage() {
        CelValidation.Result result = CelValidation.validate(cel, null);
        assertFalse(result.valid());
        assertTrue(result.errors().contains("Expression must not be blank"));
        assertTrue(result.warnings().isEmpty());
    }

    @Test
    void blankExpressionIsInvalidWithABlankMessage() {
        CelValidation.Result result = CelValidation.validate(cel, "   ");
        assertFalse(result.valid());
        assertTrue(result.errors().contains("Expression must not be blank"));
    }

    @Test
    void syntaxErrorPopulatesTheErrorList() {
        CelValidation.Result result = CelValidation.validate(cel, "1 +");
        assertFalse(result.valid());
        assertFalse(result.errors().isEmpty());
    }

    @Test
    void validExpressionHasNoErrors() {
        CelValidation.Result result = CelValidation.validate(cel, "1 + 1 == 2");
        assertTrue(result.valid());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    void typedUnknownFieldErrorNamesTheField() {
        Cel typed = CelEnvironmentFactory.builder()
                .addMessageVar("input", CelFixtures.DOCUMENT)
                .build();
        CelValidation.Result result = CelValidation.validate(typed, "input.nope");
        assertFalse(result.valid());
        assertTrue(result.errors().stream().anyMatch(e -> e.contains("nope")),
                "expected a field-level diagnostic naming 'nope', got: " + result.errors());
    }

    @Test
    void nullCelIsRejected() {
        assertThrows(NullPointerException.class, () -> CelValidation.validate(null, "true"));
    }
}
