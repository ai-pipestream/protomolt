package ai.protomolt.proto.cel;

import dev.cel.bundle.Cel;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.SimpleType;
import dev.cel.runtime.CelFunctionBinding;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Environment construction: typed message variables, variable typing, and custom functions. */
class CelEnvironmentFactoryTest {

    @Test
    void nullArgumentsAreRejected() {
        var factory = CelEnvironmentFactory.builder();
        assertThrows(NullPointerException.class, () -> factory.addMessageType(null));
        assertThrows(NullPointerException.class, () -> factory.addVar(null));
        assertThrows(NullPointerException.class, () -> factory.addVar("x", null));
    }

    @Test
    void buildReturnsAWorkingCel() {
        Cel cel = CelEnvironmentFactory.builder().addVar("input").build();
        assertNotNull(cel);
        assertTrue(CelValidation.validate(cel, "1 + 1 == 2").valid());
    }

    @Test
    void typedMessageVariableTypeChecksFieldAccess() {
        Cel cel = CelEnvironmentFactory.builder()
                .addMessageVar("input", CelFixtures.DOCUMENT)
                .build();
        assertTrue(CelValidation.validate(cel, "input.title == 'x'").valid());
        assertFalse(CelValidation.validate(cel, "input.nope == 'x'").valid(),
                "unknown field on a typed message variable must fail compilation");
    }

    @Test
    void typedMessageVariableRejectsTypeMismatchedComparison() {
        Cel cel = CelEnvironmentFactory.builder()
                .addMessageVar("input", CelFixtures.DOCUMENT)
                .build();
        // score is a double; comparing it to a string is a compile-time type error.
        assertFalse(CelValidation.validate(cel, "input.score == 'x'").valid());
    }

    @Test
    void lastVariableDeclarationWins() {
        Cel cel = CelEnvironmentFactory.builder()
                .addVar("x", SimpleType.INT)
                .addVar("x", SimpleType.STRING)
                .build();
        assertTrue(CelValidation.validate(cel, "x + 'a'").valid());
        assertFalse(CelValidation.validate(cel, "x + 1").valid());
    }

    @Test
    void customFunctionsExtendTheEnvironment() {
        Cel cel = CelEnvironmentFactory.builder()
                .addFunctions(
                        List.of(CelFunctionDecl.newFunctionDeclaration("shout",
                                CelOverloadDecl.newGlobalOverload(
                                        "shout_string", SimpleType.STRING, SimpleType.STRING))),
                        List.of(CelFunctionBinding.from("shout_string", String.class,
                                (String s) -> s.toUpperCase(Locale.ROOT))))
                .build();
        assertEquals("HI", new CelEvaluator(cel).evaluateValue("shout('hi')", Map.of()));
    }

    @Test
    void undeclaredFunctionDoesNotCompile() {
        Cel cel = CelEnvironmentFactory.builder().addVar("input").build();
        assertFalse(CelValidation.validate(cel, "shout('hi')").valid());
    }

    @Test
    void advisoryBuilderAllowsFurtherCustomization() {
        Cel cel = CelEnvironmentFactory.builder()
                .addVar("input")
                .advisoryBuilder()
                .addVar("extra", SimpleType.INT)
                .build();
        assertTrue(CelValidation.validate(cel, "extra + 1").valid());
    }
}
