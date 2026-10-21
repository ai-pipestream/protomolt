package ai.protomolt.proto.delegation.repository;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyReferencesTest {
    @Test void referencesAndVariablesPreserveAsciiGrammarAndBounds() {
        for (String name : List.of("", "_", "KEY_1", "1KEY", "é", "a".repeat(128), "a".repeat(129))) {
            assertEquals(name.matches("[A-Za-z_][A-Za-z0-9_]{0,127}"), KeyReferences.variable(name));
            for (String scheme : List.of("", "env", "a+1.-", "_env", "a".repeat(32), "a".repeat(33))) {
                String text = scheme + ":" + name;
                assertEquals(text.matches("[A-Za-z][A-Za-z0-9+.-]{0,31}:[A-Za-z_][A-Za-z0-9_]{0,127}"),
                        KeyReferences.reference(text), text);
            }
        }
        for (char c = 0; c < 256; c++) {
            String text = "e" + c + ":A" + c;
            assertEquals(text.matches("[A-Za-z][A-Za-z0-9+.-]{0,31}:[A-Za-z_][A-Za-z0-9_]{0,127}"),
                    KeyReferences.reference(text), text);
        }
    }
}
