package ai.pipestream.proto.compat;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The value objects a check returns: {@link SchemaChange}, {@link CompatibilityResult} and the
 * two exception types. These are the programmatic surface gates and UIs consume, so the
 * defensive copies and the message formats are pinned here.
 */
class CompatibilityResultTest {

    private static final SchemaChange BREAKING = new SchemaChange(
            ChangeRules.FIELD_TYPE_CHANGED, "example.Doc.value",
            "int32 value = 1", "string value = 1",
            "Field example.Doc.value changed from int32 to string.",
            Set.of(Impact.WIRE_BACKWARD, Impact.WIRE_FORWARD));
    private static final SchemaChange INFORMATIONAL = new SchemaChange(
            ChangeRules.FIELD_ADDED, "example.Doc.label",
            "", "string label = 2",
            "Field example.Doc.label (number 2) was added.",
            Set.of());

    @Test
    void schemaChangeToStringIsRulePathMessage() {
        assertThat(BREAKING.toString()).isEqualTo(
                "FIELD_TYPE_CHANGED example.Doc.value: "
                        + "Field example.Doc.value changed from int32 to string.");
    }

    @Test
    void schemaChangeCopiesAndFreezesTheImpactSet() {
        Set<Impact> mutable = new HashSet<>(Set.of(Impact.SOURCE));
        SchemaChange change = new SchemaChange("RULE", "p", "", "", "m", mutable);

        mutable.add(Impact.WIRE_BACKWARD);
        assertThat(change.impacts()).containsExactly(Impact.SOURCE);
        assertThatThrownBy(() -> change.impacts().add(Impact.SOURCE))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void isInformationalReflectsTheImpactSet() {
        assertThat(INFORMATIONAL.isInformational()).isTrue();
        assertThat(BREAKING.isInformational()).isFalse();
    }

    @Test
    void resultCopiesAndFreezesBothLists() {
        List<SchemaChange> changes = new ArrayList<>(List.of(BREAKING));
        List<SchemaChange> violations = new ArrayList<>(List.of(BREAKING));
        CompatibilityResult result = new CompatibilityResult(
                CompatibilityMode.BACKWARD, changes, violations);

        changes.add(INFORMATIONAL);
        violations.clear();
        assertThat(result.changes()).containsExactly(BREAKING);
        assertThat(result.violations()).containsExactly(BREAKING);
        assertThatThrownBy(() -> result.changes().add(INFORMATIONAL))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.violations().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void compatibleResultNeverThrows() {
        CompatibilityResult result = new CompatibilityResult(
                CompatibilityMode.FULL, List.of(INFORMATIONAL), List.of());

        assertThat(result.isCompatible()).isTrue();
        assertThat(result.mode()).isEqualTo(CompatibilityMode.FULL);
        assertThatCode(result::throwIfIncompatible).doesNotThrowAnyException();
    }

    @Test
    void exceptionMessageUsesTheSingularForOneViolation() {
        CompatibilityResult result = new CompatibilityResult(
                CompatibilityMode.BACKWARD, List.of(BREAKING), List.of(BREAKING));

        assertThatThrownBy(result::throwIfIncompatible)
                .isInstanceOfSatisfying(IncompatibleSchemaException.class, e -> {
                    assertThat(e.getMessage()).contains("(1 violation)");
                    assertThat(e.getMessage()).doesNotContain("violations");
                    assertThat(e.getMessage()).contains("Schema is incompatible under BACKWARD");
                    assertThat(e.getMessage()).contains(BREAKING.toString());
                    assertThat(e.mode()).isEqualTo(CompatibilityMode.BACKWARD);
                    assertThat(e.violations()).containsExactly(BREAKING);
                });
    }

    @Test
    void exceptionMessageEnumeratesEveryViolationOnItsOwnLine() {
        SchemaChange second = new SchemaChange(
                ChangeRules.MESSAGE_REMOVED, "example.Gone", "message example.Gone", "",
                "Message example.Gone was removed.",
                Set.of(Impact.WIRE_BACKWARD));
        CompatibilityResult result = new CompatibilityResult(
                CompatibilityMode.FULL, List.of(BREAKING, second), List.of(BREAKING, second));

        assertThatThrownBy(result::throwIfIncompatible)
                .isInstanceOfSatisfying(IncompatibleSchemaException.class, e -> {
                    assertThat(e.getMessage()).contains("(2 violations)");
                    assertThat(e.getMessage().lines())
                            .anyMatch(line -> line.contains(BREAKING.toString()))
                            .anyMatch(line -> line.contains(second.toString()));
                    assertThat(e.violations()).containsExactly(BREAKING, second);
                });
    }

    @Test
    void compatibilityExceptionCarriesMessageAndCause() {
        CompatibilityException plain = new CompatibilityException("broken");
        assertThat(plain).hasMessage("broken").hasNoCause();

        RuntimeException cause = new RuntimeException("root");
        CompatibilityException wrapped = new CompatibilityException("broken", cause);
        assertThat(wrapped).hasMessage("broken").hasCause(cause);
    }
}
