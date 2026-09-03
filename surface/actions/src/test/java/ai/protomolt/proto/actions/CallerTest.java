package ai.protomolt.proto.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.Test;

class CallerTest {

    @Test
    void theOperatorHoldsEveryScopeInTheVocabulary() {
        Caller operator = Caller.operator();
        assertThat(operator.unrestricted()).isTrue();
        for (String scope : Scopes.VOCABULARY) {
            assertThat(operator.holds(scope)).as(scope).isTrue();
        }
    }

    @Test
    void aScopedCallerHoldsExactlyItsScopes() {
        Caller caller = Caller.scoped("ci-reader", Set.of(Scopes.SCHEMA_READ));
        assertThat(caller.holds(Scopes.SCHEMA_READ)).isTrue();
        assertThat(caller.holds(Scopes.SCHEMA_WRITE)).isFalse();
        assertThat(caller.unrestricted()).isFalse();
        assertThat(caller.name()).isEqualTo("ci-reader");
    }

    @Test
    void aBlankNameRefuses() {
        assertThatThrownBy(() -> Caller.scoped(" ", Set.of(Scopes.SCHEMA_READ)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
    }

    @Test
    void anUnknownScopeRefusesNamingIt() {
        assertThatThrownBy(() -> Caller.scoped("x", Set.of("schema-red")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema-red");
    }

    @Test
    void anUnrestrictedCallerEnumeratingScopesRefuses() {
        assertThatThrownBy(() -> new Caller("x", Set.of(Scopes.SCHEMA_READ), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unrestricted");
    }

    @Test
    void theCallersScopeSetIsImmutable() {
        Caller caller = Caller.scoped("x", Set.of(Scopes.SCHEMA_READ));
        assertThatThrownBy(() -> caller.scopes().add(Scopes.SCHEMA_WRITE))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
