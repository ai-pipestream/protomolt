package ai.protomolt.proto.grpc.workspace;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ReflectedServiceActions#kebab(String)} turns a method or profile name into the
 * kebab form a caller types: camel humps become dashes, underscores and dots become
 * dashes, and a name already kebab stays as it is.
 */
class ReflectedVerbNamingTest {

    @Test
    void camelCaseBecomesKebab() {
        assertThat(ReflectedServiceActions.kebab("ListOrders")).isEqualTo("list-orders");
    }

    @Test
    void kebabNameStaysAsItIs() {
        assertThat(ReflectedServiceActions.kebab("already-kebab")).isEqualTo("already-kebab");
    }

    @Test
    void underscoresAndDotsBecomeDashes() {
        assertThat(ReflectedServiceActions.kebab("with_underscore.dot"))
                .isEqualTo("with-underscore-dot");
    }

    @Test
    void consecutiveUppercaseGetsOneDashPerLetter() {
        assertThat(ReflectedServiceActions.kebab("HTTPGet")).isEqualTo("h-t-t-p-get");
    }

    @Test
    void leadingUppercaseGetsNoLeadingDash() {
        assertThat(ReflectedServiceActions.kebab("Charge")).isEqualTo("charge");
    }

    @Test
    void digitsPassThrough() {
        assertThat(ReflectedServiceActions.kebab("Sha256Sum")).isEqualTo("sha256-sum");
    }
}
