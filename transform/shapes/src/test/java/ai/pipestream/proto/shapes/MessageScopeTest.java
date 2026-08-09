package ai.pipestream.proto.shapes;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The resolution scope itself: an ordered, duplicate-free set of named messages with a
 * defensive bindings view. Every combination surface resolves {@code name.path} against it.
 */
class MessageScopeTest {

    private static final StringValue FIRST = StringValue.of("first");
    private static final StringValue SECOND = StringValue.of("second");

    @Test
    void lookupsAndNamesFollowInsertionOrder() {
        MessageScope scope = MessageScope.builder()
                .add("beta", FIRST)
                .add("alpha", SECOND)
                .build();
        assertThat(scope.get("beta")).isSameAs(FIRST);
        assertThat(scope.get("alpha")).isSameAs(SECOND);
        assertThat(scope.names()).containsExactly("beta", "alpha");
    }

    @Test
    void unknownNamesResolveToNull() {
        MessageScope scope = MessageScope.builder().add("order", FIRST).build();
        assertThat(scope.get("invoice")).isNull();
    }

    @Test
    void bindingsAreAnUnmodifiableViewOfTheEntries() {
        MessageScope scope = MessageScope.builder().add("order", FIRST).build();
        assertThat(scope.asBindings()).containsEntry("order", FIRST);
        assertThatThrownBy(() -> scope.asBindings().put("sneaky", SECOND))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void duplicateNamesAreRejected() {
        MessageScope.Builder builder = MessageScope.builder().add("order", FIRST);
        assertThatThrownBy(() -> builder.add("order", SECOND))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate scope entry: order");
    }

    @Test
    void anEmptyScopeCannotBeBuilt() {
        assertThatThrownBy(() -> MessageScope.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least one entry");
    }

    @Test
    void nullNamesAndMessagesAreRejected() {
        MessageScope.Builder builder = MessageScope.builder();
        assertThatThrownBy(() -> builder.add(null, FIRST))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.add("order", null))
                .isInstanceOf(NullPointerException.class);
    }
}
