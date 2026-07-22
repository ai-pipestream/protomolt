package ai.pipestream.proto.rest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Route identity: {@code service/method} pairs register exactly once. Silent replacement
 * would make the surviving implementation depend on registration order — the classic
 * plugin-composition hazard.
 */
class ProtoRestMethodRegistryTest {

    @Test
    void duplicateRoutesAreRejectedNotReplaced() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("Orders", "Get", r -> r).build());

        assertThatThrownBy(() -> registry.register(
                ProtoRestMethod.builder("Orders", "Get", r -> r).build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Orders/Get")
                .hasMessageContaining("already registered");

        // Different method or service on the same registry is fine.
        registry.register(ProtoRestMethod.builder("Orders", "List", r -> r).build());
        registry.register(ProtoRestMethod.builder("Billing", "Get", r -> r).build());
        assertThat(registry.all()).hasSize(3);
    }
}
