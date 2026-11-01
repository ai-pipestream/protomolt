package ai.protomolt.proto.repo.service;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RepoCatalogMaintenanceTest {
    @Test void malformedCommandsNeverOpenTheRepository() {
        var output = new ByteArrayOutputStream();
        for (var args : List.of(new String[0], new String[] {"serve"},
                new String[] {"rebuild-entry", "a", "g", "ds", "d", "0"},
                new String[] {"rebuild-entry", "a", "g", "ds", "d", "latest"},
                new String[] {"verify-core", "a", "", "ds", "d", "1"},
                new String[] {"verify-core", "a", "g", "ds", "d", "9223372036854775808"})) {
            assertThatThrownBy(() -> RepoCatalogMaintenance.run(args, () -> {
                throw new AssertionError("Invalid command opened the repository");
            }, new PrintStream(output))).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(output.size()).isZero();
    }

    @Test void repositoryFailureIsPropagatedWithoutSuccessOutput() {
        var output = new ByteArrayOutputStream();
        var failure = new IllegalStateException("catalog unavailable");
        assertThatThrownBy(() -> RepoCatalogMaintenance.run(
                new String[] {"verify-core", "a", "g", "ds", "d", "1"},
                () -> { throw failure; }, new PrintStream(output))).isSameAs(failure);
        assertThat(output.size()).isZero();
    }
}
