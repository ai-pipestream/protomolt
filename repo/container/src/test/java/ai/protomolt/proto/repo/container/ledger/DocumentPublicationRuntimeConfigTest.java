package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentPublicationRuntimeConfigTest {
    @Test void retentionWindowsAreExplicitBoundedAndNeverRounded() {
        var bundle = Path.of("trusted-runtime");
        for (var invalid : List.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofNanos(1),
                Duration.ofDays(1).plusNanos(1000))) {
            assertThatThrownBy(() -> new DocumentPublicationRuntime.Assessments(bundle, invalid, Duration.ofNanos(1000)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new DocumentPublicationRuntime.Assessments(bundle, Duration.ofDays(1), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (var minimum : List.of(Duration.ofMinutes(1), Duration.ofMinutes(2))) {
            assertThatThrownBy(() -> new DocumentPublicationRuntime.Assessments(bundle, Duration.ofMinutes(1), minimum))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var shortest = new DocumentPublicationRuntime.Assessments(bundle, Duration.ofNanos(2000), Duration.ofNanos(1000));
        assertThat(shortest.retention()).isEqualTo(Duration.ofNanos(2000));
        assertThat(shortest.minimumRemaining()).isEqualTo(Duration.ofNanos(1000));
        var longest = new DocumentPublicationRuntime.Assessments(bundle, Duration.ofDays(1), Duration.ofHours(23));
        assertThat(longest.runtimeBundle()).isEqualTo(bundle);
        assertThatThrownBy(() -> new DocumentPublicationRuntime.Assessments(null, Duration.ofMinutes(1), Duration.ofSeconds(1)))
                .isInstanceOf(NullPointerException.class);
    }
}
