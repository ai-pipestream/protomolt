package ai.protomolt.proto.repo.service;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OwnedResourcesTest {
    @Test void closesInReverseOrderDespiteFailuresAndOnlyOnce() {
        var closed = new ArrayList<Integer>();
        var first = new IllegalStateException("latest resource");
        var second = new IllegalArgumentException("earlier resource");
        var resources = new OwnedResources();
        resources.add(() -> { closed.add(1); throw second; });
        resources.add(() -> { closed.add(2); throw first; });
        assertThatThrownBy(resources::close).isSameAs(first).hasSuppressedException(second);
        assertThat(closed).containsExactly(2, 1);
        resources.close();
        assertThat(closed).containsExactly(2, 1);
        assertThatThrownBy(() -> resources.add(() -> {})).isInstanceOf(IllegalStateException.class);
    }

    @Test void interruptionRemainsObservableAndDoesNotSkipOtherCleanup() {
        var closed = new ArrayList<Integer>();
        var resources = new OwnedResources();
        resources.add(() -> closed.add(1));
        resources.add(() -> { throw new InterruptedException("interrupted cleanup"); });
        try {
            assertThatThrownBy(resources::close).isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(closed).containsExactly(1);
        } finally { Thread.interrupted(); }
    }
}
