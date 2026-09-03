package ai.protomolt.proto.acquire.gather;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GatherExceptionTest {

    @Test
    void messageOnlyConstructor() {
        GatherException exception = new GatherException("gather failed");

        assertThat(exception).hasMessage("gather failed").hasNoCause();
    }

    @Test
    void messageAndCauseConstructor() {
        RuntimeException cause = new RuntimeException("root cause");

        GatherException exception = new GatherException("gather failed", cause);

        assertThat(exception).hasMessage("gather failed").hasCause(cause);
    }
}
