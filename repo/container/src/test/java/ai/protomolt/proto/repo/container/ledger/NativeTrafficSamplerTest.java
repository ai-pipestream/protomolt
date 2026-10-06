package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeTrafficSamplerTest {
    @Test void recordsLiveRssAndMarksExitedProcessAsMissingRatherThanZero() {
        assertThat(NativeTrafficSampler.memoryStatus(List.of("State:\tS (sleeping)", "VmRSS:\t1200 kB")))
                .isEqualTo(new NativeTrafficSampler.MemoryStatus("1200", "running"));
        assertThat(NativeTrafficSampler.memoryStatus(List.of("State:\tZ (zombie)")))
                .isEqualTo(new NativeTrafficSampler.MemoryStatus("", "exited"));
    }

    @Test void missingLiveMemoryAndMalformedStateRemainErrors() {
        assertThatThrownBy(() -> NativeTrafficSampler.memoryStatus(List.of("State:\tR (running)")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no VmRSS");
        assertThatThrownBy(() -> NativeTrafficSampler.memoryStatus(List.of("VmRSS:\t1200 kB")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no State");
    }
}
