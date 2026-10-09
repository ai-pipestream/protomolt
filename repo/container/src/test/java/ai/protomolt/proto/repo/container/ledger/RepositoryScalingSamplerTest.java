package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RepositoryScalingSamplerTest {
    @Test void hostCpuSeparatesBusyIdleAndIowaitJiffies() {
        var cpu = RepositoryScalingSampler.HostCpu.parse("cpu  100 2 30 1000 40 0 5 3 0 0");
        assertThat(cpu.total()).isEqualTo(1180);
        assertThat(cpu.idle()).isEqualTo(1000);
        assertThat(cpu.iowait()).isEqualTo(40);
        assertThat(cpu.busy()).isEqualTo(140);
    }

    @Test void hostCpuRefusesForeignOrTruncatedLines() {
        assertThatThrownBy(() -> RepositoryScalingSampler.HostCpu.parse("cpu0 1 2 3 4 5 6 7 8"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("/proc/stat");
        assertThatThrownBy(() -> RepositoryScalingSampler.HostCpu.parse("cpu 1 2 3"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test void cgroupCpuRequiresAllThreeUsageCounters() {
        var cpu = RepositoryScalingSampler.CgroupCpu.parse(List.of("usage_usec 500", "user_usec 300", "system_usec 200", "nr_periods 0"));
        assertThat(cpu).isEqualTo(new RepositoryScalingSampler.CgroupCpu(500, 300, 200));
        assertThatThrownBy(() -> RepositoryScalingSampler.CgroupCpu.parse(List.of("usage_usec 500", "user_usec 300")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("system_usec");
    }
}
