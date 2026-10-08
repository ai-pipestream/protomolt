package ai.protomolt.proto.repo.container.ledger;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RepositoryScalingPlanTest {
    @Test void plansMirrorEveryTopologyAnEvenNumberOfTimes() {
        for (String name : new String[] {"mirrored", "scaleout"}) {
            String[] plan = NativeReplicaRuntimeTest.plan(name);
            for (int i = 0; i < plan.length; i++) assertThat(plan[i]).as(name).isEqualTo(plan[plan.length - 1 - i]);
            assertThat(Arrays.stream(plan).distinct().count() * 2).isEqualTo(plan.length);
        }
        assertThat(NativeReplicaRuntimeTest.plan("scaleout")).noneMatch(config -> config.endsWith("1"));
        assertThatThrownBy(() -> NativeReplicaRuntimeTest.plan("linear")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aggregateHeapDividesEquallyAndZeroKeepsTheOriginalWorkerHeap() {
        assertThat(NativeReplicaRuntimeTest.heapPerReplica(0, 4)).isEqualTo(512);
        assertThat(NativeReplicaRuntimeTest.heapPerReplica(2048, 1)).isEqualTo(2048);
        assertThat(NativeReplicaRuntimeTest.heapPerReplica(2048, 4)).isEqualTo(512);
        assertThatThrownBy(() -> NativeReplicaRuntimeTest.heapPerReplica(256, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeReplicaRuntimeTest.heapPerReplica(500, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeReplicaRuntimeTest.heapPerReplica(512, 8)).isInstanceOf(IllegalArgumentException.class);
    }
}
