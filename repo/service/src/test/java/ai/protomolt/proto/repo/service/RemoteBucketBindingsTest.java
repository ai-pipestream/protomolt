package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteBucketBindingsTest {
    @Test void parsesOnlyExplicitOneToOneStringBindings() {
        assertThat(RemoteBucketBindings.parse("{\"local\":\"remote\"}"))
                .containsExactlyEntriesOf(java.util.Map.of("local", "remote"));
        for (String bad : new String[] {"null", "[]", "{\"a\":1}", "{\"a\":\"x\",\"a\":\"y\"}",
                "{\"a\":\"x\",\"b\":\"x\"}", "{\"a\":\"\"}", "{} {}"}) {
            assertThatThrownBy(() -> RemoteBucketBindings.parse(bad)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
