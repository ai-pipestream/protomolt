package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteRoutingTest {
    private static RepoServiceConfig config(String mode, String target) {
        return new RepoServiceConfig(0, new LedgerConfig("jdbc:postgresql://unused/db", "unused", "unused"),
                null, null, null, null, "base", 0, mode, target, "drive", null, 0, 0);
    }

    @Test void rejectsLocalTcpTargetsOnActualListenerPort() {
        for (String target : new String[] {"localhost:9090", "127.0.0.1:9090", "[::1]:9090", "dns:///localhost:9090"}) {
            var config = config("repo", target);
            assertThatThrownBy(() -> RemoteRouting.rejectTcp(config, 9090))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("this TCP listener");
            RemoteRouting.rejectTcp(config, 9091);
        }
    }

    @Test void rejectsMatchingInProcessNameOnly() {
        var config = config("repo-inprocess", "local");
        assertThatThrownBy(() -> RemoteRouting.rejectInProcess(config, "local"))
                .isInstanceOf(IllegalArgumentException.class);
        RemoteRouting.rejectInProcess(config, "different");
    }

    @Test void parsesOnlySupportedTcpTargetSyntax() {
        assertThat(RemoteRouting.endpoint("dns:///remote.example:9090").getHost()).isEqualTo("remote.example");
        for (String target : new String[] {"host", "host:0", "host:65536", "https://host:9090", "user@host:9090", "host:9090/path"}) {
            assertThatThrownBy(() -> RemoteRouting.endpoint(target)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
