package ai.protomolt.proto.repo.service;

import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcServerLifetimeTest {
    @Test void closesServerAndBorrowedExecutor() throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var lifetime = GrpcServerLifetime.start(
                InProcessServerBuilder.forName(InProcessServerBuilder.generateName()), executor);
        executor.submit(() -> {}).get();
        lifetime.close();
        assertThat(lifetime.server().isTerminated()).isTrue();
        assertThat(executor.isTerminated()).isTrue();
        lifetime.close();
    }

    @Test void failedTransportStartClosesExecutorWithoutStoppingExistingListener() throws Exception {
        String name = InProcessServerBuilder.generateName();
        try (var existing = GrpcServerLifetime.start(InProcessServerBuilder.forName(name))) {
            var executor = Executors.newVirtualThreadPerTaskExecutor();
            assertThatThrownBy(() -> GrpcServerLifetime.start(InProcessServerBuilder.forName(name), executor))
                    .isInstanceOf(IOException.class);
            assertThat(executor.isTerminated()).isTrue();
            assertThat(existing.server().isShutdown()).isFalse();
        }
    }
}
