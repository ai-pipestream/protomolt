package ai.protomolt.proto.intake.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.composer.Channels;
import ai.protomolt.proto.composer.Contributions;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.protomolt.proto.intake.service.identity.IntakeScope;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Unit tests verifying that {@link IntakeModule} closes an owned
 * {@link AutoCloseable} {@link ApiKeyIdentityResolver} without swallowing
 * cleanup exceptions or masking primary wiring exceptions.
 */
class IntakeModuleCloseTest {

    @Test
    void mountClosePropagatesOwnedResolverCloseException() throws Exception {
        IOException closeFailure = new IOException("resolver pool drain failed");
        AtomicBoolean closed = new AtomicBoolean(false);
        CloseableResolver resolver = new CloseableResolver(() -> {
            closed.set(true);
            throw closeFailure;
        });
        ManagedChannel repoChannel = InProcessChannelBuilder.forName("test-repo-mount-close")
                .directExecutor()
                .build();
        try {
            IntakeModule module = new IntakeModule(
                    new IntakeModule.Config(0, -1, 1024L, null),
                    env -> resolver);
            ServiceMount mount = module.wire(testContext(repoChannel, false));

            assertThatThrownBy(mount::close)
                    .isSameAs(closeFailure);
            assertThat(closed).isTrue();
        } finally {
            repoChannel.shutdownNow();
        }
    }

    @Test
    void wireFailureAttachesOwnedResolverCloseFailureAsSuppressed() {
        RuntimeException publishFailure = new IllegalStateException("publish in-process failed");
        IOException closeFailure = new IOException("resolver close failed");
        CloseableResolver resolver = new CloseableResolver(() -> {
            throw closeFailure;
        });
        ManagedChannel repoChannel = InProcessChannelBuilder.forName("test-repo-wire-fail")
                .directExecutor()
                .build();
        try {
            IntakeModule module = new IntakeModule(
                    new IntakeModule.Config(0, -1, 1024L, null),
                    env -> resolver);

            assertThatThrownBy(() -> module.wire(testContext(repoChannel, true, publishFailure)))
                    .isSameAs(publishFailure)
                    .satisfies(t -> assertThat(t.getSuppressed()).containsExactly(closeFailure));
        } finally {
            repoChannel.shutdownNow();
        }
    }

    private static NodeContext testContext(ManagedChannel repoChannel, boolean failPublish) {
        return testContext(repoChannel, failPublish, new IllegalStateException("publish failed"));
    }

    private static NodeContext testContext(
            ManagedChannel repoChannel, boolean failPublish, RuntimeException publishFailure) {
        Channels channels = new Channels() {
            @Override
            public void publishInProcess(String role, String inProcessName) {
                if (failPublish) {
                    throw publishFailure;
                }
            }

            @Override
            public String targetOf(String role) {
                return "inprocess:test-repo";
            }

            @Override
            public ManagedChannel to(String role) {
                return repoChannel;
            }

            @Override
            public boolean isLocal(String role) {
                return true;
            }
        };
        Contributions contributions = new Contributions() {
            @Override
            public <T> void contribute(Class<T> type, T instance) {
            }

            @Override
            public <T> List<T> all(Class<T> type) {
                return List.of();
            }

            @Override
            public <T> T shared(Class<T> type, Supplier<T> factory) {
                return factory.get();
            }
        };
        return new NodeContext() {
            @Override
            public Map<String, String> environment() {
                return Map.of();
            }

            @Override
            public String nodeId() {
                return "test-node";
            }

            @Override
            public Channels channels() {
                return channels;
            }

            @Override
            public Contributions contributions() {
                return contributions;
            }

            @Override
            public void onClose(AutoCloseable resource) {
            }
        };
    }

    private static final class CloseableResolver implements ApiKeyIdentityResolver, AutoCloseable {
        private final AutoCloseable onClose;

        private CloseableResolver(AutoCloseable onClose) {
            this.onClose = onClose;
        }

        @Override
        public Optional<IntakeScope> resolve(String credential) {
            return Optional.empty();
        }

        @Override
        public void close() throws Exception {
            onClose.close();
        }
    }
}
