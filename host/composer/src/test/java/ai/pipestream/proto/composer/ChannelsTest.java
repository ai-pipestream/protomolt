package ai.pipestream.proto.composer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ChannelsTest {

    /** A module that publishes an in-process endpoint for its role. */
    private static final class PublishingModule implements ServiceModule {

        private final String role;

        PublishingModule(String role) {
            this.role = role;
        }

        @Override
        public String role() {
            return role;
        }

        @Override
        public ServiceMount wire(NodeContext context) throws IOException {
            String name = role + "-" + context.nodeId();
            Server server = InProcessServerBuilder.forName(name).build().start();
            context.channels().publishInProcess(role, name);
            context.onClose(server::shutdownNow);
            return ServiceMount.inert(() -> {
            });
        }
    }

    /** A module that resolves a channel to another role at wire time. */
    private static final class CallingModule implements ServiceModule {

        private final AtomicReference<ManagedChannel> seen = new AtomicReference<>();

        @Override
        public String role() {
            return "intake";
        }

        @Override
        public Set<String> requires() {
            return Set.of("repo");
        }

        @Override
        public ServiceMount wire(NodeContext context) {
            seen.set(context.channels().to("repo"));
            return ServiceMount.inert(() -> {
            });
        }
    }

    @Test
    void coMountedRoleResolvesInProcessAndChannelIsCached() {
        CallingModule caller = new CallingModule();
        Composer composer = Composer.emptyBuilder()
                .module(new PublishingModule("repo"))
                .module(caller)
                .environment(Map.of())
                .build();

        try (Composer.Node node = composer.boot(List.of("repo", "intake"))) {
            Channels channels = node.context().channels();
            assertThat(channels.isLocal("repo")).isTrue();
            assertThat(caller.seen.get()).isNotNull();
            assertThat(channels.to("repo")).isSameAs(caller.seen.get());
        }
    }

    @Test
    void nodeCloseShutsDownChannelsItHandedOut() {
        CallingModule caller = new CallingModule();
        Composer composer = Composer.emptyBuilder()
                .module(new PublishingModule("repo"))
                .module(caller)
                .environment(Map.of())
                .build();

        try (Composer.Node node = composer.boot(List.of("repo", "intake"))) {
            assertThat(caller.seen.get()).isNotNull();
        }
        assertThat(caller.seen.get().isShutdown()).isTrue();
    }

    @Test
    void aWireFailureClosesChannelsOpenedDuringWiring() {
        AtomicReference<ManagedChannel> opened = new AtomicReference<>();
        ServiceModule caller = new ServiceModule() {
            @Override
            public String role() {
                return "intake";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                opened.set(context.channels().to("repo"));
                return ServiceMount.inert(() -> {
                });
            }
        };
        ServiceModule broken = new ServiceModule() {
            @Override
            public String role() {
                return "jobs";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                throw new IllegalStateException("jobs refuses to wire");
            }
        };
        Composer composer = Composer.emptyBuilder()
                .module(caller)
                .module(broken)
                .environment(Map.of("PROTOMOLT_REPO_TARGET", "repo.example.internal:9090"))
                .remoteOpener(target -> InProcessChannelBuilder
                        .forName("boot-failure-probe-" + System.nanoTime())
                        .build())
                .build();

        assertThatThrownBy(() -> composer.boot(List.of("intake", "jobs")))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("refuses to wire");
        assertThat(opened.get()).isNotNull();
        assertThat(opened.get().isShutdown()).isTrue();
    }

    @Test
    void aStartFailureClosesChannelsOpenedDuringWiring() {
        AtomicReference<ManagedChannel> opened = new AtomicReference<>();
        ServiceModule caller = new ServiceModule() {
            @Override
            public String role() {
                return "intake";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                opened.set(context.channels().to("repo"));
                return ServiceMount.inert(() -> {
                });
            }
        };
        ServiceModule broken = new ServiceModule() {
            @Override
            public String role() {
                return "jobs";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                return new ServiceMount() {
                    @Override
                    public void start() {
                        throw new IllegalStateException("jobs refuses to start");
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
        Composer composer = Composer.emptyBuilder()
                .module(caller)
                .module(broken)
                .environment(Map.of("PROTOMOLT_REPO_TARGET", "repo.example.internal:9090"))
                .remoteOpener(target -> InProcessChannelBuilder
                        .forName("boot-failure-probe-" + System.nanoTime())
                        .build())
                .build();

        assertThatThrownBy(() -> composer.boot(List.of("intake", "jobs")))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("refuses to start");
        assertThat(opened.get()).isNotNull();
        assertThat(opened.get().isShutdown()).isTrue();
    }

    @Test
    void targetVariableUppercasesAndUnderscoresTheRoleName() {
        assertThat(Channels.targetVariable("repo")).isEqualTo("PROTOMOLT_REPO_TARGET");
        assertThat(Channels.targetVariable("parse-text")).isEqualTo("PROTOMOLT_PARSE_TEXT_TARGET");
    }

    @Test
    void aliasedRolesReadTheirDeprecatedTargetVariableSecond() {
        assertThat(Channels.targetVariables("repo"))
                .containsExactly("PROTOMOLT_REPO_TARGET");
        assertThat(Channels.targetVariables("metric"))
                .containsExactly("PROTOMOLT_METRIC_TARGET", "PROTOMOLT_METRICS_TARGET");
        assertThat(Channels.targetVariables("parse-text"))
                .containsExactly("PROTOMOLT_PARSE_TEXT_TARGET", "PROTOMOLT_PARSER_TEXT_TARGET");
    }

    @Test
    void theAliasTargetVariableResolvesAndTheCanonicalOneWins() {
        Composer aliasOnly = Composer.emptyBuilder()
                .module(new PublishingModule("repo"))
                .environment(Map.of("PROTOMOLT_METRICS_TARGET", "metric.example.internal:9095"))
                .build();

        try (Composer.Node node = aliasOnly.boot(List.of("repo"))) {
            assertThat(node.context().channels().targetOf("metric"))
                    .isEqualTo("metric.example.internal:9095");
        }

        Composer both = Composer.emptyBuilder()
                .module(new PublishingModule("repo"))
                .environment(Map.of(
                        "PROTOMOLT_METRIC_TARGET", "canonical.example.internal:9095",
                        "PROTOMOLT_METRICS_TARGET", "metric.example.internal:9095"))
                .build();

        try (Composer.Node node = both.boot(List.of("repo"))) {
            assertThat(node.context().channels().targetOf("metric"))
                    .isEqualTo("canonical.example.internal:9095");
        }
    }

    @Test
    void unmountedRoleWithoutTargetFailsNamingTheVariable() {
        Composer composer = Composer.emptyBuilder()
                .module(new PublishingModule("repo"))
                .environment(Map.of())
                .build();

        try (Composer.Node node = composer.boot(List.of("repo"))) {
            assertThatThrownBy(() -> node.context().channels().to("inference"))
                    .isInstanceOf(ComposerException.class)
                    .hasMessageContaining("PROTOMOLT_INFERENCE_TARGET");
        }
    }

    @Test
    void remoteTargetGoesThroughThePolicyAndTheInjectedOpener() throws IOException {
        String backingName = "remote-repo-" + System.nanoTime();
        Server backing = InProcessServerBuilder.forName(backingName).build().start();
        try {
            Composer composer = Composer.emptyBuilder()
                    .module(new PublishingModule("registry"))
                    .environment(Map.of("PROTOMOLT_REPO_TARGET", "repo.example.internal:9090"))
                    .remoteOpener(target -> InProcessChannelBuilder.forName(backingName).build())
                    .build();

            try (Composer.Node node = composer.boot(List.of("registry"))) {
                ManagedChannel channel = node.context().channels().to("repo");
                assertThat(channel).isNotNull();
                assertThat(node.context().channels().isLocal("repo")).isFalse();
            }
        } finally {
            backing.shutdownNow();
        }
    }

    @Test
    void remoteTargetWithoutTransportFailsLoudly() {
        Composer composer = Composer.emptyBuilder()
                .module(new PublishingModule("registry"))
                .environment(Map.of("PROTOMOLT_REPO_TARGET", "repo.example.internal:9090"))
                .build();

        try (Composer.Node node = composer.boot(List.of("registry"))) {
            assertThatThrownBy(() -> node.context().channels().to("repo"))
                    .isInstanceOf(ComposerException.class)
                    .hasMessageContaining("no remote transport");
        }
    }

    @Test
    void targetStringsPivotBetweenInProcessAndRemote() {
        Composer composer = Composer.emptyBuilder()
                .module(new PublishingModule("repo"))
                .environment(Map.of("PROTOMOLT_PARSE_TARGET", "parse.example.internal:9093"))
                .build();

        try (Composer.Node node = composer.boot(List.of("repo"))) {
            Channels channels = node.context().channels();
            assertThat(channels.targetOf("repo"))
                    .startsWith(Channels.IN_PROCESS_PREFIX + "repo-");
            assertThat(channels.targetOf("parse")).isEqualTo("parse.example.internal:9093");
            assertThatThrownBy(() -> channels.targetOf("inference"))
                    .isInstanceOf(ComposerException.class)
                    .hasMessageContaining("PROTOMOLT_INFERENCE_TARGET");
        }
    }

    @Test
    void duplicateEndpointPublicationIsRejected() {
        ServiceModule doublePublisher = new ServiceModule() {
            @Override
            public String role() {
                return "repo";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                context.channels().publishInProcess("repo", "first");
                context.channels().publishInProcess("repo", "second");
                return ServiceMount.inert(() -> {
                });
            }
        };
        Composer composer = Composer.emptyBuilder()
                .module(doublePublisher)
                .environment(Map.of())
                .build();

        assertThatThrownBy(() -> composer.boot(List.of("repo")))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("already published");
    }
}
