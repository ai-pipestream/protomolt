package ai.pipestream.proto.composer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ComposerTest {

    /** A module that records lifecycle events into a shared journal. */
    private static final class RecordingModule implements ServiceModule {

        private final String role;
        private final Set<String> requires;
        private final List<String> journal;
        private final boolean failOnStart;

        RecordingModule(String role, Set<String> requires, List<String> journal) {
            this(role, requires, journal, false);
        }

        RecordingModule(String role, Set<String> requires, List<String> journal, boolean failOnStart) {
            this.role = role;
            this.requires = requires;
            this.journal = journal;
            this.failOnStart = failOnStart;
        }

        @Override
        public String role() {
            return role;
        }

        @Override
        public Set<String> requires() {
            return requires;
        }

        @Override
        public ServiceMount wire(NodeContext context) {
            journal.add("wire:" + role);
            return new ServiceMount() {
                @Override
                public void start() {
                    if (failOnStart) {
                        throw new IllegalStateException(role + " refuses to start");
                    }
                    journal.add("start:" + role);
                }

                @Override
                public void close() {
                    journal.add("close:" + role);
                }
            };
        }
    }

    @Test
    void wiresInDependencyOrderStartsInOrderClosesInReverse() {
        List<String> journal = new ArrayList<>();
        Composer composer = Composer.emptyBuilder()
                .module(new RecordingModule("intake", Set.of("repo"), journal))
                .module(new RecordingModule("repo", Set.of(), journal))
                .module(new RecordingModule("jobs", Set.of("registry"), journal))
                .module(new RecordingModule("registry", Set.of(), journal))
                .environment(Map.of())
                .build();

        try (Composer.Node node = composer.boot(List.of("intake", "jobs", "repo", "registry"))) {
            assertThat(node.context().nodeId()).hasSize(8);
        }

        assertThat(journal).containsExactly(
                "wire:repo", "wire:intake", "wire:registry", "wire:jobs",
                "start:repo", "start:intake", "start:registry", "start:jobs",
                "close:jobs", "close:registry", "close:intake", "close:repo");
    }

    @Test
    void unknownRoleFailsLoudlyNamingKnownRoles() {
        Composer composer = Composer.emptyBuilder()
                .module(new RecordingModule("repo", Set.of(), new ArrayList<>()))
                .environment(Map.of())
                .build();

        assertThatThrownBy(() -> composer.boot(List.of("repo", "acounts")))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("unknown role acounts")
                .hasMessageContaining("repo");
    }

    @Test
    void requirementCycleIsDetected() {
        List<String> journal = new ArrayList<>();
        Composer composer = Composer.emptyBuilder()
                .module(new RecordingModule("a", Set.of("b"), journal))
                .module(new RecordingModule("b", Set.of("a"), journal))
                .environment(Map.of())
                .build();

        assertThatThrownBy(() -> composer.boot(List.of("a", "b")))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void requirementOutsideSelectionDoesNotForceMounting() {
        List<String> journal = new ArrayList<>();
        Composer composer = Composer.emptyBuilder()
                .module(new RecordingModule("intake", Set.of("repo"), journal))
                .environment(Map.of())
                .build();

        try (Composer.Node node = composer.boot(List.of("intake"))) {
            assertThat(node.context().channels().isLocal("repo")).isFalse();
        }
        assertThat(journal).containsExactly("wire:intake", "start:intake", "close:intake");
    }

    @Test
    void startFailureClosesEverythingAlreadyCreatedInReverse() {
        List<String> journal = new ArrayList<>();
        Composer composer = Composer.emptyBuilder()
                .module(new RecordingModule("repo", Set.of(), journal))
                .module(new RecordingModule("jobs", Set.of(), journal, true))
                .environment(Map.of())
                .build();

        assertThatThrownBy(() -> composer.boot(List.of("repo", "jobs")))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("refuses to start");

        assertThat(journal).containsExactly(
                "wire:repo", "wire:jobs", "start:repo", "close:jobs", "close:repo");
    }

    @Test
    void duplicateRoleIsAConfigurationError() {
        List<String> journal = new ArrayList<>();
        Composer.Builder builder = Composer.emptyBuilder()
                .module(new RecordingModule("repo", Set.of(), journal));

        assertThatThrownBy(() -> builder.module(new RecordingModule("repo", Set.of(), journal)))
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining("two modules claim role repo");
    }

    @Test
    void contributionsFlowFromWireToStartAndFreezeAfterWiring() {
        List<String> seenAtStart = new ArrayList<>();
        NodeContext[] leaked = new NodeContext[1];
        ServiceModule contributor = new ServiceModule() {
            @Override
            public String role() {
                return "jobs";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                context.contributions().contribute(String.class, "submit-workflow");
                context.contributions().contribute(String.class, "get-job");
                return ServiceMount.inert(() -> {
                });
            }
        };
        ServiceModule host = new ServiceModule() {
            @Override
            public String role() {
                return "registry";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                leaked[0] = context;
                return new ServiceMount() {
                    @Override
                    public void start() {
                        seenAtStart.addAll(context.contributions().all(String.class));
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
        Composer composer = Composer.emptyBuilder()
                .module(host)
                .module(contributor)
                .environment(Map.of())
                .build();

        try (Composer.Node node = composer.boot(List.of("registry", "jobs"))) {
            assertThat(seenAtStart).containsExactly("submit-workflow", "get-job");
            assertThatThrownBy(() -> leaked[0].contributions().contribute(String.class, "late"))
                    .isInstanceOf(ComposerException.class)
                    .hasMessageContaining("after the wire phase");
        }
    }

    @Test
    void bootFromEnvironmentParsesRolesAndRequiresTheVariable() {
        List<String> journal = new ArrayList<>();
        Composer composer = Composer.emptyBuilder()
                .module(new RecordingModule("repo", Set.of(), journal))
                .module(new RecordingModule("intake", Set.of("repo"), journal))
                .environment(Map.of(Composer.ENV_ROLES, "Intake, repo"))
                .build();

        try (Composer.Node node = composer.bootFromEnvironment()) {
            assertThat(journal).startsWith("wire:repo", "wire:intake");
        }

        Composer bare = Composer.emptyBuilder().environment(Map.of()).build();
        assertThatThrownBy(bare::bootFromEnvironment)
                .isInstanceOf(ComposerException.class)
                .hasMessageContaining(Composer.ENV_ROLES);
    }

    @Test
    void onCloseResourcesUnwindWithTheMounts() {
        List<String> journal = new ArrayList<>();
        ServiceModule module = new ServiceModule() {
            @Override
            public String role() {
                return "repo";
            }

            @Override
            public ServiceMount wire(NodeContext context) {
                context.onClose(() -> journal.add("close:extra"));
                journal.add("wire:repo");
                return new ServiceMount() {
                    @Override
                    public void start() {
                        journal.add("start:repo");
                    }

                    @Override
                    public void close() {
                        journal.add("close:repo");
                    }
                };
            }
        };
        try (Composer.Node node = Composer.emptyBuilder()
                .module(module)
                .environment(Map.of())
                .build()
                .boot(List.of("repo"))) {
            assertThat(node).isNotNull();
        }
        assertThat(journal).containsExactly("wire:repo", "start:repo", "close:repo", "close:extra");
    }
}
