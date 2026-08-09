package ai.pipestream.proto.compat;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.compile;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CompatibilityChecker#checkAgainstHistory} in the forward and full transitive
 * directions; {@link CompatibilityCheckerTest} covers the backward variants.
 */
class HistoryCheckTest {

    private static final String TWO_METHODS = """
            syntax = "proto3";
            package example;
            message Req { string q = 1; }
            service Search {
              rpc Query(Req) returns (Req);
              rpc Suggest(Req) returns (Req);
            }
            """;
    private static final String ONE_METHOD = """
            syntax = "proto3";
            package example;
            message Req { string q = 1; }
            service Search { rpc Query(Req) returns (Req); }
            """;

    private final CompatibilityChecker wire = CompatibilityChecker.create();

    @Test
    void forwardTransitiveChecksEveryVersionNotOnlyTheLatest() throws Exception {
        // The method was dropped between history[0] and history[1]; the new schema matches the
        // latest version, so only a transitive check sees the removal.
        List<FileDescriptorSet> history = List.of(compile(TWO_METHODS), compile(ONE_METHOD));
        FileDescriptorSet newSet = compile(ONE_METHOD);

        CompatibilityResult latest = wire.checkAgainstHistory(history, newSet,
                CompatibilityMode.FORWARD);
        assertThat(latest.isCompatible()).isTrue();
        assertThat(latest.changes()).isEmpty();

        CompatibilityResult transitive = wire.checkAgainstHistory(history, newSet,
                CompatibilityMode.FORWARD_TRANSITIVE);
        assertThat(transitive.isCompatible()).isFalse();
        assertThat(transitive.violations())
                .anyMatch(v -> v.ruleId().equals("METHOD_REMOVED")
                        && v.message().startsWith("history[0]: "));
    }

    @Test
    void fullTransitiveFlagsViolationsInEitherDirection() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc {
                  int32 value = 1;
                  string label = 2;
                }
                """;
        String latest = """
                syntax = "proto3";
                package example;
                message Doc { int32 value = 1; }
                """;
        // Field 2 existed in history[0] as string; reviving it as int64 breaks both directions.
        String revived = """
                syntax = "proto3";
                package example;
                message Doc {
                  int32 value = 1;
                  int64 label = 2;
                }
                """;
        List<FileDescriptorSet> history = List.of(compile(old), compile(latest));

        assertThat(wire.checkAgainstHistory(history, compile(revived),
                CompatibilityMode.FULL).isCompatible()).isTrue();

        CompatibilityResult transitive = wire.checkAgainstHistory(history, compile(revived),
                CompatibilityMode.FULL_TRANSITIVE);
        assertThat(transitive.isCompatible()).isFalse();
        assertThat(transitive.violations())
                .anyMatch(v -> v.ruleId().equals("FIELD_TYPE_CHANGED"));
    }

    @Test
    void historyUnderNoneModeReportsChangesWithoutViolations() throws Exception {
        List<FileDescriptorSet> history = List.of(compile(TWO_METHODS));
        FileDescriptorSet newSet = compile(ONE_METHOD);

        CompatibilityResult result = wire.checkAgainstHistory(history, newSet,
                CompatibilityMode.NONE);

        assertThat(result.isCompatible()).isTrue();
        assertThat(result.violations()).isEmpty();
        assertThat(result.changes())
                .anyMatch(c -> c.ruleId().equals("METHOD_REMOVED")
                        && c.message().startsWith("history[0]: "));
    }
}
