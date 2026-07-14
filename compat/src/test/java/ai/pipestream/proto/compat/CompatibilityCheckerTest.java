package ai.pipestream.proto.compat;

import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.compile;
import static ai.pipestream.proto.compat.TestSchemas.sourceSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Policy layer: modes, directions, rule toggles, history and result ergonomics. */
class CompatibilityCheckerTest {

    private static final String INT_FIELD = """
            syntax = "proto3";
            package example;
            message Doc { int32 value = 1; }
            """;
    private static final String STRING_FIELD = """
            syntax = "proto3";
            package example;
            message Doc { string value = 1; }
            """;
    private static final String BYTES_FIELD = """
            syntax = "proto3";
            package example;
            message Doc { bytes value = 1; }
            """;
    private static final String TWO_FIELDS = """
            syntax = "proto3";
            package example;
            message Doc {
              int32 value = 1;
              string label = 2;
            }
            """;

    private final CompatibilityChecker wire = CompatibilityChecker.create();

    @Test
    void incompatibleTypeChangeViolatesBothDirections() throws Exception {
        FileDescriptorSet oldSet = compile(INT_FIELD);
        FileDescriptorSet newSet = compile(STRING_FIELD);

        for (CompatibilityMode mode : List.of(CompatibilityMode.BACKWARD,
                CompatibilityMode.FORWARD, CompatibilityMode.FULL)) {
            CompatibilityResult result = wire.check(oldSet, newSet, mode);
            assertThat(result.isCompatible()).as("mode %s", mode).isFalse();
            assertThat(result.violations())
                    .anyMatch(v -> v.ruleId().equals("FIELD_TYPE_CHANGED"));
        }
    }

    @Test
    void bytesToStringViolatesBackwardOnly() throws Exception {
        FileDescriptorSet oldSet = compile(BYTES_FIELD);
        FileDescriptorSet newSet = compile(STRING_FIELD);

        assertThat(wire.check(oldSet, newSet, CompatibilityMode.BACKWARD).isCompatible())
                .isFalse();
        assertThat(wire.check(oldSet, newSet, CompatibilityMode.FORWARD).isCompatible())
                .isTrue();
    }

    @Test
    void stringToBytesIsCleanUnderWireRules() throws Exception {
        FileDescriptorSet oldSet = compile(STRING_FIELD);
        FileDescriptorSet newSet = compile(BYTES_FIELD);

        CompatibilityResult result = wire.check(oldSet, newSet, CompatibilityMode.FULL);
        assertThat(result.isCompatible()).isTrue();
        assertThat(result.changes()).isNotEmpty(); // the change is still reported
    }

    @Test
    void fieldRemovalIsWireCleanButViolatesBackwardWithJsonRules() throws Exception {
        FileDescriptorSet oldSet = compile(TWO_FIELDS);
        FileDescriptorSet newSet = compile(INT_FIELD);

        for (CompatibilityMode mode : List.of(CompatibilityMode.BACKWARD,
                CompatibilityMode.FORWARD, CompatibilityMode.FULL)) {
            assertThat(wire.check(oldSet, newSet, mode).isCompatible())
                    .as("wire mode %s", mode).isTrue();
        }

        CompatibilityChecker json = CompatibilityChecker.builder()
                .includeJsonRules(true)
                .build();
        assertThat(json.check(oldSet, newSet, CompatibilityMode.BACKWARD).isCompatible())
                .isFalse();
        assertThat(json.check(oldSet, newSet, CompatibilityMode.FORWARD).isCompatible())
                .isTrue();
    }

    @Test
    void compatibleWireGroupChangeIsCleanByDefault() throws Exception {
        FileDescriptorSet oldSet = compile(INT_FIELD);
        FileDescriptorSet newSet = compile("""
                syntax = "proto3";
                package example;
                message Doc { int64 value = 1; }
                """);

        assertThat(wire.check(oldSet, newSet, CompatibilityMode.FULL).isCompatible()).isTrue();
    }

    @Test
    void sourceRulesTurnSourceImpactsIntoViolationsInEveryMode() throws Exception {
        String withMethod = """
                syntax = "proto3";
                package example;
                message Req { string q = 1; }
                service Search {
                  rpc Query(Req) returns (Req);
                  rpc Suggest(Req) returns (Req);
                }
                """;
        String withoutMethod = """
                syntax = "proto3";
                package example;
                message Req { string q = 1; }
                service Search { rpc Query(Req) returns (Req); }
                """;
        FileDescriptorSet oldSet = compile(withMethod);
        FileDescriptorSet newSet = compile(withoutMethod);

        // METHOD_REMOVED carries WIRE_FORWARD + SOURCE: clean under BACKWARD by default.
        assertThat(wire.check(oldSet, newSet, CompatibilityMode.BACKWARD).isCompatible())
                .isTrue();

        CompatibilityChecker source = CompatibilityChecker.builder()
                .includeSourceRules(true)
                .build();
        assertThat(source.check(oldSet, newSet, CompatibilityMode.BACKWARD).isCompatible())
                .isFalse();
        assertThat(source.check(oldSet, newSet, CompatibilityMode.NONE).isCompatible())
                .isTrue(); // NONE never violates, even with source rules on
    }

    @Test
    void noneModeIsAlwaysCompatibleButStillReportsChanges() throws Exception {
        CompatibilityResult result = wire.check(compile(INT_FIELD), compile(STRING_FIELD),
                CompatibilityMode.NONE);

        assertThat(result.isCompatible()).isTrue();
        assertThat(result.violations()).isEmpty();
        assertThat(result.changes()).isNotEmpty();
        assertThat(result.mode()).isEqualTo(CompatibilityMode.NONE);
        assertThatCode(result::throwIfIncompatible).doesNotThrowAnyException();
    }

    @Test
    void throwIfIncompatibleEnumeratesEveryViolation() throws Exception {
        String old = """
                syntax = "proto3";
                package example;
                message Doc { int32 value = 1; }
                message Gone { string id = 1; }
                """;
        String updated = """
                syntax = "proto3";
                package example;
                message Doc { string value = 1; }
                """;
        CompatibilityResult result = wire.check(compile(old), compile(updated),
                CompatibilityMode.FULL);

        assertThatThrownBy(result::throwIfIncompatible)
                .isInstanceOf(IncompatibleSchemaException.class)
                .hasMessageContaining("FIELD_TYPE_CHANGED")
                .hasMessageContaining("example.Doc.value")
                .hasMessageContaining("MESSAGE_REMOVED")
                .hasMessageContaining("example.Gone")
                .satisfies(e -> {
                    IncompatibleSchemaException incompatible = (IncompatibleSchemaException) e;
                    assertThat(incompatible.violations()).hasSize(2);
                    assertThat(incompatible.mode()).isEqualTo(CompatibilityMode.FULL);
                });
    }

    @Test
    void historyNonTransitiveChecksOnlyLatest() throws Exception {
        List<FileDescriptorSet> history = List.of(compile(TWO_FIELDS), compile(INT_FIELD));
        // Field 2 existed in history[0] as string; the new schema revives it as int64.
        FileDescriptorSet newSet = compile("""
                syntax = "proto3";
                package example;
                message Doc {
                  int32 value = 1;
                  int64 label = 2;
                }
                """);

        CompatibilityResult latest = wire.checkAgainstHistory(history, newSet,
                CompatibilityMode.BACKWARD);
        assertThat(latest.isCompatible()).isTrue();
        assertThat(latest.changes()).allMatch(c -> c.message().startsWith("history[1]: "));

        CompatibilityResult transitive = wire.checkAgainstHistory(history, newSet,
                CompatibilityMode.BACKWARD_TRANSITIVE);
        assertThat(transitive.isCompatible()).isFalse();
        assertThat(transitive.violations())
                .anyMatch(v -> v.ruleId().equals("FIELD_TYPE_CHANGED")
                        && v.message().startsWith("history[0]: "));
    }

    @Test
    void emptyHistoryIsRejected() throws Exception {
        FileDescriptorSet newSet = compile(INT_FIELD);

        assertThatThrownBy(() -> wire.checkAgainstHistory(List.of(), newSet,
                CompatibilityMode.BACKWARD))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourceSetOverloadCompilesAndChecksEndToEnd() throws Exception {
        CompatibilityResult compatible = wire.check(sourceSet(INT_FIELD),
                sourceSet(TWO_FIELDS), CompatibilityMode.FULL);
        assertThat(compatible.isCompatible()).isTrue();
        assertThat(compatible.changes())
                .anyMatch(c -> c.ruleId().equals("FIELD_ADDED"));

        CompatibilityResult incompatible = wire.check(sourceSet(INT_FIELD),
                sourceSet(STRING_FIELD), CompatibilityMode.BACKWARD);
        assertThat(incompatible.isCompatible()).isFalse();
    }

    @Test
    void sourceSetCompileFailureIsWrapped() {
        ProtoSourceSet broken = sourceSet("this is not proto");

        assertThatThrownBy(() -> wire.check(broken, sourceSet(INT_FIELD),
                CompatibilityMode.BACKWARD))
                .isInstanceOf(CompatibilityException.class)
                .hasMessageContaining("old");
        assertThatThrownBy(() -> wire.check(sourceSet(INT_FIELD), broken,
                CompatibilityMode.BACKWARD))
                .isInstanceOf(CompatibilityException.class)
                .hasMessageContaining("new");
    }

    @Test
    void fileDescriptorOverloadChecksLinkedFiles() throws Exception {
        FileDescriptor oldFile = TestSchemas.link(INT_FIELD);
        FileDescriptor newFile = TestSchemas.link(STRING_FIELD);

        assertThat(wire.check(oldFile, newFile, CompatibilityMode.BACKWARD).isCompatible())
                .isFalse();
        assertThat(wire.check(oldFile, oldFile, CompatibilityMode.FULL).isCompatible())
                .isTrue();
    }

    @Test
    void registryLoaderOverloadChecksDescriptorsAgainstSources() throws Exception {
        List<FileDescriptor> oldFiles = List.of(TestSchemas.link(INT_FIELD));

        CompatibilityResult result = wire.check(oldFiles, sourceSet(STRING_FIELD),
                CompatibilityMode.BACKWARD);
        assertThat(result.isCompatible()).isFalse();
        assertThat(result.violations())
                .anyMatch(v -> v.ruleId().equals("FIELD_TYPE_CHANGED"));

        assertThat(wire.check(oldFiles, sourceSet(TWO_FIELDS), CompatibilityMode.BACKWARD)
                .isCompatible()).isTrue();
    }
}
