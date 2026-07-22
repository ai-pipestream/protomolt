package ai.pipestream.proto.compat;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static ai.pipestream.proto.compat.TestSchemas.all;
import static ai.pipestream.proto.compat.TestSchemas.compile;
import static ai.pipestream.proto.compat.TestSchemas.single;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Reserved number/name rules. Wire's encoder drops {@code reserved} declarations from compiled
 * descriptors, so these tests patch them in via {@link TestSchemas}.
 */
class ReservedRuleTest {

    /** Exclusive end of {@code reserved n to max}: protobuf's largest field number plus one. */
    private static final int MAX_END = 536870912;

    private static final String BASE = """
            syntax = "proto3";
            package example;
            message Person { string name = 1; }
            """;
    private static final String WITH_FIELD_FIVE = """
            syntax = "proto3";
            package example;
            message Person {
              string name = 1;
              string legacy = 5;
            }
            """;

    @Test
    void reservedNumberReuseBreaksWireAndJson() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveNumbers(compile(BASE), "Person", 5);

        SchemaChange change = single(SchemaDiff.diff(oldSet, compile(WITH_FIELD_FIVE)),
                "RESERVED_NUMBER_REUSED");
        assertThat(change.path()).isEqualTo("example.Person.legacy");
        assertThat(change.impacts()).containsExactlyInAnyOrder(Impact.WIRE_BACKWARD,
                Impact.WIRE_FORWARD, Impact.JSON_BACKWARD, Impact.JSON_FORWARD);
    }

    @Test
    void reservedNameReuseBreaksJson() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveNames(compile(BASE), "Person", "legacy");

        SchemaChange change = single(SchemaDiff.diff(oldSet, compile(WITH_FIELD_FIVE)),
                "RESERVED_NAME_REUSED");
        assertThat(change.path()).isEqualTo("example.Person.legacy");
        assertThat(change.impacts())
                .containsExactlyInAnyOrder(Impact.JSON_BACKWARD, Impact.JSON_FORWARD);
    }

    @Test
    void newFieldOnUnreservedNumberIsOnlyAnAddition() throws Exception {
        List<SchemaChange> changes = SchemaDiff.diff(compile(BASE), compile(WITH_FIELD_FIVE));

        assertThat(single(changes, "FIELD_ADDED").isInformational()).isTrue();
        assertThat(all(changes, "RESERVED_NUMBER_REUSED")).isEmpty();
        assertThat(all(changes, "RESERVED_NAME_REUSED")).isEmpty();
    }

    @Test
    void reservedRangeRemovalIsAdvisory() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveNumbers(compile(BASE), "Person", 5);

        SchemaChange change = single(SchemaDiff.diff(oldSet, compile(BASE)),
                "RESERVED_RANGE_REMOVED");
        assertThat(change.path()).isEqualTo("example.Person");
        assertThat(change.before()).isEqualTo("reserved 5");
        assertThat(change.isInformational()).isTrue();
    }

    @Test
    void keptReservedRangeProducesNoChanges() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveNumbers(compile(BASE), "Person", 5);
        FileDescriptorSet newSet = TestSchemas.reserveNumbers(compile(BASE), "Person", 5);

        assertThat(SchemaDiff.diff(oldSet, newSet)).isEmpty();
    }

    /**
     * {@code reserved 5 to max} spans over half a billion numbers. Coverage is decided by
     * comparing intervals; a diff that walked the range one number at a time would not finish.
     */
    @Test
    void keptOpenEndedReservedRangeProducesNoChanges() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveRange(compile(BASE), "Person", 5, MAX_END);
        FileDescriptorSet newSet = TestSchemas.reserveRange(compile(BASE), "Person", 5, MAX_END);

        // Comparing the two ranges is a few operations; enumerating them took seconds.
        assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertThat(SchemaDiff.diff(oldSet, newSet)).isEmpty());
    }

    @Test
    void narrowedOpenEndedReservedRangeIsReported() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveRange(compile(BASE), "Person", 5, MAX_END);
        FileDescriptorSet newSet = TestSchemas.reserveRange(compile(BASE), "Person", 5, 10);

        SchemaChange change = single(SchemaDiff.diff(oldSet, newSet),
                "RESERVED_RANGE_REMOVED");
        assertThat(change.before()).isEqualTo("reserved 5 to " + (MAX_END - 1));
    }

    @Test
    void reservedRangeSplitAcrossTwoRangesIsStillCovered() throws Exception {
        FileDescriptorSet oldSet = TestSchemas.reserveRange(compile(BASE), "Person", 5, 20);
        FileDescriptorSet newSet = TestSchemas.reserveRange(
                TestSchemas.reserveRange(compile(BASE), "Person", 12, 20), "Person", 5, 12);

        assertThat(all(SchemaDiff.diff(oldSet, newSet), "RESERVED_RANGE_REMOVED")).isEmpty();
    }
}
