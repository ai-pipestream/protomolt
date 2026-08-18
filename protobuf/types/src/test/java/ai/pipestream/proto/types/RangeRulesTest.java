package ai.pipestream.proto.types;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The range invariants ride the schema itself: order, at-least-one-bound,
 * and flag-without-bound contradictions are message-level CEL rules, and
 * the date bounds go through the Tier-1 strict calendar parser — all
 * enforced by the house validator with named violations, nothing
 * range-specific in any consumer.
 */
class RangeRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static List<String> ruleIds(Message message) {
        return VALIDATOR.validate(message).violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
    }

    @Test
    void wellFormedRangesPass() {
        assertThat(ruleIds(DateRange.newBuilder()
                .setBegin("2026-01-01").setEnd("2026-12-31").build())).isEmpty();
        assertThat(ruleIds(DateRange.newBuilder()
                .setBegin("2026-01-01").build())).isEmpty();
        assertThat(ruleIds(DateRange.newBuilder()
                .setEnd("2026-12-31").setIncludeTail(false).build())).isEmpty();
        assertThat(ruleIds(LongRange.newBuilder()
                .setBegin(5).setEnd(5).build())).isEmpty();
        assertThat(ruleIds(DoubleRange.newBuilder()
                .setBegin(-1.5).setEnd(2.5).setIncludeHead(true).build())).isEmpty();
    }

    @Test
    void emptyRangesAreRefusedAsUnbounded() {
        assertThat(ruleIds(DateRange.getDefaultInstance()))
                .containsExactly("date_range.bounded");
        assertThat(ruleIds(LongRange.getDefaultInstance()))
                .containsExactly("long_range.bounded");
        assertThat(ruleIds(DoubleRange.getDefaultInstance()))
                .containsExactly("double_range.bounded");
    }

    @Test
    void invertedBoundsAreRefusedByName() {
        assertThat(ruleIds(DateRange.newBuilder()
                .setBegin("2026-12-31").setEnd("2026-01-01").build()))
                .containsExactly("date_range.ordered");
        assertThat(ruleIds(LongRange.newBuilder()
                .setBegin(10).setEnd(9).build()))
                .containsExactly("long_range.ordered");
        assertThat(ruleIds(DoubleRange.newBuilder()
                .setBegin(2.5).setEnd(-1.5).build()))
                .containsExactly("double_range.ordered");
    }

    @Test
    void inclusivityFlagsWithoutTheirBoundsAreContradictions() {
        assertThat(ruleIds(LongRange.newBuilder()
                .setEnd(9).setIncludeHead(true).build()))
                .containsExactly("long_range.head_flag_without_bound");
        assertThat(ruleIds(DateRange.newBuilder()
                .setBegin("2026-01-01").setIncludeTail(false).build()))
                .containsExactly("date_range.tail_flag_without_bound");
    }

    @Test
    void dateBoundsGoThroughTheStrictCalendar() {
        assertThat(ruleIds(DateRange.newBuilder()
                .setBegin("2026-02-30").setEnd("2026-03-01").build()))
                .contains("string.date");
    }

    @Test
    void doubleBoundsMustBeFinite() {
        assertThat(ruleIds(DoubleRange.newBuilder()
                .setBegin(Double.NEGATIVE_INFINITY).setEnd(1.0).build()))
                .contains("double.finite");
        assertThat(ruleIds(DoubleRange.newBuilder()
                .setBegin(Double.NaN).setEnd(1.0).build()))
                .contains("double.finite");
    }
}
