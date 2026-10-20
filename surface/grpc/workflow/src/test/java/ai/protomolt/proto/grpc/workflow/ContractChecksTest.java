package ai.protomolt.proto.grpc.workflow;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Timestamp presence, well-formedness and ordering as every workflow record checks them. */
class ContractChecksTest {

    private static final Timestamp UNSET = Timestamp.getDefaultInstance();

    @Test
    void anUnsetTimestampIsAbsentAndRefusedOnlyWhenRequired() {
        assertThatCode(() -> ContractChecks.timestamp(UNSET, "run.completed_at", false))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> ContractChecks.timestamp(UNSET, "run.started_at", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("run.started_at must be present");
    }

    @Test
    void nanosOnlyCountsAsPresentAndOutOfRangeNanosAreRefused() {
        assertThatCode(() -> ContractChecks.timestamp(at(0, 1), "t", true))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> ContractChecks.timestamp(at(5, 1_000_000_000), "t", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("t must be a valid timestamp");
        assertThatThrownBy(() -> ContractChecks.timestamp(at(5, -1), "t", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("t must be a valid timestamp");
    }

    @Test
    void anOpenEndOrAnEqualEndIsInOrderAndAnEarlierEndIsNot() {
        assertThatCode(() -> ContractChecks.order(at(10, 0), UNSET, "run"))
                .doesNotThrowAnyException();
        assertThatCode(() -> ContractChecks.order(at(10, 5), at(10, 5), "run"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> ContractChecks.order(at(10, 5), at(10, 4), "step_evidence"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("step_evidence.completed_at must not precede started_at");
    }

    private static Timestamp at(long seconds, int nanos) {
        return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    }
}
