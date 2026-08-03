package ai.pipestream.proto.jobs.service;

import ai.pipestream.proto.jobs.v1.ChainJobEvent;
import ai.pipestream.proto.jobs.v1.ChainJobRequest;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the jobs contract's own validate.v1 annotations compile and evaluate — the
 * dogfood gate. The protomolt serde enforces these rules on write, so a regression here
 * means a malformed event could reach the topic (or a good one be rejected).
 */
class ChainJobContractValidationTest {

    private static ChainJobEvent.Builder validEvent() {
        return ChainJobEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setJobId(UUID.randomUUID().toString())
                .setChainName("court-decoration")
                .setType(ChainJobEvent.Type.TYPE_COMPLETED)
                .setAttempt(1)
                .setVerdict("3 steps, output court.v1.OpinionMetadata")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build());
    }

    @Test
    void aWellFormedEventValidates() {
        ValidationResult result = ProtoValidator
                .forMessageType(ChainJobEvent.getDescriptor())
                .validate(validEvent().build());
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aCompletedEventWithoutAVerdictFailsTheMessageCel() {
        ValidationResult result = ProtoValidator
                .forMessageType(ChainJobEvent.getDescriptor())
                .validate(validEvent().clearVerdict().build());
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("completed-carries-verdict"));
    }

    @Test
    void aFailedEventWithoutAnErrorFailsTheMessageCel() {
        ChainJobEvent event = validEvent()
                .setType(ChainJobEvent.Type.TYPE_DEAD)
                .clearVerdict()
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(ChainJobEvent.getDescriptor())
                .validate(event);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("terminal-carries-error"));
    }

    @Test
    void fieldRulesBiteOnTheEventIds() {
        ChainJobEvent event = validEvent()
                .setEventId("not-a-uuid")
                .setType(ChainJobEvent.Type.TYPE_UNSPECIFIED)
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(ChainJobEvent.getDescriptor())
                .validate(event);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.path()).contains("event_id"));
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.path()).contains("type"));
    }

    @Test
    void aWellFormedRequestValidates() {
        ChainJobRequest request = ChainJobRequest.newBuilder()
                .setJobId(UUID.randomUUID().toString())
                .setChainName("court-decoration")
                .setInput(Struct.newBuilder().build())
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(ChainJobRequest.getDescriptor())
                .validate(request);
        assertThat(result.valid()).as("violations: %s", result.violations()).isTrue();
    }

    @Test
    void aRequestWithoutAChainNameFails() {
        ChainJobRequest request = ChainJobRequest.newBuilder()
                .setJobId(UUID.randomUUID().toString())
                .setInput(Struct.newBuilder().build())
                .build();
        ValidationResult result = ProtoValidator
                .forMessageType(ChainJobRequest.getDescriptor())
                .validate(request);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anySatisfy(v -> assertThat(v.path()).contains("chain_name"));
    }
}
