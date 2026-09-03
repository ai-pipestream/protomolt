package ai.protomolt.proto.mesh;

import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import ai.protomolt.proto.mesh.v1.EntityState;
import ai.protomolt.proto.mesh.v1.EntityStatus;
import ai.protomolt.proto.mesh.v1.TerminalState;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves every Timestamp field in the mesh contract is rejected when it violates the
 * well-known-type validity rules (seconds within 0001-01-01T00:00:00Z..9999-12-31T23:59:59Z,
 * nanos within [0, 999999999]).
 */
class MeshTimestampValidityTest {

    private static final Instant BEFORE_DEADLINE = Instant.ofEpochSecond(1_700_000_300L);
    /** One second before the WKT minimum (0001-01-01T00:00:00Z is -62135596800). */
    private static final long INVALID_SECONDS_LOW = -62135596801L;
    /** One second past the WKT maximum (9999-12-31T23:59:59Z is 253402300799). */
    private static final long INVALID_SECONDS_HIGH = 253402300800L;

    private static Timestamp invalidSeconds() {
        return Timestamp.newBuilder().setSeconds(INVALID_SECONDS_LOW).build();
    }

    private static Timestamp invalidSecondsHigh() {
        return Timestamp.newBuilder().setSeconds(INVALID_SECONDS_HIGH).build();
    }

    private static Timestamp invalidNanos() {
        return Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(1_000_000_000).build();
    }

    private static void assertRejected(EntityEnvelope entity, String field) {
        assertThatThrownBy(() -> MeshValidation.validate(entity, BEFORE_DEADLINE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(field)
                .hasMessageContaining("valid protobuf Timestamp");
    }

    @Test
    void createdAtWithInvalidSecondsIsRejected() {
        assertRejected(MeshFixtures.inlineEntity()
                        .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                                .setCreatedAt(invalidSeconds()))
                        .build(),
                "header.created_at");
        assertRejected(MeshFixtures.inlineEntity()
                        .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                                .setCreatedAt(invalidSecondsHigh()))
                        .build(),
                "header.created_at");
    }

    @Test
    void createdAtWithInvalidNanosIsRejected() {
        assertRejected(MeshFixtures.inlineEntity()
                        .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                                .setCreatedAt(invalidNanos()))
                        .build(),
                "header.created_at");
    }

    @Test
    void deadlineWithInvalidSecondsIsRejected() {
        assertRejected(MeshFixtures.inlineEntity()
                        .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                                .setDeadline(invalidSeconds()))
                        .build(),
                "header.deadline");
        assertRejected(MeshFixtures.inlineEntity()
                        .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                                .setDeadline(invalidSecondsHigh()))
                        .build(),
                "header.deadline");
    }

    @Test
    void deadlineWithInvalidNanosIsRejected() {
        assertRejected(MeshFixtures.inlineEntity()
                        .setHeader(MeshFixtures.inlineEntity().getHeader().toBuilder()
                                .setDeadline(invalidNanos()))
                        .build(),
                "header.deadline");
    }

    @Test
    void claimCheckExpiresAtWithInvalidSecondsIsRejected() {
        assertRejected(MeshFixtures.claimCheckEntity()
                        .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck().toBuilder()
                                .setExpiresAt(invalidSeconds()))
                        .build(),
                "claim_check.expires_at");
    }

    @Test
    void claimCheckExpiresAtWithInvalidNanosIsRejected() {
        assertRejected(MeshFixtures.claimCheckEntity()
                        .setClaimCheck(MeshFixtures.claimCheckEntity().getClaimCheck().toBuilder()
                                .setExpiresAt(invalidNanos()))
                        .build(),
                "claim_check.expires_at");
    }

    @Test
    void terminalAtWithInvalidSecondsIsRejected() {
        EntityStatus status = terminalStatus().setTerminalAt(invalidSeconds()).build();
        assertStatusRejected(status, "status.terminal_at");
    }

    @Test
    void terminalAtWithInvalidNanosIsRejected() {
        EntityStatus status = terminalStatus().setTerminalAt(invalidNanos()).build();
        assertStatusRejected(status, "status.terminal_at");
    }

    @Test
    void updatedAtWithInvalidSecondsIsRejected() {
        EntityStatus status = terminalStatus().setUpdatedAt(invalidSeconds()).build();
        assertStatusRejected(status, "status.updated_at");
    }

    @Test
    void updatedAtWithInvalidNanosIsRejected() {
        EntityStatus status = terminalStatus().setUpdatedAt(invalidNanos()).build();
        assertStatusRejected(status, "status.updated_at");
    }

    private static EntityStatus.Builder terminalStatus() {
        return EntityStatus.newBuilder()
                .setState(EntityState.ENTITY_STATE_TERMINAL)
                .setTerminalState(TerminalState.TERMINAL_STATE_COMPLETED)
                .setTerminalAt(MeshFixtures.DEADLINE)
                .setUpdatedAt(MeshFixtures.DEADLINE);
    }

    private static void assertStatusRejected(EntityStatus status, String field) {
        assertThatThrownBy(() -> MeshValidation.validate(status))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(field)
                .hasMessageContaining("valid protobuf Timestamp");
    }
}
