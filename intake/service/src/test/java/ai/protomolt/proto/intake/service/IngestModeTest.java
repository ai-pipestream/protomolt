package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.intake.v1.IngestMode;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class IngestModeTest {
    @Test void unspecifiedPreservesCreateAndExplicitExpectation() {
        assertThat(RawIngests.resolveExpected(IngestMode.INGEST_MODE_UNSPECIFIED, null).getNone()).isTrue();
        var expected = ExpectedRevision.newBuilder().setRevision(4).build();
        assertThat(RawIngests.resolveExpected(IngestMode.INGEST_MODE_UNSPECIFIED, expected)).isEqualTo(expected);
        assertThat(RawIngests.resolveExpected(IngestMode.INGEST_MODE_EXPECTED_REVISION, expected)).isEqualTo(expected);
        assertThat(RawIngests.resolveExpected(IngestMode.INGEST_MODE_UPSERT, null)).isNull();
    }

    @Test void contradictoryOrInvalidModesNeverReachRepository() {
        var none = ExpectedRevision.newBuilder().setNone(true).build();
        var revision = ExpectedRevision.newBuilder().setRevision(4).build();
        rejects(IngestMode.INGEST_MODE_UPSERT, none);
        rejects(IngestMode.INGEST_MODE_UPSERT, revision);
        rejects(IngestMode.INGEST_MODE_CREATE_ONLY, revision);
        rejects(IngestMode.INGEST_MODE_EXPECTED_REVISION, null);
        rejects(IngestMode.INGEST_MODE_EXPECTED_REVISION, none);
        rejects(IngestMode.INGEST_MODE_UNSPECIFIED, ExpectedRevision.getDefaultInstance());
        rejects(IngestMode.UNRECOGNIZED, null);
    }

    private static void rejects(IngestMode mode, ExpectedRevision expected) {
        assertThatThrownBy(() -> RawIngests.resolveExpected(mode, expected))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
