package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.ChangeOperation;
import ai.protomolt.proto.acquire.confluence.v1.ChangeSource;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.protomolt.proto.acquire.confluence.v1.Page;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * {@link LoggingChangeSink} has no state to assert on; the contract is that
 * the log-line formatting never throws, on fully populated envelopes and on
 * bare default instances alike.
 */
class LoggingChangeSinkTest {

    private final LoggingChangeSink sink = new LoggingChangeSink();

    @Test
    void logsPopulatedChangeAndSnapshot() {
        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setSpaceId("100")))
                .build();
        ConfluenceSnapshot snapshot = ConfluenceSnapshot.newBuilder()
                .setSnapshotId("s1")
                .setSpaceKey("ENG")
                .putEntityCounts("page", 3L)
                .setCursor("2024-03-01T00:00:00Z")
                .build();

        assertThatCode(() -> {
            sink.emit(change);
            sink.snapshot(snapshot);
        }).doesNotThrowAnyException();
    }

    @Test
    void logsDefaultInstancesWithoutThrowing() {
        // Default instances render empty ids and the UNSPECIFIED enum members.
        assertThatCode(() -> {
            sink.emit(ConfluenceChange.getDefaultInstance());
            sink.snapshot(ConfluenceSnapshot.getDefaultInstance());
        }).doesNotThrowAnyException();
    }
}
