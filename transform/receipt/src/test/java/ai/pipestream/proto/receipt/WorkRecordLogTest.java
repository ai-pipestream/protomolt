package ai.pipestream.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

/** The log document's declared rules gate it on the config lane. */
class WorkRecordLogTest {

    @Test
    void aWellFormedLogPassesItsDeclaredRules() {
        WorkRecordLog log = WorkRecordLog.newBuilder()
                .addEntries(WorkRecordLogEntry.newBuilder()
                        .setRecordId("record-1")
                        .setIssuer("records.protomolt.dev")
                        .setManifestSha256(WorkRecords.sha256Hex("manifest".getBytes())))
                .addEntries(WorkRecordLogEntry.newBuilder()
                        .setRecordId("record-2")
                        .setIssuer("records.protomolt.dev")
                        .setManifestSha256(WorkRecords.sha256Hex("second".getBytes()))
                        .setPriorManifestSha256(
                                WorkRecords.sha256Hex("manifest".getBytes())))
                .build();
        assertThat(ProtoValidator.create().validate(log).valid()).isTrue();
    }

    @Test
    void aMalformedEntryRefusesByRule() {
        WorkRecordLog log = WorkRecordLog.newBuilder()
                .addEntries(WorkRecordLogEntry.newBuilder()
                        .setRecordId("record-1")
                        .setIssuer("records.protomolt.dev")
                        .setManifestSha256("not-a-digest"))
                .build();
        assertThat(ProtoValidator.create().validate(log).valid()).isFalse();

        WorkRecordLog blankIssuer = WorkRecordLog.newBuilder()
                .addEntries(WorkRecordLogEntry.newBuilder()
                        .setRecordId("record-1")
                        .setManifestSha256(WorkRecords.sha256Hex("manifest".getBytes())))
                .build();
        assertThat(ProtoValidator.create().validate(blankIssuer).valid()).isFalse();
    }
}
