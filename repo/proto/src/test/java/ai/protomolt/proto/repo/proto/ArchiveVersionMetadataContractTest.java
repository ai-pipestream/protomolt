package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ArchiveVersionMetadataContractTest {
    private static final ProtoValidator VALIDATOR=ProtoValidator.create();
    @Test void absentLegacySnapshotDoesNotInventCurrentMetadata() throws Exception {
        var legacy=manifest().clearMetadataSnapshot().build();
        check(legacy,true);
        assertThat(legacy.hasMetadataSnapshot()).isFalse();
        check(manifest().build(),true);
        assertThat(VersionManifest.parseFrom(manifest().build().toByteArray()).getMetadataSnapshot().getTitle()).isEqualTo("captured title");
    }
    @Test void snapshotIdentityMustMatchItsVersion() throws Exception {
        var valid=manifest().build();
        check(valid.toBuilder().setMetadataSnapshot(valid.getMetadataSnapshot().toBuilder().setCurrentVersion(2)).build(),false);
        check(valid.toBuilder().setVersion(0).setMetadataSnapshot(valid.getMetadataSnapshot().toBuilder().setCurrentVersion(0)).build(),false);
        check(valid.toBuilder().setMetadataSnapshot(valid.getMetadataSnapshot().toBuilder().setAddress(address().toBuilder().setEntryId("another"))).build(),false);
        check(valid.toBuilder().setMetadataSnapshot(valid.getMetadataSnapshot().toBuilder().clearEntryUuid()).build(),false);
        check(valid.toBuilder().setMetadataSnapshot(valid.getMetadataSnapshot().toBuilder().setEntryUuid("not-a-uuid")).build(),false);
        check(valid.toBuilder().setMetadataSnapshot(valid.getMetadataSnapshot().toBuilder().setTitle("x".repeat(501))).build(),false);
    }
    private static EntryAddress address() {
        return EntryAddress.newBuilder().setAccountId("account").setArchive("archive").setEntryId("entry").build();
    }
    private static VersionManifest.Builder manifest() {
        return VersionManifest.newBuilder().setAddress(address()).setVersion(1)
                .setMetadataSnapshot(EntryInfo.newBuilder().setAddress(address()).setCurrentVersion(1)
                        .setEntryUuid("00000000-0000-0000-0000-000000000001").setTitle("captured title"));
    }
    private static void check(Message message,boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(message).valid()).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(message.getDescriptorForType(),message.toByteArray())).valid()).isEqualTo(valid);
    }
}
