package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ArchiveUploadFilenameContractTest {
    @Test
    void theHouseRuleBoundsTheFilenameOnGeneratedAndDynamicHeaders() throws Exception {
        var header = UploadRenditionHeader.newBuilder()
                .setAddress(EntryAddress.newBuilder().setAccountId("account").setArchive("docs").setEntryId("entry"))
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setSizeBytes(3);
        var validator = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
        for (int length : new int[] {0, 500, 501}) {
            var generated = header.setFilename("x".repeat(length)).build();
            var dynamic = DynamicMessage.parseFrom(generated.getDescriptorForType(), generated.toByteString());
            assertThat(validator.validate(generated).valid()).isEqualTo(length <= 500);
            assertThat(validator.validate(dynamic).valid()).isEqualTo(length <= 500);
        }
    }

    @Test
    void pinsUploadHeaderClosureIncludingFilenameAndItsHouseRule() {
        assertThat(DescriptorFingerprints.fingerprintOf(UploadRenditionHeader.getDescriptor()))
                .isEqualTo("120cc1579b39dabbfb248b25f04d9e867d02d2c9ba356bd240c76e7a136df90c");
    }
}
