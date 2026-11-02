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
        // The closure includes archive_service.proto, including DeleteEntryRequest's
        // optional expected_version guard added by #742; filename rules are unchanged.
        assertThat(DescriptorFingerprints.fingerprintOf(UploadRenditionHeader.getDescriptor()))
                .isEqualTo("64be438a84dccc95cd6e12a5cb4085d089c7378df409e187e128582a4efff2ef");
    }
}
