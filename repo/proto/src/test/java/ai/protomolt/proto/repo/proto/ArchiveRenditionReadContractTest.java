package ai.protomolt.proto.repo.proto;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

/** The ReadRendition contract's house validate.v1 rules (#536). */
class ArchiveRenditionReadContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final EntryAddress ADDRESS = EntryAddress.newBuilder().setAccountId("account")
            .setArchive("records").setEntryId("entry").build();

    @Test void requestsNameOneRenditionAndANonNegativeRange() throws Exception {
        var request = ReadRenditionRequest.newBuilder().setAddress(ADDRESS).setRendition("original");
        check(request.build(), true);
        check(request.clone().setVersion(3).setSubKey("page-1").setOffset(10).setLength(20).build(), true);
        check(request.clone().clearAddress().build(), false);
        check(request.clone().clearRendition().build(), false);
        check(request.clone().setRendition("../escape").build(), false);
        check(request.clone().setSubKey("a/b").build(), false);
        check(request.clone().setVersion(-1).build(), false);
        check(request.clone().setOffset(-1).build(), false);
        check(request.clone().setLength(-1).build(), false);
        long largest = (1L << 62) - 1;
        check(request.clone().setOffset(largest).setLength(largest).build(), true);
        check(request.clone().setOffset(largest + 1).build(), false);
        check(request.clone().setLength(largest + 1).build(), false);
        check(request.clone().setOffset(Long.MAX_VALUE).build(), false);
    }

    @Test void headersMarkWholeObjectsExactlyAndKeepRangesInside() throws Exception {
        var whole = RenditionReadHeader.newBuilder().setVersion(1)
                .setRendition(RenditionDescriptor.newBuilder().setName("original")).setSizeBytes(100)
                .setSha256("a".repeat(64)).setOffset(0).setLength(100).setWholeObject(true);
        check(whole.build(), true);
        check(whole.clone().setWholeObject(false).build(), false);
        check(whole.clone().setLength(99).build(), false);
        check(whole.clone().setLength(99).setWholeObject(false).build(), true);
        check(whole.clone().setOffset(1).setLength(99).setWholeObject(false).build(), true);
        check(whole.clone().setOffset(1).setLength(100).setWholeObject(false).build(), false);
        check(whole.clone().setOffset(101).setLength(0).setWholeObject(false).build(), false);
        check(whole.clone().setSizeBytes(0).setLength(0).build(), true);
        check(whole.clone().setVersion(0).build(), false);
        check(whole.clone().clearSha256().build(), false);
        check(whole.clone().setSha256("A".repeat(64)).build(), false);
        check(whole.clone().clearRendition().build(), false);
    }

    @Test void framesAreAHeaderOrBoundedData() throws Exception {
        check(ReadRenditionResponse.getDefaultInstance(), false);
        check(ReadRenditionResponse.newBuilder().setData(ByteString.copyFrom(new byte[65536])).build(), true);
        check(ReadRenditionResponse.newBuilder().setData(ByteString.copyFrom(new byte[65537])).build(), false);
        check(ReadRenditionResponse.newBuilder().setData(ByteString.EMPTY).build(), false);
    }

    private static void check(Message value, boolean valid) throws Exception {
        var result = VALIDATOR.validate(value);
        assertThat(result.valid()).as("generated %s: %s", value, result).isEqualTo(valid);
        var dynamic = DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString());
        assertThat(VALIDATOR.validate(dynamic).valid()).as("dynamic %s", value).isEqualTo(valid);
    }
}
