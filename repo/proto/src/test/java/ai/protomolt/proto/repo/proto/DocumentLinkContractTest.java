package ai.protomolt.proto.repo.proto;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.Backlink;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DynamicTarget;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import ai.protomolt.proto.repo.v2.ListBacklinksRequest;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.ResolveLinkRequest;
import ai.protomolt.proto.repo.v2.ResolvedLink;
import ai.protomolt.proto.repo.v2.StaticLinkState;
import ai.protomolt.proto.repo.v2.StaticTarget;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

/** The §17 link contract's house validate.v1 rules on generated and dynamic messages. */
class DocumentLinkContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setDocId("doc-1")
            .setGraphAddressId("ds-1").setAccountId("acct-1").setGraphId("intake:acct-1").build();

    @Test void theLinkOptionTakesTheNextFreeExtensionNumber() {
        assertThat(LinksProto.link.getNumber()).isEqualTo(59100551);
        assertThat(LinksProto.link.getDescriptor().getContainingType().getFullName())
                .isEqualTo("google.protobuf.FieldOptions");
    }

    @Test void aLinkFieldNamesARelationAndDistinctTypeNames() throws Exception {
        check(LinkField.getDefaultInstance(), false);
        check(LinkField.newBuilder().setRelation("isPartOf").build(), true);
        check(LinkField.newBuilder().setRelation("canonical").build(), true);
        check(LinkField.newBuilder().setRelation("https://example.com/rel/parent").build(), true);
        check(LinkField.newBuilder().setRelation("is part of").build(), false);
        check(LinkField.newBuilder().setRelation("parent").build(), false);
        check(LinkField.newBuilder().setRelation("references").addTargetTypes("acme.v1.Contract").build(), true);
        check(LinkField.newBuilder().setRelation("references").addTargetTypes("acme.v1.Contract")
                .addTargetTypes("acme.v1.Contract").build(), false);
        check(LinkField.newBuilder().setRelation("references").addTargetTypes("not a type").build(), false);
    }

    @Test void aLinkNamesExactlyOneTarget() throws Exception {
        var fixed = StaticTarget.newBuilder().setAddress(ADDRESS).build();
        check(DocumentLink.getDefaultInstance(), false);
        check(DocumentLink.newBuilder().setStaticTarget(fixed).build(), true);
        check(DocumentLink.newBuilder().setStaticTarget(fixed.toBuilder().setRevision(3)).build(), true);
        check(DocumentLink.newBuilder().setStaticTarget(fixed.toBuilder().setRevision(0)).build(), false);
        check(DocumentLink.newBuilder().setStaticTarget(StaticTarget.getDefaultInstance()).build(), false);
        check(DocumentLink.newBuilder().setStaticTarget(fixed).setLabel("").build(), false);
        check(DocumentLink.newBuilder().setStaticTarget(fixed).setLabel("See also").build(), true);
        check(DocumentLink.newBuilder().setDynamicTarget(DynamicTarget.newBuilder()
                .setFilter(DocumentFilter.newBuilder().setCategory("contracts"))).build(), true);
        check(DocumentLink.newBuilder().setDynamicTarget(DynamicTarget.getDefaultInstance()).build(), false);
    }

    @Test void linkReadsAreBounded() throws Exception {
        var resolve = ResolveLinkRequest.newBuilder().setAddress(ADDRESS).setFieldPath("structured_data.cites").setPageSize(10);
        check(resolve.build(), true);
        check(resolve.clone().setPageSize(0).build(), false);
        check(resolve.clone().setPageSize(1001).build(), false);
        check(resolve.clone().setOrdinal(-1).build(), false);
        check(resolve.clone().clearFieldPath().build(), false);
        check(resolve.clone().setFieldPath("p".repeat(1024)).build(), true);
        check(resolve.clone().setFieldPath("p".repeat(1025)).build(), false);
        var backlinks = ListBacklinksRequest.newBuilder().setTarget(ADDRESS).setPageSize(10);
        check(backlinks.build(), true);
        check(backlinks.clone().setRelation("isPartOf").build(), true);
        check(backlinks.clone().setRelation("part of").build(), false);
        check(backlinks.clone().clearTarget().build(), false);
        check(Backlink.newBuilder().setAddress(ADDRESS).setRevision(2).setFieldPath("structured_data.cites")
                .setRelation("references").setPinnedRevision(1).build(), true);
        check(Backlink.newBuilder().setAddress(ADDRESS).setRevision(2).setFieldPath("structured_data.cites")
                .setRelation("references").setPinnedRevision(0).build(), false);
    }

    @Test void aStaticTargetIsTombstonedUnavailableOrNeither() throws Exception {
        var state = StaticLinkState.newBuilder().setAddress(ADDRESS);
        check(link(state.clone()), true);
        check(link(state.clone().setTargetTombstoned(true)), true);
        check(link(state.clone().setTargetUnavailable(true)), true);
        check(link(state.clone().setTargetTombstoned(true).setTargetUnavailable(true)), false);
        check(ResolvedLink.newBuilder().setFieldPath("cites").setRelation("references").build(), false);
    }

    @Test void theLookupAndTheReasonsAreAdditive() {
        assertThat(DocumentLookup.getDescriptor().findFieldByName("structured_data_type").getNumber()).isEqualTo(13);
        assertThat(DocumentLookup.getDescriptor().findFieldByName("structured_data_type").hasPresence()).isTrue();
        assertThat(RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND.getNumber()).isEqualTo(13);
        assertThat(RejectionReason.REJECTION_REASON_LINK_TARGET_TYPE.getNumber()).isEqualTo(14);
    }

    private static ResolvedLink link(StaticLinkState.Builder state) {
        return ResolvedLink.newBuilder().setFieldPath("structured_data.cites").setRelation("references")
                .setStaticTarget(state).build();
    }

    private static void check(Message value, boolean valid) throws Exception {
        var result = VALIDATOR.validate(value);
        assertThat(result.valid()).as("generated %s: %s", value, result).isEqualTo(valid);
        var dynamic = DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString());
        assertThat(VALIDATOR.validate(dynamic).valid()).as("dynamic %s", value).isEqualTo(valid);
    }
}
