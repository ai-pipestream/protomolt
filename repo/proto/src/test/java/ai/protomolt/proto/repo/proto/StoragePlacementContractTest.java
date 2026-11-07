package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class StoragePlacementContractTest {
    @Test void bothRuleDialectsRejectAmbiguousPlacementOnGeneratedAndDynamicRequests() throws Exception {
        var valid = SaveDocumentRequest.newBuilder().setOperationId("promotion")
                .setTarget(DocumentTarget.newBuilder().setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId("doc")))
                .setExpected(ExpectedRevision.newBuilder().setRevision(1)).setDrive("durable")
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("source")))
                .setStoragePlacement(StoragePlacement.STORAGE_PLACEMENT_SELECTED_DRIVE).build();
        for (var rules : List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource())) {
            var validator = ProtoValidator.create(List.of(rules));
            assertThat(validator.validate(valid).valid()).isTrue();
            for (var invalid : List.of(valid.toBuilder().clearDrive().build(),
                    valid.toBuilder().setStoragePlacementValue(500).build(),
                    valid.toBuilder().setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME).build())) {
                assertThat(validator.validate(invalid).valid()).isFalse();
                assertThat(validator.validate(DynamicMessage.parseFrom(invalid.getDescriptorForType(),
                        invalid.toByteString())).valid()).isFalse();
            }
        }
    }

    @Test void pinsThePublicPromotionClosureIncludingBothValidationDialects() {
        // This new public-contract pin deliberately covers the additive placement field,
        // its enum and both house/Buf annotations. The retained Document closure is unchanged.
        assertThat(DescriptorFingerprints.fingerprintOf(SaveDocumentRequest.getDescriptor()))
                .isEqualTo("ab62987982c4b1b342d578697a751826882118ff9f79a1354b892d0709876a6c");
    }
}
