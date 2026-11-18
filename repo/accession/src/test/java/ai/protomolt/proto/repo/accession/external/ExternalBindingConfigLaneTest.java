package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalEndpointSummary;
import ai.protomolt.proto.repo.external.v1.ExternalIdentityProof;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * The pre-save config-lane gate checks the binding as protobuf against the type the lane
 * serves: a lane whose schema cannot hold the whole binding, or whose rules it breaks, refuses.
 */
class ExternalBindingConfigLaneTest {

    private static ExternalSystemBinding binding() {
        return ExternalSystemBinding.newBuilder()
                .setBindingId("rust-search").setAccountId("tenant-alpha").setBindingRevision(1L)
                .setProvider("protomolt-managed-search").setServiceProfileId("search-cluster-a")
                .setServiceEndpointId("ingest-grpc").setDataset("corpus-main")
                .setNamespace("ns-alpha").setContractVersion("managed-search.v1")
                .setEndpointSummary(ExternalEndpointSummary.newBuilder()
                        .setHost("search.internal").setPort(9443).setTlsEnabled(true))
                .setLifecycle(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_ACTIVE)
                .setRetentionPolicy(ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY)
                .setRequiredIdentityProof(
                        ExternalIdentityProof.EXTERNAL_IDENTITY_PROOF_MANAGED_SEARCH)
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1_770_000_000L))
                .build();
    }

    /** The binding type as an older config lane would serve it: without one of its fields. */
    private static Descriptor bindingTypeWithout(int fieldNumber) throws Exception {
        FileDescriptor current = ExternalSystemBinding.getDescriptor().getFile();
        FileDescriptorProto.Builder older = current.toProto().toBuilder();
        for (int i = 0; i < older.getMessageTypeCount(); i++) {
            DescriptorProto.Builder message = older.getMessageTypeBuilder(i);
            if (!message.getName().equals("ExternalSystemBinding")) {
                continue;
            }
            for (int f = message.getFieldCount() - 1; f >= 0; f--) {
                if (message.getField(f).getNumber() == fieldNumber) {
                    message.removeField(f);
                }
            }
        }
        FileDescriptor rebuilt = FileDescriptor.buildFrom(
                older.build(), current.getDependencies().toArray(new FileDescriptor[0]));
        return rebuilt.findMessageTypeByName("ExternalSystemBinding");
    }

    @Test
    void aBindingTheLanesSchemaHoldsInFullIsAdmitted() {
        assertThatCode(() -> ExternalBindingConfigLane.requireAdmissible(
                binding(), ExternalSystemBinding.getDescriptor()))
                .doesNotThrowAnyException();
    }

    @Test
    void aLaneWhoseSchemaLacksAFieldTheBindingSetsRefusesInsteadOfDroppingIt() throws Exception {
        Descriptor withoutIdentityProof = bindingTypeWithout(
                ExternalSystemBinding.REQUIRED_IDENTITY_PROOF_FIELD_NUMBER);
        assertThatThrownBy(() -> ExternalBindingConfigLane.requireAdmissible(
                binding(), withoutIdentityProof))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                    org.assertj.core.api.Assertions.assertThat(refused.getStatus().getCode())
                            .isEqualTo(Status.Code.INVALID_ARGUMENT);
                    org.assertj.core.api.Assertions.assertThat(refused.getStatus().getDescription())
                            .contains("field number(s) [18]");
                });
    }

    @Test
    void aBindingThatBreaksTheLanesRulesIsRefusedWithTheRule() {
        ExternalSystemBinding blankDataset = binding().toBuilder().setDataset("").build();
        assertThatThrownBy(() -> ExternalBindingConfigLane.requireAdmissible(
                blankDataset, ExternalSystemBinding.getDescriptor()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                    org.assertj.core.api.Assertions.assertThat(refused.getStatus().getCode())
                            .isEqualTo(Status.Code.INVALID_ARGUMENT);
                    org.assertj.core.api.Assertions.assertThat(refused.getStatus().getDescription())
                            .contains("dataset");
                });
    }

    @Test
    void aLaneWhoseSchemaLacksANestedFieldRefusesToo() throws Exception {
        // The lane's ExternalEndpointSummary has no tls_enabled.
        FileDescriptor current = ExternalSystemBinding.getDescriptor().getFile();
        FileDescriptorProto.Builder older = current.toProto().toBuilder();
        for (int i = 0; i < older.getMessageTypeCount(); i++) {
            DescriptorProto.Builder message = older.getMessageTypeBuilder(i);
            if (!message.getName().equals("ExternalEndpointSummary")) {
                continue;
            }
            for (int f = message.getFieldCount() - 1; f >= 0; f--) {
                if (message.getField(f).getName().equals("tls_enabled")) {
                    message.removeField(f);
                }
            }
        }
        Descriptor laneType = FileDescriptor.buildFrom(
                        older.build(), current.getDependencies().toArray(new FileDescriptor[0]))
                .findMessageTypeByName("ExternalSystemBinding");
        assertThatThrownBy(() -> ExternalBindingConfigLane.requireAdmissible(binding(), laneType))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                    org.assertj.core.api.Assertions.assertThat(refused.getStatus().getCode())
                            .isEqualTo(Status.Code.INVALID_ARGUMENT);
                    org.assertj.core.api.Assertions.assertThat(refused.getStatus().getDescription())
                            .contains("endpoint_summary");
                });
    }
}
