package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ArchiveMutationContractTest {
    private final ProtoValidator validator = ProtoValidator.create();
    private static final EntryAddress ADDRESS = EntryAddress.newBuilder().setAccountId("account")
            .setArchive("records").setEntryId("entry").build();

    @Test void destructiveOperationsRequireTheIdentifiedServiceAndPreservePayloadTags() {
        var file = DeleteEntryRequest.getDescriptor().getFile();
        var service = file.findServiceByName("ArchiveService");
        for (String name : List.of("DeleteEntry", "DeleteRendition", "PruneVersions")) {
            assertThat(service.findMethodByName(name)).isNull();
            assertThat(file.findMessageTypeByName(name + "Response")).isNull();
            assertThat(file.findMessageTypeByName(name + "Request").findFieldByName("address").getNumber()).isEqualTo(1);
        }
        assertThat(DeleteRenditionRequest.getDescriptor().findFieldByName("rendition").getNumber()).isEqualTo(2);
        assertThat(DeleteRenditionRequest.getDescriptor().findFieldByName("reason").getNumber()).isEqualTo(3);
        assertThat(PruneVersionsRequest.getDescriptor().findFieldByName("keep_latest").getNumber()).isEqualTo(2);
        var mutation = ArchiveMutationRequest.getDescriptor();
        assertThat(mutation.findFieldByName("operation_id").getNumber()).isEqualTo(1);
        assertThat(mutation.findFieldByName("delete_entry").getMessageType()).isEqualTo(DeleteEntryRequest.getDescriptor());
        assertThat(mutation.findFieldByName("delete_rendition").getMessageType()).isEqualTo(DeleteRenditionRequest.getDescriptor());
        assertThat(mutation.findFieldByName("prune_versions").getMessageType()).isEqualTo(PruneVersionsRequest.getDescriptor());
    }

    @Test void requiresAnIdentifiedExclusiveCommandAndValidNestedRequest() throws Exception {
        var delete = ArchiveMutationRequest.newBuilder().setOperationId(UUID.randomUUID().toString())
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(ADDRESS)).build();
        check(delete, true);
        check(delete.toBuilder().clearOperationId().build(), false);
        check(delete.toBuilder().setOperationId("not-a-uuid").build(), false);
        check(delete.toBuilder().clearMutation().build(), false);
        check(delete.toBuilder().setDeleteEntry(DeleteEntryRequest.getDefaultInstance()).build(), false);
        var prune = delete.toBuilder().setPruneVersions(PruneVersionsRequest.newBuilder().setAddress(ADDRESS).setKeepLatest(1)).build();
        assertThat(prune.hasDeleteEntry()).isFalse();
        check(prune, true);
        check(prune.toBuilder().setPruneVersions(prune.getPruneVersions().toBuilder().setKeepLatest(0)).build(), false);
        check(delete.toBuilder().setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(ADDRESS)
                .setRendition("original").setReason("retention")).build(), true);
        check(delete.toBuilder().setDeleteRendition(DeleteRenditionRequest.newBuilder().setAddress(ADDRESS)
                .setRendition("original")).build(), false);
    }

    @Test void validatesLogicalOutcomesAndPhysicalObservationSeparately() throws Exception {
        var admitted = receipt().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED)
                .setEntryDeleted(true).setVersionsRemoved(2).setObjectsTargeted(1).setObjectsPending(1).build();
        check(admitted, true);
        check(admitted.toBuilder().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_RECLAIMING).build(), true);
        check(admitted.toBuilder().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_RETRY_REQUIRED)
                .setErrorCode("BACKEND_UNAVAILABLE").build(), true);
        var completed = admitted.toBuilder().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED)
                .setObjectsPending(0).setObjectsConfirmedAbsent(1).build();
        check(completed, true);
        // Identified absent-entry result is a valid durable no-op, not permission to retry deletion later.
        check(receipt().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED).build(), true);
        check(receipt().setEntryDeleted(true).setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED).build(), true);
        for (var kind : List.of(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_RENDITION, ArchiveMutationKind.ARCHIVE_MUTATION_KIND_PRUNE_VERSIONS)) {
            check(receipt().setKind(kind).setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED).build(), true);
            check(admitted.toBuilder().setKind(kind).build(), false);
        }
        check(admitted.toBuilder().setKind(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_PRUNE_VERSIONS).setEntryDeleted(false).build(), true);
        check(admitted.toBuilder().setKind(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_RENDITION)
                .setEntryDeleted(false).setVersionsRemoved(0).setVersionsTombstoned(2).build(), true);
        for (var invalid : List.of(
                admitted.toBuilder().setStateValue(99).build(),
                admitted.toBuilder().clearKind().build(),
                admitted.toBuilder().setVersionsRemoved(-1).build(),
                admitted.toBuilder().setObjectsPending(2).build(),
                admitted.toBuilder().setObjectsConfirmedAbsent(-1).build(),
                admitted.toBuilder().setVersionsTombstoned(1).build(),
                admitted.toBuilder().setKind(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_RENDITION).setEntryDeleted(false).build(),
                admitted.toBuilder().setEntryDeleted(false).build(),
                admitted.toBuilder().clearObservedAt().build(),
                admitted.toBuilder().clearStatusRevision().build(),
                admitted.toBuilder().setStatusRevision(-1).build(),
                admitted.toBuilder().setEntryDeleted(false).setVersionsRemoved(0).build(),
                admitted.toBuilder().setCommandSha256("bad").build(),
                admitted.toBuilder().setErrorCode("BACKEND_UNAVAILABLE").build(),
                admitted.toBuilder().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_RETRY_REQUIRED).build(),
                completed.toBuilder().setObjectsPending(1).build())) check(invalid, false);
    }

    @Test void lookupRequiresAccountAndUuidAndSchemaRecordsRuntimeRules() throws Exception {
        var lookup = GetArchiveMutationRequest.newBuilder().setAccountId("account").setOperationId(UUID.randomUUID().toString()).build();
        check(lookup, true);
        check(lookup.toBuilder().clearAccountId().build(), false);
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var schema = new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(generator.generateRooted(ArchiveMutationReceipt.getDescriptor()));
        assertThat(schema.at("/properties/operationId/format").asText()).isEqualTo("uuid");
        assertThat(schema.path("x-protomolt-cel").toString()).contains("archive-mutation-target-accounting");
        // State accounting CEL remains runtime-only, not portable OpenAPI enforcement.
    }

    private static ArchiveMutationReceipt.Builder receipt() {
        return ArchiveMutationReceipt.newBuilder().setOperationId(UUID.randomUUID().toString()).setAddress(ADDRESS)
                .setKind(ArchiveMutationKind.ARCHIVE_MUTATION_KIND_DELETE_ENTRY).setCommandSha256("a".repeat(64))
                .setObservedAt(Timestamp.newBuilder().setSeconds(1)).setStatusRevision(1);
    }

    @Test void successfulTransportResponsesRequireAValidReceipt() throws Exception {
        var noop = receipt().setState(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED).build();
        check(ArchiveMutationResponse.newBuilder().setReceipt(noop).build(), true);
        check(GetArchiveMutationResponse.newBuilder().setReceipt(noop).build(), true);
        check(ArchiveMutationResponse.getDefaultInstance(), false);
        check(GetArchiveMutationResponse.getDefaultInstance(), false);
        check(ArchiveMutationResponse.newBuilder().setReceipt(noop.toBuilder().setObjectsPending(1)).build(), false);
    }

    private void check(Message message, boolean valid) throws Exception {
        var result = validator.validate(message);
        assertThat(result.valid()).as("%s: %s", message, result).isEqualTo(valid);
        assertThat(validator.validate(DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray())).valid())
                .as("dynamic %s", message.getDescriptorForType().getName()).isEqualTo(valid);
    }
}
