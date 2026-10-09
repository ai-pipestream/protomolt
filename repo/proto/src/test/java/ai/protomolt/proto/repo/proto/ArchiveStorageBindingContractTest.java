package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ArchiveStorageBindingContractTest {
    @Test void recordsJsonSchemaCoverageWithoutClaimingRuntimeRuleParity() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(RenditionManifestEntry.getDescriptor()));
        assertThat(schema.at("/properties/storageObjectId/type").asText()).isEqualTo("string");
        assertThat(schema.path("x-protomolt-cel").toString()).contains("archive-storage-binding-state");
        assertThat(schema.at("/properties/storageObjectId/format").asText()).isEqualTo("uuid");
        // CEL is exposed as an extension, not executable standard JSON Schema.
    }

    @Test void validatesBindingSyntaxThroughTheRuntimeWithoutInventingLegacyBindings() throws Exception {
        var legacy = RenditionManifestEntry.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setState(RenditionState.RENDITION_STATE_PRESENT)
                .setObjectKey("legacy/original").setSizeBytes(7).setSha256("a".repeat(64)).build();
        var validator = ProtoValidator.create();
        assertThat(validator.validate(legacy).valid()).isTrue();
        assertThat(legacy.getStorageObjectId()).isEmpty();
        var bound = legacy.toBuilder().setStorageObjectId(UUID.randomUUID().toString()).build();
        assertThat(validator.validate(bound).valid()).isTrue();
        assertThat(validator.validate(bound.toBuilder().setState(RenditionState.RENDITION_STATE_DELETED).build()).valid()).isTrue();
        for (var state : new RenditionState[] {RenditionState.RENDITION_STATE_EMPTY, RenditionState.RENDITION_STATE_UNSPECIFIED}) {
            var invalidState = bound.toBuilder().setState(state).build();
            assertThat(validator.validate(invalidState).valid()).isFalse();
            assertThat(validator.validate(DynamicMessage.parseFrom(invalidState.getDescriptorForType(), invalidState.toByteArray())).valid())
                    .isFalse();
        }
        for (String invalid : new String[] {"not-an-id", " ", "a".repeat(1000)}) {
            var malformed = bound.toBuilder().setStorageObjectId(invalid).build();
            assertThat(validator.validate(malformed).valid()).isFalse();
            assertThat(validator.validate(DynamicMessage.parseFrom(malformed.getDescriptorForType(), malformed.toByteArray())).valid())
                    .isFalse();
        }
        assertThat(validator.validate(DynamicMessage.parseFrom(bound.getDescriptorForType(), bound.toByteArray())).valid())
                .isTrue();
        assertThat(Any.pack(bound).getTypeUrl())
                .isEqualTo("type.googleapis.com/ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry");
    }

    @Test void anOlderDescriptorRetainsTheAdditiveFieldAsUnknownData() throws Exception {
        var descriptor = RenditionManifestEntry.getDescriptor();
        assertThat(descriptor.findFieldByName("storage_object_id").getNumber()).isEqualTo(10);
        var file = descriptor.getFile().toProto().toBuilder();
        int messageIndex = -1;
        for (int i = 0; i < file.getMessageTypeCount(); i++) {
            if (file.getMessageType(i).getName().equals("RenditionManifestEntry")) messageIndex = i;
        }
        assertThat(messageIndex).isGreaterThanOrEqualTo(0);
        var oldMessage = file.getMessageType(messageIndex).toBuilder();
        // The previous message had no message-level validation option.
        oldMessage.clearOptions();
        oldMessage.clearField();
        descriptor.toProto().getFieldList().stream().filter(field -> field.getNumber() != 10).forEach(oldMessage::addField);
        file.setMessageType(messageIndex, oldMessage);
        var olderFile = Descriptors.FileDescriptor.buildFrom(file.build(),
                descriptor.getFile().getDependencies().toArray(Descriptors.FileDescriptor[]::new));
        var bound = RenditionManifestEntry.newBuilder().setStorageObjectId(UUID.randomUUID().toString()).build();
        var oldReader = DynamicMessage.parseFrom(olderFile.findMessageTypeByName("RenditionManifestEntry"), bound.toByteArray());
        assertThat(oldReader.getUnknownFields().hasField(10)).isTrue();
        assertThat(RenditionManifestEntry.parseFrom(oldReader.toByteArray()).getStorageObjectId())
                .isEqualTo(bound.getStorageObjectId());
    }
}
