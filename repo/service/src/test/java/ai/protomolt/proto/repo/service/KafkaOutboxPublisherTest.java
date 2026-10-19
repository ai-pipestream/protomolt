package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufSerializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.Accessioned;
import ai.protomolt.proto.repo.v2.ChangeKind;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartChange;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Timestamp;
import java.util.Base64;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

/**
 * The publisher's serde contract without a broker: the descriptor set it ships
 * configures the protomolt serializer for {@link DocumentEvent}, carries the event's
 * declared rules, and round-trips through the validating deserializer.
 */
class KafkaOutboxPublisherTest {
    private static final String TOPIC = "document-events";

    static DocumentEvent accessioned(String eventId, String docId) {
        return DocumentEvent.newBuilder()
                .setEventId(eventId)
                .setAddress(NodeAddress.newBuilder().setAccountId("acct-1").setDocId(docId)
                        .setGraphAddressId("ds-1").setGraphId("intake:acct-1"))
                .setRevision(1)
                .setOperationId("op-" + eventId)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1_760_000_000L))
                .setAccessioned(Accessioned.newBuilder()
                        .setValidationStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                        .addChanges(PartChange.newBuilder()
                                .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                .setKind(ChangeKind.CHANGE_KIND_ADDED)
                                .setSha256("a".repeat(64))))
                .build();
    }

    private static Map<String, Object> serde() {
        return Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaOutboxPublisher.documentEventDescriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true);
    }

    @Test void theDescriptorSetIsTheEventFileAndItsTransitiveImports() throws Exception {
        var set = DescriptorProtos.FileDescriptorSet.parseFrom(
                Base64.getDecoder().decode(KafkaOutboxPublisher.documentEventDescriptorSetBase64()));
        assertThat(set.getFileList()).extracting(DescriptorProtos.FileDescriptorProto::getName)
                .doesNotHaveDuplicates()
                .contains(DocumentEvent.getDescriptor().getFile().getName(),
                        NodeAddress.getDescriptor().getFile().getName(),
                        "ai/protomolt/proto/validate/v1/validate.proto",
                        "google/protobuf/timestamp.proto");
    }

    @Test void aValidEventRoundTripsThroughTheValidatingSerde() {
        var event = accessioned("event-1", "doc-1");
        try (var serializer = new ProtoMoltProtobufSerializer();
                var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(serde(), false);
            deserializer.configure(serde(), false);
            byte[] framed = serializer.serialize(TOPIC, event);
            assertThat(deserializer.deserialize(TOPIC, framed)).isInstanceOf(DocumentEvent.class).isEqualTo(event);
        }
    }

    @Test void anEventWithoutAKindIsRefusedAgainstTheShippedRules() {
        var kindless = accessioned("event-2", "doc-2").toBuilder().clearKind().build();
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(serde(), false);
            assertThatThrownBy(() -> serializer.serialize(TOPIC, kindless))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("event-kind-required");
        }
    }

    @Test void blankConnectionSettingsAreRejectedByName() {
        assertThatThrownBy(() -> new KafkaOutboxPublisher(" ", TOPIC))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bootstrapServers");
        assertThatThrownBy(() -> new KafkaOutboxPublisher("localhost:9092", ""))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");
        assertThatThrownBy(() -> new KafkaOutboxPublisher(null, TOPIC))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("bootstrapServers");
    }
}
