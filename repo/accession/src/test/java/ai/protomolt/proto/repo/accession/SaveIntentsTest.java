package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.Descriptors;
import java.util.ArrayDeque;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class SaveIntentsTest {
    @Test void frozenIntentKeepsExistingDigest() throws Exception {
        var intent = fixture();
        try (var bytes = getClass().getResourceAsStream("/save-intent/v1.pb");
             var digest = getClass().getResourceAsStream("/save-intent/v1.sha256")) {
            assertThat(bytes).isNotNull();
            assertThat(digest).isNotNull();
            var frozen = bytes.readAllBytes();
            var expected = new String(digest.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
            assertThat(SaveIntent.parseFrom(frozen)).isEqualTo(intent);
            assertThat(intent.getAllFields()).hasSize(SaveIntent.getDescriptor().getFields().size());
            assertThat(ai.protomolt.proto.descriptors.MessageFingerprints.deterministicBytes(intent)).isEqualTo(frozen);
            assertThat(SaveIntents.sha256Hex(frozen)).isEqualTo(expected);
            assertThat(SaveIntents.sha256(intent)).isEqualTo(expected);
        }
    }

    @Test void reachableIntentDescriptorsHaveNoMaps() {
        // #325: retain until #319 supplies cross-language vectors for map-bearing intents.
        var pending = new ArrayDeque<Descriptors.Descriptor>();
        var visited = new HashSet<String>();
        pending.add(SaveIntent.getDescriptor());
        while (!pending.isEmpty()) {
            var descriptor = pending.removeFirst();
            if (!visited.add(descriptor.getFullName())) continue;
            for (var field : descriptor.getFields()) {
                assertThat(field.isMapField()).as(field.getFullName()).isFalse();
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE)
                    pending.add(field.getMessageType());
            }
        }
    }

    private static SaveIntent fixture() {
        var address = NodeAddress.newBuilder().setAccountId("account").setGraphId("graph")
                .setGraphAddressId("address").setDocId("document").build();
        var source = SourceRevision.newBuilder().setAddress(address).setRevision(7).build();
        var selector = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var producer = WriteProvenance.newBuilder().setModuleId("module").setNodeId("node")
                .setGraphId("graph").setGraphVersion(9007199254740993L).build();
        var occurrence = OccurrenceLocator.newBuilder().setPart(selector)
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)).build();
        return SaveIntent.newBuilder().setEncodingVersion(1).setAddress(address)
                .setExpected(ExpectedRevision.newBuilder().setRevision(7))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addWrites(IntentWrite.newBuilder().setWrite(PartWrite.newBuilder().setSelector(selector)
                        .setAction(WriteAction.WRITE_ACTION_PUT)).setContentSha256("ab".repeat(32)).setContentLength(123))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_PARSED)).setFromAddress(address).setFromRevision(6))
                .setDrive("archive").setOwner(SaveOwner.newBuilder().setStep(producer))
                .addMetadata(IntentMetadata.newBuilder().setKey("a").setValue("first"))
                .addMetadata(IntentMetadata.newBuilder().setKey("é").setValue("second"))
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(occurrence)
                        .setTypeUrl("type.example/Record").setOwner(producer)
                        .setCarriedFrom(OpaqueSource.newBuilder().setSource(source).setOccurrence(occurrence)))
                .setOperationKind("save").addDerivedFrom(source).build();
    }
}
