package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.Descriptors;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * The canonical SaveIntent encoding (requirements §7) frozen by golden vectors:
 * every .pb under test resources is the deterministic encoding of the intent the
 * matching builder produces, and its .sha256 is the intent digest. The vectors
 * pin field-number order, unsigned-UTF-8 metadata order (multi-byte and
 * supplementary code points), validation-mode normalization, request order of
 * writes and sources, attestation normalization and ordering, and the
 * save/delete domain separation.
 */
class SaveIntentsIT {
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setAccountId("account")
            .setGraphId("graph").setGraphAddressId("address").setDocId("document").build();
    private static final WriteProvenance STEP = WriteProvenance.newBuilder().setModuleId("module")
            .setNodeId("node").setGraphId("graph").setGraphVersion(9007199254740993L).build();
    private static final SaveOwner STEP_OWNER = SaveOwner.newBuilder().setStep(STEP).build();

    @Test void frozenIntentKeepsExistingDigest() throws Exception {
        var intent = fixture();
        try (var bytes = getClass().getResourceAsStream("/save-intent/v1.pb");
             var digest = getClass().getResourceAsStream("/save-intent/v1.sha256")) {
            assertThat(bytes).isNotNull();
            assertThat(digest).isNotNull();
            var frozen = bytes.readAllBytes();
            var expected = new String(digest.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
            assertThat(SaveIntent.parseFrom(frozen)).isEqualTo(intent);
            // Every field but unchanged, which the unchanged-policy vector freezes: v1 predates it,
            // and an unset policy leaves the encoding, and so every recorded digest, as it was.
            assertThat(intent.getAllFields().keySet()).extracting(Descriptors.FieldDescriptor::getName)
                    .containsExactlyInAnyOrderElementsOf(SaveIntent.getDescriptor().getFields().stream()
                            .map(Descriptors.FieldDescriptor::getName).filter(name -> !name.equals("unchanged")).toList());
            assertThat(ai.protomolt.proto.descriptors.MessageFingerprints.deterministicBytes(intent)).isEqualTo(frozen);
            assertThat(SaveIntents.sha256Hex(frozen)).isEqualTo(expected);
            assertThat(SaveIntents.sha256(intent)).isEqualTo(expected);
        }
    }

    @Test void goldenVectorsFreezeEveryNormalizationRule() throws Exception {
        for (var vector : vectors().entrySet()) {
            var intent = vector.getValue();
            try (var bytes = getClass().getResourceAsStream("/save-intent/" + vector.getKey() + ".pb");
                 var digest = getClass().getResourceAsStream("/save-intent/" + vector.getKey() + ".sha256")) {
                assertThat(bytes).as("%s.pb exists", vector.getKey()).isNotNull();
                assertThat(digest).as("%s.sha256 exists", vector.getKey()).isNotNull();
                var frozen = bytes.readAllBytes();
                var expected = new String(digest.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
                assertThat(SaveIntent.parseFrom(frozen)).as(vector.getKey()).isEqualTo(intent);
                assertThat(ai.protomolt.proto.descriptors.MessageFingerprints.deterministicBytes(intent))
                        .as(vector.getKey()).isEqualTo(frozen);
                assertThat(SaveIntents.sha256Hex(frozen)).as(vector.getKey()).isEqualTo(expected);
                assertThat(SaveIntents.sha256(intent)).as(vector.getKey()).isEqualTo(expected);
            }
        }
    }

    @Test void normalizationRulesAreVisibleInTheBuiltIntents() {
        // Metadata sorts by unsigned UTF-8 key: ascii, decomposed accent, z, precomposed, CJK, supplementary.
        assertThat(vectors().get("unicode-metadata").getMetadataList().stream().map(IntentMetadata::getKey).toList())
                .containsExactly("a", "é", "z", "é", "中", "😀");
        // Writes and sources keep request order; nothing is resorted by part number or selector encoding.
        var ordered = vectors().get("ordered-writes");
        assertThat(ordered.getWritesList().stream().map(write -> write.getWrite().getSelector().getPart()).toList())
                .containsExactly(DocumentPart.DOCUMENT_PART_PARSED, DocumentPart.DOCUMENT_PART_CORE);
        assertThat(ordered.getSourcesList().stream().map(source -> source.getSelector().getSegmentationId()).toList())
                .containsExactly("seg-b", "seg-a");
        // Attestations sort by their locator's encoding and inherit the save's step owner.
        var attestations = vectors().get("attestations").getOpaqueAttestationsList();
        assertThat(attestations.stream().map(OpaqueAttestation::getTypeUrl).toList())
                .containsExactly("type.example/Earlier", "type.example/Later");
        assertThat(attestations).allSatisfy(attestation -> assertThat(attestation.getOwner()).isEqualTo(STEP));
        // Domain separation: the same content under another operation kind encodes and digests differently.
        var delete = vectors().get("delete-kind");
        assertThat(delete.getOperationKind()).isEqualTo("delete");
        assertThat(SaveIntents.sha256(delete)).isNotEqualTo(SaveIntents.sha256(vectors().get("unicode-metadata")));
    }

    @Test void unspecifiedValidationModeNormalizesToTyped() {
        var core = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-mode")
                .setTarget(DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .addWrites(PartWrite.newBuilder().setSelector(core).setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(STEP_OWNER);
        var contents = Map.of(0, new SaveIntents.Content("ab".repeat(32), 10));
        var unspecified = SaveIntents.intent(request.clone().build(), ADDRESS, contents);
        var typed = SaveIntents.intent(request.setValidationMode(ValidationMode.VALIDATION_MODE_TYPED).build(),
                ADDRESS, contents);
        assertThat(unspecified.getValidationMode()).isEqualTo(ValidationMode.VALIDATION_MODE_TYPED);
        assertThat(unspecified).isEqualTo(typed);
        assertThat(SaveIntents.sha256(unspecified)).isEqualTo(SaveIntents.sha256(typed));
    }

    @Test void duplicateAttestationLocatorsAreInvalid() {
        var core = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var occurrence = OccurrenceLocator.newBuilder().setPart(core)
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)).build();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-duplicates")
                .setTarget(DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addWrites(PartWrite.newBuilder().setSelector(core).setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(STEP_OWNER)
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(occurrence).setTypeUrl("type.example/A"))
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(occurrence).setTypeUrl("type.example/B"));
        assertThatThrownBy(() -> SaveIntents.intent(request.build(), ADDRESS,
                Map.of(0, new SaveIntents.Content("ab".repeat(32), 10))))
                .isInstanceOf(AccessionException.class)
                .hasMessageContaining("Duplicate opaque occurrence locator");
    }

    /**
     * Cross-language proof, independent of MessageFingerprints: a Python wire
     * decoder/encoder in the goldens directory re-parses every vector, requires
     * canonical field order and minimal varints, re-encodes, and must reproduce
     * the exact .pb bytes and the .sha256 digest. The script runs in a pinned
     * Python container so the proof never depends on the host's interpreter.
     */
    @Test void anIndependentWireEncoderReproducesTheGoldens() throws Exception {
        var script = getClass().getResource("/save-intent/verify_save_intent_goldens.py");
        assertThat(script).isNotNull();
        var goldens = Path.of(script.toURI()).getParent();
        long count;
        try (var files = Files.list(goldens)) {
            count = files.filter(path -> path.toString().endsWith(".pb")).count();
        }
        assertThat(count).isPositive();
        try (var python = new GenericContainer<>(DockerImageName.parse("python:3.12-slim"))
                .withCopyFileToContainer(MountableFile.forHostPath(goldens), "/goldens")
                .withCommand("sleep", "infinity")) {
            python.start();
            var run = python.execInContainer("python3", "/goldens/verify_save_intent_goldens.py", "/goldens");
            var output = run.getStdout() + run.getStderr();
            assertThat(run.getExitCode()).as(output).isZero();
            assertThat(run.getStdout()).as(output).contains(count + " vectors verified");
        }
    }

    @Test void reachableIntentDescriptorsHaveNoMaps() {
        // Map fields have no portable canonical order; the intent model spells them as
        // repeated key/value messages instead. The cross-language vectors depend on this.
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

    /** The named golden vectors; every one is also checked by the independent wire encoder. */
    static Map<String, SaveIntent> vectors() {
        var vectors = new LinkedHashMap<String, SaveIntent>();
        vectors.put("unicode-metadata", unicodeMetadata());
        vectors.put("ordered-writes", orderedWrites());
        vectors.put("attestations", attestations());
        vectors.put("delete-kind", unicodeMetadata().toBuilder().setOperationKind("delete").build());
        vectors.put("unchanged-policy", unicodeMetadata().toBuilder()
                .setUnchanged(UnchangedPolicy.UNCHANGED_POLICY_RECORD_OUTCOME).build());
        return vectors;
    }

    /** Metadata in scrambled insertion order; the intent sorts it by unsigned UTF-8 key. */
    private static SaveIntent unicodeMetadata() {
        var core = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-unicode")
                .setTarget(DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                .addWrites(PartWrite.newBuilder().setSelector(core).setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(STEP_OWNER).setDrive("hot")
                .putMetadata("😀", "supplementary")
                .putMetadata("中", "cjk")
                .putMetadata("é", "precomposed")
                .putMetadata("é", "decomposed")
                .putMetadata("z", "last-ascii")
                .putMetadata("a", "first");
        return SaveIntents.intent(request.build(), ADDRESS,
                Map.of(0, new SaveIntents.Content("cd".repeat(32), 42)));
    }

    /** Writes PARSED before CORE and sources seg-b before seg-a; the intent keeps that order. */
    private static SaveIntent orderedWrites() {
        var parsed = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build();
        var core = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-ordered")
                .setTarget(DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ExpectedRevision.newBuilder().setRevision(7))
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                .addWrites(PartWrite.newBuilder().setSelector(parsed).setAction(WriteAction.WRITE_ACTION_PUT))
                .addWrites(PartWrite.newBuilder().setSelector(core).setAction(WriteAction.WRITE_ACTION_PUT))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setSegmentationId("seg-b"))
                        .setFromAddress(ADDRESS).setFromRevision(5))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setSegmentationId("seg-a"))
                        .setFromAddress(ADDRESS).setFromRevision(3))
                .setOwner(STEP_OWNER).setDrive("archive");
        return SaveIntents.intent(request.build(), ADDRESS, Map.of(
                0, new SaveIntents.Content("ab".repeat(32), 100),
                1, new SaveIntents.Content("cd".repeat(32), 200)));
    }

    /** Two ownerless attestations whose locators encode in the opposite of request order. */
    private static SaveIntent attestations() {
        var core = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var later = OccurrenceLocator.newBuilder().setPart(core)
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(9)).build();
        var earlier = OccurrenceLocator.newBuilder().setPart(core)
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(3)).build();
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-attest")
                .setTarget(DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addWrites(PartWrite.newBuilder().setSelector(core).setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(STEP_OWNER)
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(later)
                        .setTypeUrl("type.example/Later"))
                .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(earlier)
                        .setTypeUrl("type.example/Earlier"));
        return SaveIntents.intent(request.build(), ADDRESS, Map.of(0, new SaveIntents.Content("ef".repeat(32), 64)));
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
