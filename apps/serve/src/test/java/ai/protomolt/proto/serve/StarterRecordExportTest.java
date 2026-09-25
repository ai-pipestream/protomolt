package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.DelegationRecordProjector;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionAccepted;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskAccept;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.RecordSignature;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.WorkRecords;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the record and all referenced bytes before producing any export bytes. */
class StarterRecordExportTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TASK = "9d4e71a0-d840-482c-a3de-6f63ba37b0ec";
    private static final String WORKER = "export-test-worker";
    private static final String ISSUER = "records.protomolt.dev";
    private static final String KEY_ID = "starter-export-test";
    private static final String CHECK = "export-check";
    private static final Timestamp ISSUED = Timestamp.newBuilder().setSeconds(1_790_000_000L).build();
    private static final KeyPair KEYS = RecordKeys.generate();
    private static final byte[] ARTIFACT = "verified output bytes\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    @TempDir
    Path temporary;

    @Test
    void exportsTheSignedTranscriptAndEveryReferencedArtifactAsVerifiedZipEntries() throws Exception {
        Fixture fixture = fixture();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        StarterRecordExport.writeSnapshot(fixture.snapshot(), fixture.trustBytes(),
                fixture.evidence(), output);

        Map<String, byte[]> entries = unzip(output.toByteArray());
        assertThat(entries).containsKeys("record.binpb", "trust.binpb",
                "artifacts/" + fixture.transcriptDigest(),
                "artifacts/" + fixture.artifactDigest());
        assertThat(entries.get("record.binpb")).containsExactly(fixture.recordBytes());
        assertThat(entries.get("trust.binpb")).containsExactly(fixture.trustBytes());
        assertThat(entries.get("artifacts/" + fixture.transcriptDigest()))
                .containsExactly(fixture.transcriptBytes());
        assertThat(entries.get("artifacts/" + fixture.artifactDigest()))
                .containsExactly(ARTIFACT);
    }

    @Test
    void missingArtifactFailsBeforeWritingAnyOutput() throws Exception {
        Fixture fixture = fixture();
        Files.delete(fixture.evidence().resolve(fixture.artifactDigest()));
        assertRefusedWithoutOutput(fixture.snapshot(), fixture.trustBytes(), fixture.evidence());
    }

    @Test
    void tamperedArtifactFailsBeforeWritingAnyOutput() throws Exception {
        Fixture fixture = fixture();
        Files.write(fixture.evidence().resolve(fixture.artifactDigest()),
                "different bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertRefusedWithoutOutput(fixture.snapshot(), fixture.trustBytes(), fixture.evidence());
    }

    @Test
    void aTranscriptDifferentFromTheSignedReferenceFailsBeforeWritingAnyOutput() throws Exception {
        Fixture fixture = fixture();
        ObjectNode altered = altered(fixture.snapshot());
        byte[] changed = java.util.Arrays.copyOf(fixture.transcriptBytes(),
                fixture.transcriptBytes().length + 1);
        changed[changed.length - 1] = 1;
        altered.put("transcriptBase64", Base64.getEncoder().encodeToString(changed));
        assertRefusedWithoutOutput(altered, fixture.trustBytes(), fixture.evidence());
    }

    @Test
    void invalidSignatureFailsBeforeWritingAnyOutput() throws Exception {
        Fixture fixture = fixture();
        SignedWorkRecord signed = SignedWorkRecord.parseFrom(fixture.recordBytes());
        RecordSignature signature = signed.getSignatures(0);
        byte[] damaged = signature.getSignature().toByteArray();
        damaged[0] ^= 1;
        byte[] invalidRecord = signed.toBuilder()
                .setSignatures(0, signature.toBuilder()
                        .setSignature(ByteString.copyFrom(damaged)).build())
                .build().toByteArray();
        JsonNode changedSnapshot = altered(fixture.snapshot())
                .put("recordBase64", Base64.getEncoder().encodeToString(invalidRecord));
        assertRefusedWithoutOutput(changedSnapshot, fixture.trustBytes(), fixture.evidence());
    }

    @Test
    void symlinkedArtifactFailsWithoutFollowingTheLinkOrWritingAnyOutput() throws Exception {
        Fixture fixture = fixture();
        Path target = temporary.resolve("outside-evidence");
        Files.write(target, ARTIFACT);
        Files.delete(fixture.evidence().resolve(fixture.artifactDigest()));
        Files.createSymbolicLink(fixture.evidence().resolve(fixture.artifactDigest()), target);
        assertRefusedWithoutOutput(fixture.snapshot(), fixture.trustBytes(), fixture.evidence());
    }

    private Fixture fixture() throws Exception {
        byte[] transcript = transcriptBytes();
        String transcriptDigest = WorkRecords.sha256Hex(transcript);
        String artifactDigest = WorkRecords.sha256Hex(ARTIFACT);
        var manifest = DelegationRecordProjector.project(TASK, lifecycle(artifactDigest),
                new DelegationRecordProjector.Issuance("record-" + TASK, ISSUER, KEY_ID,
                        ISSUED, ""));
        byte[] record = new RecordSigner(KEY_ID, KEYS.getPrivate()).sign(manifest).toByteArray();
        byte[] trust = trust().toByteArray();
        Path evidence = Files.createDirectory(temporary.resolve("evidence-" + System.nanoTime()));
        Files.write(evidence.resolve(artifactDigest), ARTIFACT);
        JsonNode snapshot = JSON.createObjectNode()
                .put("recordBase64", Base64.getEncoder().encodeToString(record))
                .put("transcriptBase64", Base64.getEncoder().encodeToString(transcript));
        assertThat(RecordVerifier.verify(record, trust()).verified()).isTrue();
        return new Fixture(snapshot, trust, evidence, record, transcript,
                transcriptDigest, artifactDigest);
    }

    private static List<TranscriptEntry> lifecycle(String artifactDigest) {
        TaskSpec spec = TaskSpec.newBuilder().setObjective("Export verified evidence")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName(CHECK)
                        .setDescription("the artifact hash is verified"))
                .build();
        CompletionCandidate candidate = CompletionCandidate.newBuilder()
                .setAttempt(1).setRevision(1).setSummary("verified fixture output")
                .addEvidence(CheckEvidence.newBuilder().setCheckName(CHECK)
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED))
                .addArtifacts(ArtifactReference.newBuilder().setSha256(artifactDigest)
                        .setMediaType("text/plain").setSizeBytes(ARTIFACT.length).setRedacted(true))
                .build();
        return List.of(
                coordinator(DelegateResponse.newBuilder().setOffer(TaskOffer.newBuilder()
                        .setAttempt(1).setSpec(spec).setExpiresAt(Timestamp.newBuilder()
                                .setSeconds(1_800_000_000L)))),
                worker(DelegateRequest.newBuilder().setAccept(TaskAccept.newBuilder().setAttempt(1))),
                worker(DelegateRequest.newBuilder().setCompletion(candidate)),
                coordinator(DelegateResponse.newBuilder().setAccepted(CompletionAccepted.newBuilder()
                        .setRevision(1).setVerdict("artifact checked"))));
    }

    private static byte[] transcriptBytes() {
        return WorkRecords.deterministicBytes(Transcript.newBuilder()
                .addAllEntries(lifecycle(WorkRecords.sha256Hex(ARTIFACT))).build());
    }

    private static TranscriptEntry coordinator(DelegateResponse.Builder frame) {
        return TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR)
                .setWorkerId(WORKER).setCoordinatorFrame(frame).build();
    }

    private static TranscriptEntry worker(DelegateRequest.Builder frame) {
        return TranscriptEntry.newBuilder().setLane(Lane.LANE_WORKER)
                .setWorkerId(WORKER).setWorkerFrame(frame).build();
    }

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer(ISSUER).addSubjectKinds("delegation-task")
                .addKeys(TrustedKey.newBuilder().setKeyId(KEY_ID)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(KEYS.getPublic())))
                        .setState(KeyState.KEY_STATE_ACTIVE))).build();
    }

    private static Map<String, byte[]> unzip(byte[] bytes) throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(bytes))) {
            for (var entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                entries.put(entry.getName(), zip.readAllBytes());
            }
        }
        return entries;
    }

    private static void assertRefusedWithoutOutput(JsonNode snapshot, byte[] trust, Path evidence) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertThatThrownBy(() -> StarterRecordExport.writeSnapshot(snapshot, trust, evidence, output))
                .isInstanceOf(Exception.class);
        assertThat(output.size()).as("export bytes are emitted only after full verification")
                .isZero();
    }

    private static ObjectNode altered(JsonNode snapshot) {
        return JSON.valueToTree(snapshot);
    }

    private record Fixture(JsonNode snapshot, byte[] trustBytes, Path evidence,
                           byte[] recordBytes, byte[] transcriptBytes,
                           String transcriptDigest, String artifactDigest) { }

}
