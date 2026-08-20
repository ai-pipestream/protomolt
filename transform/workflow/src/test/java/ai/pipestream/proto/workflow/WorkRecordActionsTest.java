package ai.pipestream.proto.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.RunStatus;
import ai.pipestream.proto.receipt.KeyState;
import ai.pipestream.proto.receipt.RecordKeys;
import ai.pipestream.proto.receipt.RecordSigner;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.SignatureAlgorithm;
import ai.pipestream.proto.receipt.SignedWorkRecord;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustedIssuer;
import ai.pipestream.proto.receipt.TrustedKey;
import ai.pipestream.proto.receipt.Verification;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkRecordActionsTest {

    private static final byte[] SEED = HexFormat.of().parseHex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    private static final byte[] PUBLIC_KEY = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ActionContext CONTEXT = ActionContext.create();

    private final RecordSigning signing = new RecordSigning("records.protomolt.dev",
            new RecordSigner("key-2026", RecordKeys.privateKey(SEED)));
    private final Clock clock =
            Clock.fixed(Instant.ofEpochSecond(1750000000), ZoneOffset.UTC);
    private final StubRuns runs = new StubRuns(
            WorkRecordProjectorTest.evidence().build());

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer("records.protomolt.dev")
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId("key-2026")
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(PUBLIC_KEY))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN))
                .build();
    }

    @Test
    void exportSignsARecordThatVerifies() throws Exception {
        ObjectNode input = MAPPER.createObjectNode().put("runId", "run-1");
        ObjectNode output = new ExportWorkRecordAction(runs, signing, clock)
                .execute(input, CONTEXT);

        assertThat(output.path("recordId").asText()).isEqualTo("record-run-1");
        byte[] record = Base64.getDecoder().decode(output.path("recordBase64").asText());
        Verification verification = RecordVerifier.verify(record, trust());
        assertThat(verification.verified())
                .as("refusal: %s", verification.refusal())
                .isTrue();
        assertThat(verification.manifestDigest())
                .isEqualTo(output.path("manifestDigest").asText());

        WorkRecord manifest = verification.manifest();
        assertThat(manifest.getRecordId()).isEqualTo("record-run-1");
        assertThat(manifest.getIssuer()).isEqualTo("records.protomolt.dev");
        assertThat(manifest.getIssuedAt().getSeconds()).isEqualTo(1750000000);
        assertThat(manifest.getSubject().getRunId()).isEqualTo("run-1");
    }

    @Test
    void exportCarriesTheChosenIdentityAndPriorLink() throws Exception {
        String prior = WorkRecords.sha256Hex("prior".getBytes());
        ObjectNode input = MAPPER.createObjectNode()
                .put("runId", "run-1")
                .put("recordId", "record-reissue")
                .put("priorManifestSha256", prior);
        ObjectNode output = new ExportWorkRecordAction(runs, signing, clock)
                .execute(input, CONTEXT);
        byte[] record = Base64.getDecoder().decode(output.path("recordBase64").asText());
        WorkRecord manifest = WorkRecord.parseFrom(
                SignedWorkRecord.parseFrom(record).getManifest());
        assertThat(manifest.getRecordId()).isEqualTo("record-reissue");
        assertThat(manifest.getPriorManifestSha256()).isEqualTo(prior);
    }

    @Test
    void exportRefusalsNameTheirCause() {
        ObjectNode input = MAPPER.createObjectNode().put("runId", "run-1");
        assertThatThrownBy(() -> new ExportWorkRecordAction(null, signing, clock)
                .execute(input, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("--workflow-workspace");
        assertThatThrownBy(() -> new ExportWorkRecordAction(runs, null, clock)
                .execute(input, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining(RecordSigning.ENV_KEY_FILE);
        assertThatThrownBy(() -> new ExportWorkRecordAction(runs, signing, clock)
                .execute(MAPPER.createObjectNode().put("runId", "run-9"), CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("run-9");
        assertThatThrownBy(() -> new ExportWorkRecordAction(runs, signing, clock)
                .execute(MAPPER.createObjectNode().put("runId", "run-1")
                        .put("priorManifestSha256", "nope"), CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("priorManifestSha256");
    }

    @Test
    void exportOfALiveRunRefuses() {
        StubRuns live = new StubRuns(WorkRecordProjectorTest.evidence()
                .setStatus(RunStatus.RUN_STATUS_RUNNING).build());
        assertThatThrownBy(() -> new ExportWorkRecordAction(live, signing, clock)
                .execute(MAPPER.createObjectNode().put("runId", "run-1"), CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("only terminal run evidence");
    }

    @Test
    void verifyReportsChecksAndNonClaims() throws Exception {
        ObjectNode exported = new ExportWorkRecordAction(runs, signing, clock)
                .execute(MAPPER.createObjectNode().put("runId", "run-1"), CONTEXT);
        ObjectNode input = MAPPER.createObjectNode()
                .put("recordBase64", exported.path("recordBase64").asText());
        input.set("trust", (ObjectNode) MAPPER.readTree(
                JsonFormat.printer().print(trust())));

        ObjectNode output = new VerifyWorkRecordAction().execute(input, CONTEXT);
        assertThat(output.path("verified").asBoolean()).isTrue();
        assertThat(output.path("manifestDigest").asText())
                .isEqualTo(exported.path("manifestDigest").asText());
        assertThat(output.path("checks")).hasSize(8);
        assertThat(output.path("checks").get(7).path("status").asText())
                .isEqualTo("SKIPPED");
        assertThat(output.path("nonClaims").toString())
                .contains(Verification.NON_CLAIM_ARTIFACT_CUSTODY);
    }

    @Test
    void aDisclosureMasksBeforeProjectingAndSignsItsOwnRecord() throws Exception {
        String source = WorkRecords.sha256Hex("original-manifest".getBytes());
        ObjectNode input = MAPPER.createObjectNode()
                .put("runId", "run-1")
                .put("recordId", "record-disclosed")
                .put("discloseOf", source);
        input.putArray("maskClasses").add("internal");
        ObjectNode output = new ExportWorkRecordAction(runs, signing, clock)
                .execute(input, CONTEXT);

        assertThat(output.path("maskedPaths").toString()).contains("structured");
        byte[] record = Base64.getDecoder().decode(output.path("recordBase64").asText());
        Verification verification = RecordVerifier.verify(record, trust());
        assertThat(verification.verified())
                .as("refusal: %s", verification.refusal())
                .isTrue();

        WorkRecord manifest = verification.manifest();
        assertThat(manifest.getDisclosure().getSourceManifestSha256()).isEqualTo(source);
        assertThat(manifest.getDisclosure().getPolicy()).isEqualTo("remove internal");
        assertThat(manifest.getSteps(1).getModel())
                .as("model identity rides the masked structured evidence")
                .isEmpty();
        assertThat(manifest.getSteps(1).getPromptTokens()).isZero();
    }

    @Test
    void aDisclosureNamesItsClassesAndSourceTogether() {
        ObjectNode missingSource = MAPPER.createObjectNode().put("runId", "run-1");
        missingSource.putArray("maskClasses").add("internal");
        assertThatThrownBy(() -> new ExportWorkRecordAction(runs, signing, clock)
                .execute(missingSource, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("discloseOf");

        ObjectNode missingClasses = MAPPER.createObjectNode()
                .put("runId", "run-1")
                .put("discloseOf", WorkRecords.sha256Hex("original".getBytes()));
        assertThatThrownBy(() -> new ExportWorkRecordAction(runs, signing, clock)
                .execute(missingClasses, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("maskClasses");

        ObjectNode emptyClasses = MAPPER.createObjectNode()
                .put("runId", "run-1")
                .put("discloseOf", WorkRecords.sha256Hex("original".getBytes()));
        emptyClasses.putArray("maskClasses");
        assertThatThrownBy(() -> new ExportWorkRecordAction(runs, signing, clock)
                .execute(emptyClasses, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("non-empty");
    }

    @Test
    void verifyRehashesWhenArtifactBytesAreSupplied() throws Exception {
        ObjectNode exported = new ExportWorkRecordAction(runs, signing, clock)
                .execute(MAPPER.createObjectNode().put("runId", "run-1"), CONTEXT);
        ObjectNode input = MAPPER.createObjectNode()
                .put("recordBase64", exported.path("recordBase64").asText());
        input.set("trust", (ObjectNode) MAPPER.readTree(
                JsonFormat.printer().print(trust())));
        ObjectNode artifacts = input.putObject("artifacts");
        for (String content : new String[] {"input", "output", "request", "response"}) {
            artifacts.put(WorkRecords.sha256Hex(content.getBytes()),
                    Base64.getEncoder().encodeToString(content.getBytes()));
        }

        ObjectNode output = new VerifyWorkRecordAction().execute(input, CONTEXT);
        assertThat(output.path("verified").asBoolean()).isTrue();
        JsonNode last = output.path("checks").get(output.path("checks").size() - 1);
        assertThat(last.path("id").asText())
                .isEqualTo(RecordVerifier.CHECK_ARTIFACT_REHASH);
        assertThat(last.path("status").asText()).isEqualTo("PASSED");
        assertThat(output.path("nonClaims").toString())
                .doesNotContain(Verification.NON_CLAIM_ARTIFACT_CUSTODY);

        artifacts.put(WorkRecords.sha256Hex("output".getBytes()),
                Base64.getEncoder().encodeToString("outpud".getBytes()));
        ObjectNode refused = new VerifyWorkRecordAction().execute(input, CONTEXT);
        assertThat(refused.path("verified").asBoolean()).isFalse();
        JsonNode failed = refused.path("checks").get(refused.path("checks").size() - 1);
        assertThat(failed.path("id").asText())
                .isEqualTo(RecordVerifier.CHECK_ARTIFACT_REHASH);
        assertThat(failed.path("status").asText()).isEqualTo("FAILED");
    }

    @Test
    void verifyRefusesATamperedRecordByName() throws Exception {
        ObjectNode exported = new ExportWorkRecordAction(runs, signing, clock)
                .execute(MAPPER.createObjectNode().put("runId", "run-1"), CONTEXT);
        byte[] record = Base64.getDecoder()
                .decode(exported.path("recordBase64").asText());
        record[record.length - 1] ^= 0x01;
        ObjectNode input = MAPPER.createObjectNode()
                .put("recordBase64", Base64.getEncoder().encodeToString(record));
        input.set("trust", (ObjectNode) MAPPER.readTree(
                JsonFormat.printer().print(trust())));

        ObjectNode output = new VerifyWorkRecordAction().execute(input, CONTEXT);
        assertThat(output.path("verified").asBoolean()).isFalse();
        JsonNode last = output.path("checks").get(output.path("checks").size() - 1);
        assertThat(last.path("status").asText()).isEqualTo("FAILED");
    }

    @Test
    void verifyRefusesBadInputsByPointer() {
        ObjectNode badBase64 = MAPPER.createObjectNode()
                .put("recordBase64", "!!!");
        badBase64.set("trust", MAPPER.createObjectNode());
        assertThatThrownBy(() -> new VerifyWorkRecordAction()
                .execute(badBase64, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("base64");

        ObjectNode emptyTrust = MAPPER.createObjectNode()
                .put("recordBase64", Base64.getEncoder().encodeToString(new byte[] {1}));
        emptyTrust.set("trust", MAPPER.createObjectNode());
        assertThatThrownBy(() -> new VerifyWorkRecordAction()
                .execute(emptyTrust, CONTEXT))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("trust snapshot is invalid");
    }

    @Test
    void signingResolvesFromTheEnvironmentAllOrNothing(@TempDir Path directory)
            throws Exception {
        assertThat(RecordSigning.fromEnvironment(Map.of())).isNull();

        assertThatThrownBy(() -> RecordSigning.fromEnvironment(
                Map.of(RecordSigning.ENV_KEY_ID, "key-2026")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RecordSigning.ENV_KEY_FILE)
                .hasMessageContaining(RecordSigning.ENV_ISSUER);

        Path keyFile = directory.resolve("receipt.key");
        Files.write(keyFile, SEED);
        RecordSigning resolved = RecordSigning.fromEnvironment(Map.of(
                RecordSigning.ENV_KEY_FILE, keyFile.toString(),
                RecordSigning.ENV_KEY_ID, "key-2026",
                RecordSigning.ENV_ISSUER, "records.protomolt.dev"));
        assertThat(resolved).isNotNull();
        assertThat(resolved.issuer()).isEqualTo("records.protomolt.dev");
        assertThat(resolved.signer().keyId()).isEqualTo("key-2026");

        Path shortKey = directory.resolve("short.key");
        Files.write(shortKey, new byte[16]);
        assertThatThrownBy(() -> RecordSigning.fromEnvironment(Map.of(
                RecordSigning.ENV_KEY_FILE, shortKey.toString(),
                RecordSigning.ENV_KEY_ID, "key-2026",
                RecordSigning.ENV_ISSUER, "records.protomolt.dev")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
    }

    /** In-memory evidence store keyed by run id. */
    private static final class StubRuns implements RunEvidenceRepository {
        private final Map<String, RunEvidence> store = new HashMap<>();

        private StubRuns(RunEvidence... evidence) {
            for (RunEvidence run : evidence) {
                store.put(run.getRunId(), run);
            }
        }

        @Override
        public Optional<RunEvidence> find(String runId) {
            return Optional.ofNullable(store.get(runId));
        }

        @Override
        public List<RunEvidence> list(String workflowName, int limit) {
            return List.copyOf(store.values());
        }

        @Override
        public void save(RunEvidence evidence) throws IOException {
            throw new IOException("read-only");
        }
    }
}
