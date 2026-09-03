package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CommitReference;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.Verification;
import ai.protomolt.proto.workflow.RecordSigning;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The delegation receipt end to end over HTTP: an accepted task's transcript
 * exports as a signed work record that verifies offline against a trust
 * snapshot authorizing the issuer for 'delegation-task', and a task still in
 * flight is refused rather than recorded.
 */
class TaskConsoleRecordTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ISSUER = "records.protomolt.dev";
    private static final String KEY_ID = "key-console-record";
    private static final KeyPair KEYS = RecordKeys.generate();

    private static HttpServer http;
    private static HttpClient client;
    private static String base;
    private static DelegationBridge bridge;
    private static String taskId;

    @BeforeAll
    static void start() throws Exception {
        bridge = new DelegationBridge(new InProcessDelegationCoordinator());
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId("record-worker")
                .setProtocolVersion(1)
                .setProvider("scripted")
                .setModel("recorder")
                .build());
        taskId = UUID.randomUUID().toString();
        bridge.offer("record-worker", taskId, TaskSpec.newBuilder()
                        .setObjective("Earn a receipt")
                        .addRequiredChecks(AcceptanceCheck.newBuilder()
                                .setName("unit-tests")
                                .setDescription("focused tests pass"))
                        .build(),
                Duration.ofMinutes(5), null);
        bridge.accept("record-worker", taskId, 1);

        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/api/tasks", new TaskConsoleApiHandler(bridge,
                TaskConsoleSessions.open(),
                new RecordSigning(ISSUER, new RecordSigner(KEY_ID, KEYS.getPrivate()))));
        http.start();
        base = "http://127.0.0.1:" + http.getAddress().getPort();
        client = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stop() {
        http.stop(0);
        bridge.close();
    }

    @Test
    void anAcceptedTaskHandsOverAReceiptThatVerifiesOffline() throws Exception {
        // Still in flight: nothing terminal to attest yet.
        HttpResponse<String> early = post("/api/tasks/" + taskId + "/record");
        assertThat(early.statusCode()).isEqualTo(409);
        assertThat(early.body()).contains("still in flight");

        bridge.submitCandidate("record-worker", taskId, CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("receipt fixture")
                .addEvidence(CheckEvidence.newBuilder()
                        .setCheckName("unit-tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamps.fromSeconds(1_750_000_000)))
                .addCommits(CommitReference.newBuilder()
                        .setRepository("protomolt")
                        .setCommit("0123456789abcdef0123456789abcdef01234567")
                        .setSubject("earn a receipt"))
                .build());
        bridge.review(taskId, CandidateReviewer.ReviewDecision.accept("evidence holds"));

        HttpResponse<String> exported = post("/api/tasks/" + taskId + "/record");
        assertThat(exported.statusCode()).isEqualTo(200);
        JsonNode body = JSON.readTree(exported.body());
        assertThat(body.path("recordId").asText()).isEqualTo("record-" + taskId);
        byte[] record = Base64.getDecoder().decode(body.path("recordBase64").asText());

        // The relying party's walk: only the record, the snapshot, and math.
        Verification verification = RecordVerifier.verify(record, trust());
        assertThat(verification.verified())
                .as(verification.checks().toString())
                .isTrue();
    }

    private static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer(ISSUER)
                        .addKeys(TrustedKey.newBuilder()
                                .setKeyId(KEY_ID)
                                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                .setPublicKey(ByteString.copyFrom(
                                        RecordKeys.rawPublicKey(KEYS.getPublic())))
                                .setState(KeyState.KEY_STATE_ACTIVE))
                        .addSubjectKinds("delegation-task"))
                .build();
    }

    private static HttpResponse<String> post(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + path))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
