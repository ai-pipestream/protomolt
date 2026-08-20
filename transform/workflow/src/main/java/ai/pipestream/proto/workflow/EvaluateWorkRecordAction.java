package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.receipt.RecordVerifier;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.Verification;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The relying-party evaluation sidecar: a reproducible decision procedure
 * over a signed record — verification, the evidence match, and offline
 * replay — under the predeclared policy. The evaluation is written beside
 * the record; the signed record is never modified.
 */
final class EvaluateWorkRecordAction implements ProtoAction {

    /** The evaluation policy's identity. */
    static final String POLICY_ID = "record-evaluation";

    /** The evaluation policy's version. */
    static final String POLICY_VERSION = "1";

    /**
     * The predeclared decision procedure. The policy digest on every
     * evaluation is the SHA-256 of exactly this text.
     */
    static final String POLICY = """
            An evaluation accepts a work record exactly when every \
            verification check passes against the supplied trust snapshot; \
            the record is byte-identical to the projection of the stored run \
            evidence it names (a disclosure projection is exempt, carrying \
            its source's digest instead); and the recorded run replays \
            offline against the supplied workflow and descriptors without \
            drift. Any failed check rejects; a skipped check never accepts \
            on its own.""";

    /** The record reprojects byte-identically from the stored evidence. */
    static final String CHECK_RECORD_MATCHES_EVIDENCE = "record-matches-evidence";

    /** The recorded run replays offline without drift. */
    static final String CHECK_REPLAY = "replay";

    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;
    private final Clock clock;
    private final TrustSnapshot defaultTrust;

    EvaluateWorkRecordAction(ArtifactRepository artifacts, RunEvidenceRepository runs) {
        this(artifacts, runs, Clock.systemUTC(), null);
    }

    /** With a pinned snapshot, requests may omit {@code trust}; a supplied one wins. */
    EvaluateWorkRecordAction(ArtifactRepository artifacts, RunEvidenceRepository runs,
                             TrustSnapshot defaultTrust) {
        this(artifacts, runs, Clock.systemUTC(), defaultTrust);
    }

    EvaluateWorkRecordAction(ArtifactRepository artifacts, RunEvidenceRepository runs,
                             Clock clock) {
        this(artifacts, runs, clock, null);
    }

    EvaluateWorkRecordAction(ArtifactRepository artifacts, RunEvidenceRepository runs,
                             Clock clock, TrustSnapshot defaultTrust) {
        this.artifacts = artifacts;
        this.runs = runs;
        this.clock = clock;
        this.defaultTrust = defaultTrust;
    }

    static String policySha256() {
        return WorkRecords.sha256Hex(POLICY.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String name() {
        return "evaluate-work-record";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Evaluates a signed work record beside its stored evidence under the "
                + "predeclared record-evaluation policy: offline verification against the "
                + "supplied trust snapshot, a byte-identical reprojection of the stored run "
                + "evidence, and offline replay of the recorded run. Returns a versioned "
                + "evaluation record; the signed record is never modified.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = WorkflowActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("recordBase64").put("type", "string")
                .put("description", "Serialized SignedWorkRecord, base64.");
        properties.putObject("trust").put("type", "object")
                .put("description", defaultTrust == null
                        ? "TrustSnapshot encoded as protobuf JSON."
                        : "TrustSnapshot encoded as protobuf JSON; defaults to the "
                                + "server's pinned snapshot when omitted.");
        properties.putObject("workflow").put("type", "object")
                .put("description", "The workflow the run executed, for offline replay.");
        properties.putObject("schema").put("type", "object")
                .put("description", "The exact descriptors used by the recorded run.");
        ObjectNode artifactBytes = properties.putObject("artifacts");
        artifactBytes.put("type", "object");
        artifactBytes.putObject("additionalProperties").put("type", "string");
        artifactBytes.put("description",
                "Referenced artifact bytes by SHA-256, base64; runs the rehash check.");
        var required = schema.putArray("required").add("recordBase64");
        if (defaultTrust == null) {
            required.add("trust");
        }
        required.add("workflow").add("schema");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (artifacts == null || runs == null) {
            throw WorkflowActionJson.unavailable("work-record evaluation",
                    "start protomolt-serve with --workflow-workspace");
        }
        byte[] record;
        try {
            record = Base64.getDecoder()
                    .decode(WorkflowActionJson.text(input, "recordBase64"));
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid("'recordBase64' is not valid base64",
                    "/recordBase64");
        }
        TrustSnapshot trust = TrustPin.resolve(input, defaultTrust);
        Workflow workflow = (Workflow) WorkflowActionJson.parse(
                WorkflowActionJson.object(input, "workflow"), Workflow.newBuilder(),
                "/workflow");
        Map<String, byte[]> artifactBytes = WorkflowActionJson.base64Map(input, "artifacts");

        Verification verification;
        try {
            verification = RecordVerifier.verify(record, trust, artifactBytes);
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/trust");
        }

        List<Verification.Check> evaluationChecks = new ArrayList<>();
        List<WorkflowReplay.StepReplay> replaySteps = List.of();
        if (!verification.verified()) {
            evaluationChecks.add(new Verification.Check(CHECK_RECORD_MATCHES_EVIDENCE,
                    Verification.Check.Status.SKIPPED, "verification failed"));
            evaluationChecks.add(new Verification.Check(CHECK_REPLAY,
                    Verification.Check.Status.SKIPPED, "verification failed"));
        } else {
            WorkRecord manifest = verification.manifest();
            Optional<RunEvidence> evidence = findEvidence(manifest.getSubject().getRunId());
            if (evidence.isEmpty()) {
                evaluationChecks.add(new Verification.Check(CHECK_RECORD_MATCHES_EVIDENCE,
                        Verification.Check.Status.FAILED, "no run evidence named '"
                                + manifest.getSubject().getRunId() + "'"));
                evaluationChecks.add(new Verification.Check(CHECK_REPLAY,
                        Verification.Check.Status.SKIPPED, "no evidence to replay"));
            } else {
                evaluationChecks.add(matches(manifest, evidence.get(),
                        verification.manifestDigest()));
                WorkflowReplay.ReplayResult replay =
                        replay(workflow, evidence.get(), input, context);
                replaySteps = replay.steps();
                evaluationChecks.add(new Verification.Check(CHECK_REPLAY,
                        replay.ok() ? Verification.Check.Status.PASSED
                                : Verification.Check.Status.FAILED,
                        replay.ok() ? replay.steps().size() + " step(s) replayed clean"
                                : replay.failure()));
            }
        }
        boolean accepted = verification.verified() && evaluationChecks.stream()
                .noneMatch(check -> check.status() == Verification.Check.Status.FAILED);

        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("accepted", accepted);
        if (!verification.manifestDigest().isEmpty()) {
            output.put("manifestDigest", verification.manifestDigest());
        }
        output.put("policyId", POLICY_ID);
        output.put("policyVersion", POLICY_VERSION);
        output.put("policySha256", policySha256());
        output.put("evaluatedAt", clock.instant().toString());
        ArrayNode checks = output.putArray("checks");
        for (Verification.Check check : verification.checks()) {
            render(checks, check);
        }
        for (Verification.Check check : evaluationChecks) {
            render(checks, check);
        }
        ArrayNode steps = output.putArray("replaySteps");
        for (WorkflowReplay.StepReplay step : replaySteps) {
            ObjectNode node = steps.addObject();
            node.put("stepName", step.stepName());
            node.put("recordedStatus", step.recordedStatus().name());
            node.put("ok", step.ok());
            node.put("detail", step.detail());
        }
        ArrayNode nonClaims = output.putArray("nonClaims");
        verification.nonClaims().forEach(nonClaims::add);
        return output;
    }

    private Optional<RunEvidence> findEvidence(String runId) throws ActionException {
        try {
            return runs.find(runId);
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Run evidence read failed: " + e.getMessage());
        }
    }

    private static Verification.Check matches(WorkRecord manifest, RunEvidence evidence,
                                              String manifestDigest) {
        if (manifest.hasDisclosure()) {
            return new Verification.Check(CHECK_RECORD_MATCHES_EVIDENCE,
                    Verification.Check.Status.SKIPPED,
                    "disclosure projection; the source record carries the full evidence");
        }
        WorkRecord reprojected;
        try {
            reprojected = WorkRecordProjector.project(evidence,
                    new WorkRecordProjector.Issuance(manifest.getRecordId(),
                            manifest.getIssuer(), manifest.getKeyId(),
                            manifest.getIssuedAt(), manifest.getPriorManifestSha256()));
        } catch (IllegalArgumentException e) {
            return new Verification.Check(CHECK_RECORD_MATCHES_EVIDENCE,
                    Verification.Check.Status.FAILED, e.getMessage());
        }
        String digest = WorkRecords.sha256Hex(WorkRecords.canonicalBytes(reprojected));
        if (!digest.equals(manifestDigest)) {
            return new Verification.Check(CHECK_RECORD_MATCHES_EVIDENCE,
                    Verification.Check.Status.FAILED,
                    "the record is not the projection of the stored evidence");
        }
        return new Verification.Check(CHECK_RECORD_MATCHES_EVIDENCE,
                Verification.Check.Status.PASSED, "reprojection is byte-identical");
    }

    private WorkflowReplay.ReplayResult replay(Workflow workflow, RunEvidence evidence,
                                               ObjectNode input, ActionContext context)
            throws ActionException {
        try {
            return WorkflowReplay.replay(workflow, evidence,
                    SchemaResolver.resolve(input, "schema", context).files(), artifacts);
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/workflow");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Replay failed: " + e.getMessage());
        }
    }

    private static void render(ArrayNode checks, Verification.Check check) {
        ObjectNode node = checks.addObject();
        node.put("id", check.id());
        node.put("status", check.status().name());
        node.put("detail", check.detail());
    }
}
