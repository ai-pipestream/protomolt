package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.meta.SensitivityMasker;
import ai.pipestream.proto.receipt.Disclosure;
import ai.pipestream.proto.receipt.SignedWorkRecord;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import com.google.protobuf.Descriptors.Descriptor;

/** Projects a stored run's evidence into a canonical signed work record. */
final class ExportWorkRecordAction implements ProtoAction {

    private final RunEvidenceRepository runs;
    private final RecordSigning signing;
    private final Clock clock;

    ExportWorkRecordAction(RunEvidenceRepository runs, RecordSigning signing) {
        this(runs, signing, Clock.systemUTC());
    }

    ExportWorkRecordAction(RunEvidenceRepository runs, RecordSigning signing, Clock clock) {
        this.runs = runs;
        this.signing = signing;
        this.clock = clock;
    }

    @Override
    public String name() {
        return "export-work-record";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Projects a recorded run's evidence into a canonical signed work record that "
                + "verifies offline: deterministic manifest bytes, a detached Ed25519 issuer "
                + "signature, artifacts by digest, and a signed completeness claim. With "
                + "maskClasses and discloseOf it emits a disclosure projection instead: the "
                + "masker runs over the evidence first and the projection is signed as its "
                + "own whole record carrying the original's digest and the policy.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("ExportWorkRecordRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (runs == null) {
            throw WorkflowActionJson.unavailable("work-record export",
                    "start protomolt-serve with --workflow-workspace");
        }
        if (signing == null) {
            throw WorkflowActionJson.unavailable("work-record signing",
                    "set " + RecordSigning.ENV_KEY_FILE + ", " + RecordSigning.ENV_KEY_ID
                            + ", and " + RecordSigning.ENV_ISSUER);
        }
        String runId = WorkflowActionJson.identity(input, "runId");
        String recordId = WorkflowActionJson.optionalIdentity(input, "recordId");
        if (recordId == null) {
            recordId = "record-" + runId;
        }
        String prior = WorkflowActionJson.optionalText(input, "priorManifestSha256");
        if (prior != null && !prior.matches("[0-9a-f]{64}")) {
            throw WorkflowActionJson.invalid(
                    "'priorManifestSha256' must be a lowercase SHA-256 digest",
                    "/priorManifestSha256");
        }
        List<String> maskClasses = maskClasses(input);
        String discloseOf = WorkflowActionJson.optionalText(input, "discloseOf");
        if ((maskClasses == null) != (discloseOf == null)) {
            throw WorkflowActionJson.invalid(
                    "a disclosure names both 'maskClasses' and 'discloseOf'",
                    maskClasses == null ? "/maskClasses" : "/discloseOf");
        }
        if (discloseOf != null && !discloseOf.matches("[0-9a-f]{64}")) {
            throw WorkflowActionJson.invalid(
                    "'discloseOf' must be a lowercase SHA-256 digest", "/discloseOf");
        }
        RunEvidence evidence;
        try {
            evidence = runs.find(runId).orElseThrow(() ->
                    WorkflowActionJson.invalid("No run evidence named '" + runId + "'",
                            "/runId"));
        } catch (ActionException e) {
            throw e;
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Run evidence read failed: " + e.getMessage());
        }
        List<String> maskedPaths = List.of();
        if (maskClasses != null) {
            SensitivityMasker.MaskResult masked = SensitivityMasker.mask(evidence,
                    Set.copyOf(maskClasses), SensitivityMasker.Strategy.REMOVE);
            if (!masked.unresolvedPaths().isEmpty()) {
                throw WorkflowActionJson.invalid(
                        "cannot disclose evidence with unresolved payload paths: "
                                + masked.unresolvedPaths(), "/maskClasses");
            }
            evidence = (RunEvidence) masked.message();
            maskedPaths = masked.maskedPaths();
        }
        Instant now = clock.instant();
        WorkRecord manifest;
        try {
            manifest = WorkRecordProjector.project(evidence, new WorkRecordProjector.Issuance(
                    recordId, signing.issuer(), signing.signer().keyId(),
                    Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                            .setNanos(now.getNano()).build(),
                    prior == null ? "" : prior));
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/runId");
        }
        if (discloseOf != null) {
            manifest = manifest.toBuilder()
                    .setDisclosure(Disclosure.newBuilder()
                            .setSourceManifestSha256(discloseOf)
                            .setPolicy("remove " + String.join(", ", maskClasses)))
                    .build();
        }
        SignedWorkRecord record = signing.signer().sign(manifest);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("recordBase64",
                Base64.getEncoder().encodeToString(record.toByteArray()));
        output.put("manifestDigest",
                WorkRecords.sha256Hex(record.getManifest().toByteArray()));
        output.put("recordId", recordId);
        if (!maskedPaths.isEmpty()) {
            ArrayNode paths = output.putArray("maskedPaths");
            maskedPaths.forEach(paths::add);
        }
        return output;
    }

    private static List<String> maskClasses(ObjectNode input) throws ActionException {
        JsonNode node = input.get("maskClasses");
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isArray() || node.isEmpty()) {
            throw WorkflowActionJson.invalid(
                    "'maskClasses' must be a non-empty array of sensitivity classes",
                    "/maskClasses");
        }
        List<String> classes = new ArrayList<>();
        for (JsonNode entry : node) {
            if (!entry.isTextual() || entry.asText().isBlank()) {
                throw WorkflowActionJson.invalid(
                        "'maskClasses' entries must be non-empty strings", "/maskClasses");
            }
            classes.add(entry.asText());
        }
        return classes;
    }
}
