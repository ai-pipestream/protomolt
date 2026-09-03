package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.receipt.Disclosure;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;

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
    public Descriptor responseType() {
        return CatalogContract.response("ExportWorkRecordResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        if (runs == null) {
            throw WorkflowRequests.unavailable("work-record export",
                    "start protomolt-serve with --workflow-workspace");
        }
        if (signing == null) {
            throw WorkflowRequests.unavailable("work-record signing",
                    "set " + RecordSigning.ENV_KEY_FILE + ", " + RecordSigning.ENV_KEY_ID
                            + ", and " + RecordSigning.ENV_ISSUER);
        }
        String runId = WorkflowRequests.identity(input, "runId");
        String recordId = WorkflowRequests.optionalIdentity(input, "recordId");
        if (recordId == null) {
            recordId = "record-" + runId;
        }
        String prior = WorkflowRequests.optionalText(input, "priorManifestSha256");
        if (prior != null && !prior.matches("[0-9a-f]{64}")) {
            throw WorkflowRequests.invalid(
                    "'priorManifestSha256' must be a lowercase SHA-256 digest",
                    "/priorManifestSha256");
        }
        List<String> maskClasses = maskClasses(input);
        String discloseOf = WorkflowRequests.optionalText(input, "discloseOf");
        if ((maskClasses == null) != (discloseOf == null)) {
            throw WorkflowRequests.invalid(
                    "a disclosure names both 'maskClasses' and 'discloseOf'",
                    maskClasses == null ? "/maskClasses" : "/discloseOf");
        }
        if (discloseOf != null && !discloseOf.matches("[0-9a-f]{64}")) {
            throw WorkflowRequests.invalid(
                    "'discloseOf' must be a lowercase SHA-256 digest", "/discloseOf");
        }
        RunEvidence evidence;
        try {
            evidence = runs.find(runId).orElseThrow(() ->
                    WorkflowRequests.invalid("No run evidence named '" + runId + "'",
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
                throw WorkflowRequests.invalid(
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
            throw WorkflowRequests.invalid(e.getMessage(), "/runId");
        }
        if (discloseOf != null) {
            manifest = manifest.toBuilder()
                    .setDisclosure(Disclosure.newBuilder()
                            .setSourceManifestSha256(discloseOf)
                            .setPolicy("remove " + String.join(", ", maskClasses)))
                    .build();
        }
        SignedWorkRecord record = signing.signer().sign(manifest);
        return Reply.of(responseType())
                .set("recordBase64", Base64.getEncoder().encodeToString(record.toByteArray()))
                .set("manifestDigest",
                        WorkRecords.sha256Hex(record.getManifest().toByteArray()))
                .set("recordId", recordId)
                .addAll("maskedPaths", maskedPaths)
                .build();
    }

    private static List<String> maskClasses(Message input) throws ActionException {
        List<String> declared = Fields.strings(input, "maskClasses");
        if (declared.isEmpty()) {
            return null;
        }
        List<String> classes = new ArrayList<>();
        for (String entry : declared) {
            if (entry.isBlank()) {
                throw WorkflowRequests.invalid(
                        "'maskClasses' entries must be non-empty strings", "/maskClasses");
            }
            classes.add(entry);
        }
        return classes;
    }
}
