package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAssessment;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentAssessmentFailure;
import ai.protomolt.proto.repo.v1.DocumentAssessmentInstant;
import ai.protomolt.proto.repo.v1.DocumentAssessmentRootReference;
import ai.protomolt.proto.repo.v1.DocumentAssessmentRuntime;
import ai.protomolt.proto.repo.v1.DocumentMemberAssessment;
import ai.protomolt.proto.repo.v1.DocumentPublicationAssessmentManifest;
import ai.protomolt.proto.repo.v1.DocumentTypedAssessment;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Projection of borrowed completed assessments. The caller protects their lifetime and verifies replay. */
final class DocumentAssessmentProjection {
    private DocumentAssessmentProjection() {}
    static DocumentPublicationAssessmentManifest project(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Instant evaluatedAt, Map<String, DocumentPublicationCandidate.Mode> modes, Map<String, DocumentSchemaAssessment.View> typed,
            DocumentPublicationAssessment.MemberFailure failure, RepositoryOperationLedger.Owner owner,
            DocumentAssessmentRuntime runtime, Runnable control) {
        Objects.requireNonNull(owner); Objects.requireNonNull(runtime); control.run();
        if (!owner.key().account().equals(command.intent().getAccountId()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Assessment owner differs from command scope");
        var manifest = DocumentPublicationAssessmentManifest.newBuilder().setEncodingVersion(1)
                .setOperationId(command.operationId().toString()).setAccountId(owner.key().account()).setPrincipal(owner.key().principal())
                .setOwnerGeneration(owner.generation()).setCommandSha256(command.sha256())
                .setCommandCodec(DocumentPublicationCommand.CODEC).setCommandVersion(DocumentPublicationCommand.ENCODING_VERSION)
                .setPolicyRevision(policy.revision()).setPolicySha256(policy.policy().sha256())
                .setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(evaluatedAt.getEpochSecond()).setNanos(evaluatedAt.getNano()))
                .setRuntime(runtime);
        for (var member : command.intent().getMembersList()) {
            control.run();
            var id = member.getMemberId();
            var target = DocumentMemberAssessment.newBuilder().setMemberId(id);
            if (modes.get(id) == DocumentPublicationCandidate.Mode.OPAQUE) target.setOpaque(true);
            else if (modes.get(id) == DocumentPublicationCandidate.Mode.TYPED) {
                var view = Objects.requireNonNull(typed.get(id), "Missing typed assessment");
                var references = view.references();
                var result = DocumentTypedAssessment.newBuilder().setContainer(references.getFirst().toProto());
                for (int i = 1; i < references.size(); i++) {
                    control.run(); result.addPayloadSchemas(references.get(i).toProto());
                }
                DocumentAssessmentRootReference failureRoot = null;
                for (var root : view.roots()) {
                    control.run();
                    var reference = DocumentAssessmentRootReference.newBuilder().setOrdinal(root.ordinal())
                            .setCodec(root.encoded().codec()).setVersion(root.encoded().version()).setSha256(root.encoded().sha256()).build();
                    result.addRoots(reference);
                    if (failure != null && failure.member().equals(id) && failure.failure().ordinal() == root.ordinal()
                            && failure.failure().root().equals(root.locator())) {
                        if (failureRoot != null) throw new IllegalArgumentException("Ambiguous assessment failure root");
                        failureRoot = reference;
                    }
                }
                if (failure != null && failure.member().equals(id)) {
                    if (failureRoot == null) throw new IllegalArgumentException("Missing assessment failure root");
                    var value = failure.failure();
                    manifest.setFirstFailure(DocumentAssessmentFailure.newBuilder().setMemberId(id).setRoot(failureRoot)
                            .setOccurrence(RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addAllSteps(value.occurrence()))
                            .setFieldPath(value.fieldPath()).setRuleId(value.ruleId()).setRulePath(value.rulePath()));
                }
                target.setTyped(result);
            } else throw new IllegalArgumentException("Missing explicit assessment member mode");
            manifest.addMembers(target);
        }
        if ((failure != null) != manifest.hasFirstFailure()) throw new IllegalArgumentException("Unbound assessment failure");
        control.run();
        return manifest.build();
    }
}
