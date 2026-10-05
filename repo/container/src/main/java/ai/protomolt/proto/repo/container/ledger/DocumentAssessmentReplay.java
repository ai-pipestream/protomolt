package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.*;

/** Internal current-runtime reproduction, without registry lookup or terminal decision authority. */
final class DocumentAssessmentReplay {
    /** Bounded diagnostic identities, not retained payloads or a publication/review grant. */
    record Result(UUID assessment, String commandSha256, String manifestSha256, long ownerGeneration,
                  DocumentAssessmentRuntime recordedRuntime, DocumentAssessmentRuntime replayRuntime,
                  Optional<DocumentAssessmentFailure> firstFailure) {}
    private DocumentAssessmentReplay() {}

    static Result replay(DocumentReadLedger.PinnedAssessment capture, DocumentAssessmentReader reader,
            PayloadBudget budget, DocumentRevisionAssembly.Limits opaqueLimits,
            DocumentAssessmentRuntimeObserver.Observation observation, RepositoryReadControl control) {
        Objects.requireNonNull(capture); Objects.requireNonNull(reader); Objects.requireNonNull(budget);
        Objects.requireNonNull(opaqueLimits); Objects.requireNonNull(observation); Objects.requireNonNull(control);
        var active = new RepositoryReadControl() {
            @Override public boolean isCancelled() {
                if (Thread.currentThread().isInterrupted()) return true;
                return control.isCancelled() || Thread.currentThread().isInterrupted();
            }
            @Override public long remainingNanos() { return control.remainingNanos(); }
        };
        active.check();
        try (var delivery = capture.use()) {
            try (var inputs = capture.loadReplayInputs(budget, active)) {
                var runtime = observation.identity(active::check);
                var snapshot = inputs.snapshot(); var command = snapshot.command(); var manifest = snapshot.manifest();
                var recorded = new HashMap<String,DocumentMemberAssessment>();
                manifest.getMembersList().forEach(member -> recorded.put(member.getMemberId(), member));
                // Check every mode before provider work, including opaque members without schemas.
                for (var member : command.intent().getMembersList()) {
                    active.check(); var mode = recorded.get(member.getMemberId());
                    if (mode == null || (!mode.hasTyped() && !mode.getOpaque())) throw invalid("Missing retained member mode");
                    if (mode.getOpaque() && snapshot.policy().requiresTyped(member)) throw invalid("Retained policy refuses opaque member");
                }
                var at = Instant.ofEpochSecond(manifest.getEvaluatedAt().getEpochSeconds(), manifest.getEvaluatedAt().getNanos());
                DocumentAssessmentFailure first = null;
                for (var member : command.intent().getMembersList()) {
                    active.check(); var mode = recorded.get(member.getMemberId());
                    var entries = delivery.plan().entries(member.getMemberId());
                    long bytes = 0;
                    for (var entry : entries) bytes = Math.addExact(bytes, entry.part().part().size());
                    // Reserve private protobuf copies before requesting the provider batch.
                    try (var copySpace = reserve(budget, bytes);
                            var batch = reader.readAssessment(capture, member.getMemberId(), active)) {
                        var parts = batch.parts();
                        if (parts.size() != entries.size()) throw invalid("Assessment provider fragment count differs");
                        var fragments = new HashMap<Integer,ByteString>();
                        for (int i = 0; i < entries.size(); i++) {
                            active.check(); var entry = entries.get(i); var part = parts.get(i); var expected = entry.part().part();
                            if (part.part() != expected.part() || !part.subKey().equals(expected.subKey())
                                    || part.bytes().length != expected.size()) throw invalid("Assessment provider slot differs");
                            fragments.put(entry.revisionOrdinal(), ByteString.copyFrom(part.bytes()));
                        }
                        if (mode.getOpaque()) {
                            DocumentCommandContent.check(command, member.getMemberId(), fragments, false, opaqueLimits, active::check);
                            continue;
                        }
                        var typed = mode.getTyped();
                        var evidence = new HashMap<Integer,List<DocumentSchemaAdmission.EncodedEvidence>>();
                        var roots = snapshot.roots().stream().filter(root -> root.member().equals(member.getMemberId())).toList();
                        for (var root : roots) evidence.computeIfAbsent(root.ordinal(), ignored -> new ArrayList<>()).add(root.evidence());
                        var request = new DocumentSchemaAdmission.Request(ByteString.copyFrom(HexFormat.of().parseHex(command.sha256())),
                                snapshot.policy().sha256(), snapshot.policy().definition().getRequireStructuredRoot(), member,
                                Map.copyOf(fragments), evidence, DocumentSchemaAdmission.Reference.fromProto(typed.getContainer(), active::check),
                                typed.getPayloadSchemasList().stream().map(reference -> DocumentSchemaAdmission.Reference.fromProto(reference, active::check)).toList());
                        var failure = DocumentSchemaAssessmentReplay.replay(request, at, snapshot.policy(),
                                hash -> Optional.ofNullable(snapshot.artifacts().get(hash)), amount -> {
                                    var lease = reserve(budget, amount); return lease::close;
                                }, active::check);
                        // Do not stop at the first failure: later members' retained evidence must also reproduce.
                        if (first == null && failure.isPresent()) first = projectFailure(member.getMemberId(), failure.orElseThrow(), roots, budget, active);
                    }
                }
                if (manifest.hasFirstFailure() != (first != null) || (first != null && !first.equals(manifest.getFirstFailure())))
                    throw invalid("Reproduced operation failure differs from retained manifest");
                capture.authorizeDelivery(delivery, active);
                if (!runtime.equals(observation.identity(active::check))) throw invalid("Replay runtime observation changed");
                var stage = delivery.plan().stage();
                return new Result(stage.assessment(), command.sha256(), stage.manifestSha256(), manifest.getOwnerGeneration(),
                        manifest.getRuntime(), runtime, Optional.ofNullable(first));
            } catch (java.util.concurrent.CancellationException cancelled) {
                throw new RepositoryException(RepositoryException.Code.CANCELLED, "Assessment replay cancelled");
            } catch (InvalidProtocolBufferException failure) {
                active.check(); capture.authorizeDelivery(delivery, active);
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Assessment replay input cannot be decoded", failure);
            } catch (RuntimeException failure) {
                if (failure instanceof RepositoryException repository
                        && (repository.code() == RepositoryException.Code.CANCELLED || repository.code() == RepositoryException.Code.DEADLINE_EXCEEDED))
                    throw new RepositoryException(repository.code(), "Assessment replay cancelled or expired");
                active.check(); capture.authorizeDelivery(delivery, active);
                throw failure;
            }
        }
    }

    private static DocumentAssessmentFailure projectFailure(String member, DocumentSchemaAssessment.Failure failure,
            List<DocumentAssessmentReplayInputs.Root> roots, PayloadBudget budget, RepositoryReadControl control)
            throws InvalidProtocolBufferException {
        DocumentAssessmentRootReference selected = null;
        for (var root : roots) {
            control.check();
            if (root.ordinal() != failure.ordinal()) continue;
            var decoded = DocumentSchemaAdmission.decodeRootEvidence(root.ordinal(), root.evidence(), amount -> {
                var lease = reserve(budget, amount); return lease::close;
            }, control::check);
            if (!decoded.locator().equals(failure.root())) continue;
            if (selected != null) throw invalid("Ambiguous replay failure root");
            selected = DocumentAssessmentRootReference.newBuilder().setOrdinal(root.ordinal()).setCodec(root.evidence().codec())
                    .setVersion(root.evidence().version()).setSha256(root.evidence().sha256()).build();
        }
        if (selected == null) throw invalid("Replay failure root is missing");
        return DocumentAssessmentFailure.newBuilder().setMemberId(member).setRoot(selected)
                .setOccurrence(RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addAllSteps(failure.occurrence()))
                .setFieldPath(failure.fieldPath()).setRuleId(failure.ruleId()).setRulePath(failure.rulePath()).build();
    }
    private static PayloadBudget.Lease reserve(PayloadBudget budget, long size) {
        try { return budget.reserve(size); }
        catch (PayloadBudget.CapacityExceededException exhausted) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Assessment replay capacity exhausted");
        }
    }
    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, message);
    }
}
