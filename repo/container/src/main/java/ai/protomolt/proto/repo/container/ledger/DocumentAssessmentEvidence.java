package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Scoped borrowing of one replayed assessment. The parent holds every reservation
 * through the callback; getters grant neither SQL authority nor independent byte
 * ownership. A consumer must not retain the returned values beyond that scope.
 */
final class DocumentAssessmentEvidence implements AutoCloseable {
    record Root(String member, int ordinal, String locatorSha256, String fragmentSha256, long fragmentSize,
            String codec, int version, String sha256, ByteString bytes) {}
    private record RootIdentity(String member, int ordinal, String codec, int version, String sha256) {}

    private DocumentPublicationCommand command;
    private DocumentSchemaPolicies.Selection policy;
    private DocumentPublicationAssessment.ObservedManifest manifest;
    private Map<String, ByteString> artifacts;
    private List<Root> roots;

    /** Called only while the parent is busy, after its observed replay completed. */
    DocumentAssessmentEvidence(DocumentPublicationAssessment assessment, RepositoryOperationLedger.Owner owner,
            DocumentPublicationAssessment.ObservedManifest manifest, DocumentAdmissionReservations reservations,
            Runnable control) throws InvalidProtocolBufferException {
        manifest.check(control);
        this.command = assessment.command();
        this.policy = assessment.policy();
        this.manifest = manifest;
        this.artifacts = Collections.unmodifiableMap(new TreeMap<>(assessment.artifacts()));
        var typed = assessment.typed();
        var modes = assessment.modes();
        var projected = DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC,
                DocumentAssessmentManifestCodec.VERSION, manifest.bytes(control), manifest.sha256(control), reservations, control);
        var expected = DocumentAssessmentProjection.project(command, policy, assessment.evaluatedAt(), modes, typed,
                assessment.failure().orElse(null), owner, projected.getRuntime(), control);
        // Compare every scalar/global binding, including exact evaluation time and
        // first failure. Member sets are compared below without relying on order.
        if (!projected.toBuilder().clearMembers().build().equals(expected.toBuilder().clearMembers().build()))
            throw new IllegalArgumentException("Assessment retention identity differs from observed manifest");
        var expectedMembers = expected.getMembersList().stream().collect(java.util.stream.Collectors.toMap(
                ai.protomolt.proto.repo.v1.DocumentMemberAssessment::getMemberId, value -> value));
        var bindings = new ArrayList<Root>();
        for (var member : command.intent().getMembersList()) {
            manifest.check(control);
            var view = typed.get(member.getMemberId());
            if (modes.get(member.getMemberId()) == DocumentPublicationCandidate.Mode.OPAQUE) {
                if (view != null) throw new IllegalArgumentException("Opaque retention member has typed evidence");
                continue;
            }
            if (modes.get(member.getMemberId()) != DocumentPublicationCandidate.Mode.TYPED || view == null)
                throw new IllegalArgumentException("Retention member has no explicit completed assessment");
            for (var root : view.roots()) {
                manifest.check(control);
                var part = member.getParts(root.ordinal());
                long size;
                String sha;
                if (part.hasUpload()) {
                    size = part.getUpload().getSizeBytes(); sha = part.getUpload().getSha256();
                } else if (part.hasReuse()) {
                    size = part.getReuse().getObject().getSizeBytes(); sha = part.getReuse().getObject().getSha256();
                } else throw new IllegalArgumentException("Assessment root requires a present candidate part");
                var encoded = root.encoded();
                bindings.add(new Root(member.getMemberId(), root.ordinal(), root.locatorSha256(), sha, size,
                        encoded.codec(), encoded.version(), encoded.sha256(), encoded.bytes()));
            }
        }
        bindings.sort(Comparator.comparing(Root::member).thenComparingInt(Root::ordinal).thenComparing(Root::locatorSha256));
        roots = List.copyOf(bindings);
        var projectedArtifacts = new HashSet<String>();
        var projectedRoots = new HashSet<RootIdentity>();
        var projectedMembers = new HashSet<String>();
        for (var member : projected.getMembersList()) {
            manifest.check(control);
            projectedMembers.add(member.getMemberId());
            if (!member.hasTyped()) {
                if (!member.getOpaque() || modes.get(member.getMemberId()) != DocumentPublicationCandidate.Mode.OPAQUE)
                    throw new IllegalArgumentException("Retention member mode differs from observed manifest");
                continue;
            }
            if (modes.get(member.getMemberId()) != DocumentPublicationCandidate.Mode.TYPED)
                throw new IllegalArgumentException("Retention member mode differs from observed manifest");
            var expectedTyped = expectedMembers.get(member.getMemberId()).getTyped();
            if (!member.getTyped().getContainer().equals(expectedTyped.getContainer())
                    || member.getTyped().getPayloadSchemasCount() != expectedTyped.getPayloadSchemasCount()
                    || !new HashSet<>(member.getTyped().getPayloadSchemasList()).equals(new HashSet<>(expectedTyped.getPayloadSchemasList())))
                throw new IllegalArgumentException("Retention schema roles differ from observed manifest");
            addArtifacts(member.getTyped().getContainer(), projectedArtifacts);
            for (var asset : member.getTyped().getPayloadSchemasList()) {
                manifest.check(control); addArtifacts(asset, projectedArtifacts);
            }
            for (var root : member.getTyped().getRootsList()) {
                manifest.check(control);
                if (!projectedRoots.add(new RootIdentity(member.getMemberId(), root.getOrdinal(), root.getCodec(), root.getVersion(), root.getSha256())))
                    throw new IllegalArgumentException("Duplicate manifest root identity");
            }
        }
        var retainedRoots = new HashSet<RootIdentity>();
        for (var root : roots) {
            manifest.check(control);
            if (!retainedRoots.add(new RootIdentity(root.member(), root.ordinal(), root.codec(), root.version(), root.sha256())))
                throw new IllegalArgumentException("Duplicate retained root identity");
        }
        if (!projectedMembers.equals(modes.keySet()) || !projectedArtifacts.equals(this.artifacts.keySet())
                || !projectedRoots.equals(retainedRoots))
            throw new IllegalArgumentException("Retained evidence set differs from observed manifest");
        manifest.check(control);
    }

    private static void addArtifacts(RepositorySchemaAssetReference reference, HashSet<String> artifacts) {
        artifacts.add(reference.getDescriptorSha256()); artifacts.add(reference.getMetadataSha256());
        if (reference.hasSourceSha256()) artifacts.add(reference.getSourceSha256());
    }

    synchronized DocumentPublicationCommand command(Runnable control) { check(control); return command; }
    synchronized DocumentSchemaPolicies.Selection policy(Runnable control) { check(control); return policy; }
    synchronized Map<String, ByteString> artifacts(Runnable control) { check(control); return artifacts; }
    synchronized List<Root> roots(Runnable control) { check(control); return roots; }
    synchronized ByteString manifestBytes(Runnable control) {
        check(control); var value = manifest.bytes(control); requireOpen(); return value;
    }
    synchronized String manifestSha256(Runnable control) {
        check(control); var value = manifest.sha256(control); requireOpen(); return value;
    }
    synchronized void check(Runnable control) {
        requireOpen(); manifest.check(control); requireOpen();
    }
    private void requireOpen() {
        if (manifest == null) throw new IllegalStateException("Assessment evidence scope is closed");
    }
    @Override public synchronized void close() {
        // The enclosing scope closes the encoded manifest and releases the parent's
        // busy guard. Drop borrowed byte references even if this handle escapes.
        manifest = null; command = null; policy = null; artifacts = Map.of(); roots = List.of();
    }
}
