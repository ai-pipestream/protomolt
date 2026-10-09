package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentAssessmentRootReference;
import ai.protomolt.proto.repo.v1.DocumentPublicationAssessmentManifest;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

/** Bounded canonical identity, not asset retention, observed runtime identity or a validation verdict. */
public final class DocumentAssessmentManifestCodec {
    public static final String CODEC = "document-publication-assessment";
    public static final int VERSION = 1;
    public static final int MAX_BYTES = DocumentSchemaEvidenceCodec.MAX_BYTES;
    private DocumentAssessmentManifestCodec() {}

    /** Bytes are borrowed while open; parsed inputs and decoded heap remain caller-owned. */
    public static final class Encoded implements AutoCloseable {
        private final DocumentSchemaEvidenceCodec.OwnedEncoded owner;
        private Encoded(DocumentSchemaEvidenceCodec.OwnedEncoded owner) { this.owner = owner; }
        public ByteString bytes() { return owner.value().bytes(); }
        public String sha256() { return owner.value().sha256(); }
        @Override public void close() { owner.close(); }
    }

    /** Normalize set order only; duplicates and conflicting associations are errors. */
    public static Encoded encode(DocumentPublicationAssessmentManifest value,
            DocumentAdmissionReservations reservations, Runnable control) {
        Objects.requireNonNull(reservations);
        var canonical = canonical(value, control);
        var owned = DocumentSchemaEvidenceCodec.encodeOwned(canonical, reservations, control);
        try { return new Encoded(owned); }
        catch (RuntimeException | Error failed) { owned.close(); throw failed; }
    }

    /** Checks wire bounds before parse, exact digest, and canonical wire and set order. */
    public static DocumentPublicationAssessmentManifest decode(String codec, int version, ByteString bytes, String sha256,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        var parsed = DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                DocumentPublicationAssessmentManifest.getDescriptor(), DocumentPublicationAssessmentManifest.parser(),
                Objects.requireNonNull(reservations), control);
        if (!canonical(parsed, control).equals(parsed))
            throw new IllegalArgumentException("noncanonical assessment manifest ordering");
        return parsed;
    }

    private record Schema(String url, String descriptors) {}
    private record Root(int ordinal, String sha256) {}
    private static DocumentPublicationAssessmentManifest canonical(DocumentPublicationAssessmentManifest input, Runnable callerControl) {
        Objects.requireNonNull(callerControl);
        Runnable control = () -> {
            if (Thread.currentThread().isInterrupted())
                throw new java.util.concurrent.CancellationException("assessment manifest interrupted");
            callerControl.run();
        };
        // Bound work and reject unknown fields before allocating list copies or comparing keys.
        DocumentSchemaEvidenceCodec.measureAndValidate(Objects.requireNonNull(input), control);
        var runtime = input.getRuntime().toBuilder();
        var implementations = new ArrayList<>(runtime.getImplementationArtifactsList());
        var names = new HashSet<String>();
        for (var artifact : implementations) {
            control.run();
            if (!names.add(artifact.getName())) throw new IllegalArgumentException("duplicate assessment runtime artifact name");
        }
        implementations.sort((a, b) -> compare(a.getName(), b.getName(), control));
        runtime.clearImplementationArtifacts().addAllImplementationArtifacts(implementations);
        var members = new ArrayList<>(input.getMembersList());
        var ids = new HashSet<String>();
        var artifacts = new HashSet<String>();
        int roots = 0;
        for (int i = 0; i < members.size(); i++) {
            control.run();
            var member = members.get(i);
            if (!ids.add(member.getMemberId())) throw new IllegalArgumentException("duplicate assessment member");
            if (!member.hasTyped()) continue;
            var typed = member.getTyped().toBuilder();
            var schemas = new HashSet<Schema>();
            add(typed.getContainer(), schemas, artifacts);
            var payloads = new ArrayList<>(typed.getPayloadSchemasList());
            for (var schema : payloads) { control.run(); add(schema, schemas, artifacts); }
            payloads.sort((a, b) -> {
                int url = compare(a.getTypeUrl(), b.getTypeUrl(), control);
                return url != 0 ? url : compare(a.getDescriptorSha256(), b.getDescriptorSha256(), control);
            });
            typed.clearPayloadSchemas().addAllPayloadSchemas(payloads);
            if (typed.getRootsCount() > 4096 - roots) throw new IllegalArgumentException("assessment operation root bound exceeded");
            roots += typed.getRootsCount();
            var rootRefs = new ArrayList<>(typed.getRootsList());
            var unique = new HashSet<Root>();
            for (var root : rootRefs) {
                control.run();
                if (!unique.add(new Root(root.getOrdinal(), root.getSha256())))
                    throw new IllegalArgumentException("duplicate assessment root reference");
            }
            rootRefs.sort((a, b) -> compareRoots(a, b, control));
            typed.clearRoots().addAllRoots(rootRefs);
            members.set(i, member.toBuilder().setTyped(typed).build());
        }
        members.sort((a, b) -> compare(a.getMemberId(), b.getMemberId(), control));
        control.run();
        return input.toBuilder().setRuntime(runtime).clearMembers().addAllMembers(members).build();
    }

    private static void add(RepositorySchemaAssetReference reference, HashSet<Schema> schemas, HashSet<String> artifacts) {
        if (!schemas.add(new Schema(reference.getTypeUrl(), reference.getDescriptorSha256())))
            throw new IllegalArgumentException("duplicate assessment schema association");
        artifacts.add(reference.getDescriptorSha256()); artifacts.add(reference.getMetadataSha256());
        if (reference.hasSourceSha256()) artifacts.add(reference.getSourceSha256());
        if (artifacts.size() > 64) throw new IllegalArgumentException("assessment schema artifact bound exceeded");
    }
    private static int compareRoots(DocumentAssessmentRootReference a, DocumentAssessmentRootReference b, Runnable control) {
        control.run();
        int ordinal = Integer.compare(a.getOrdinal(), b.getOrdinal());
        return ordinal != 0 ? ordinal : compare(a.getSha256(), b.getSha256(), control);
    }
    private static int compare(String a, String b, Runnable control) {
        return DocumentSchemaEvidenceCodec.compareUtf8Keys(a, b, control);
    }
}
