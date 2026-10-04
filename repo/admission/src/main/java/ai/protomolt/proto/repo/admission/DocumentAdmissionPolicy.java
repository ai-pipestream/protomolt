package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicy;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicyMode;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable policy content and library checks. This does not select the active
 * account policy, authenticate its author, or fence policy changes at commit.
 * The repository host must obtain this snapshot from its authoritative catalog.
 */
public final class DocumentAdmissionPolicy {
    public static final String CODEC = "document-schema-policy";
    public static final int VERSION = 1;
    public static final int MAX_BYTES = 512 * 1024;
    private record Eligible(String url, PublicationSchemaCondition schema) {}
    private final DocumentSchemaPolicy policy;
    private final ByteString bytes;
    private final String sha256;
    private final DocumentSchemaAdmission.Limits limits;
    private final Set<Eligible> allowed;

    private DocumentAdmissionPolicy(DocumentSchemaPolicy policy, ByteString bytes, String sha256) {
        this.policy = policy; this.bytes = bytes; this.sha256 = sha256;
        var configured = policy.getLimits();
        limits = new DocumentSchemaAdmission.Limits(configured.getMaxFragments(), configured.getMaxFragmentBytes(),
                configured.getMaxRoots(), configured.getMaxEvidenceBytes(), configured.getMaxBindings(),
                configured.getMaxRetainedBytes(), configured.getMaxDecodedBytes());
        var eligible = new HashSet<Eligible>();
        for (var entry : policy.getAllowedSchemas().getBindingsList()) eligible.add(new Eligible(entry.getTypeUrl(), entry.getSchema()));
        allowed = Set.copyOf(eligible);
    }

    /** Normalize the set order; refuse duplicate, unknown, unsupported or invalid policy content. */
    public static DocumentAdmissionPolicy of(DocumentSchemaPolicy policy, Runnable control) {
        requireSize(DocumentSchemaEvidenceCodec.measureAndValidate(policy, control));
        var canonical = policy.toBuilder();
        if (policy.hasAllowedSchemas()) {
            var ordered = new ArrayList<>(policy.getAllowedSchemas().getBindingsList());
            var unique = new HashSet<Eligible>();
            for (var entry : ordered) {
                control.run();
                if (!unique.add(new Eligible(entry.getTypeUrl(), entry.getSchema())))
                    throw new IllegalArgumentException("duplicate policy schema binding");
            }
            var utf8 = ByteString.unsignedLexicographicalComparator();
            ordered.sort((a, b) -> {
                control.run();
                int result = utf8.compare(a.getTypeUrlBytes(), b.getTypeUrlBytes());
                if (result == 0) result = a.getSchema().getTypeName().compareTo(b.getSchema().getTypeName());
                if (result == 0) result = a.getSchema().getDescriptorFingerprint().compareTo(b.getSchema().getDescriptorFingerprint());
                return result;
            });
            canonical.setAllowedSchemas(policy.getAllowedSchemas().toBuilder().clearBindings().addAllBindings(ordered));
        }
        var value = canonical.build();
        var encoded = DocumentSchemaEvidenceCodec.encode(value, control);
        requireSize(encoded.bytes().size());
        return new DocumentAdmissionPolicy(value, encoded.bytes(), encoded.sha256());
    }

    /** Decode exact retained policy bytes; canonical ordering is part of the persisted identity. */
    public static DocumentAdmissionPolicy decode(String codec, int version, ByteString bytes, String sha256, Runnable control)
            throws InvalidProtocolBufferException {
        requireSize(Objects.requireNonNull(bytes).size());
        var parsed = DocumentSchemaEvidenceCodec.decode(CODEC, codec, version, bytes, sha256,
                DocumentSchemaPolicy.getDescriptor(), DocumentSchemaPolicy.parser(), control);
        var checked = of(parsed, control);
        if (!checked.bytes.equals(bytes)) throw new IllegalArgumentException("noncanonical policy schema ordering");
        return checked;
    }

    public DocumentSchemaPolicy definition() { return policy; }
    public ByteString bytes() { return bytes; }
    public String sha256() { return sha256; }
    public DocumentSchemaAdmission.Limits limits() { return limits; }

    /** An explicit schema condition requires typed admission even where opaque publication is permitted. */
    public boolean requiresTyped(DocumentPublicationMember member) {
        requireAccount(member);
        return policy.getMode() == DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED || member.hasStructuredSchema();
    }

    /** Always a typed attempt. A failure cannot authorize an opaque retry under this method. */
    public DocumentSchemaAdmission.Proof prepareAndCheck(ByteString commandSha256, DocumentPublicationMember member,
            Map<Integer, ByteString> fragments, DocumentSchemaAdmission.Definition container,
            DocumentSchemaAdmission.Resolver resolver, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(resolver); Objects.requireNonNull(control).run();
        requireAccount(member);
        var proof = DocumentSchemaAdmission.prepareAndCheck(new DocumentSchemaAdmission.Preparation(commandSha256,
                sha256, policy.getRequireStructuredRoot(), member, fragments, container), occurrence -> {
            var selected = resolver.select(occurrence);
            control.run();
            if (selected != null) requireEligible(selected.metadata().getTypeUrl(), selected.metadata().getSchema());
            return selected;
        }, limits, control);
        verifyProof(proof, control);
        return proof;
    }

    /** Policy-bound preparation with the serialized-byte lifetime defined by PreparedProof. */
    public DocumentSchemaAdmission.PreparedProof prepareAndCheck(ByteString commandSha256, DocumentPublicationMember member,
            Map<Integer, ByteString> fragments, DocumentSchemaAdmission.Definition container,
            DocumentSchemaAdmission.Resolver resolver, DocumentAdmissionReservations reservations, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(resolver); Objects.requireNonNull(reservations); Objects.requireNonNull(control).run();
        requireAccount(member);
        var prepared = DocumentSchemaAdmission.prepareAndCheck(new DocumentSchemaAdmission.Preparation(commandSha256,
                sha256, policy.getRequireStructuredRoot(), member, fragments, container), occurrence -> {
            var selected = resolver.select(occurrence);
            control.run();
            if (selected != null) requireEligible(selected.metadata().getTypeUrl(), selected.metadata().getSchema());
            return selected;
        }, limits, reservations, control);
        boolean accepted = false;
        try {
            verifyProof(prepared.proof(), reservations, control);
            accepted = true;
            return prepared;
        } finally {
            if (!accepted) prepared.close();
        }
    }

    /** Pure correspondence check; commit must additionally fence the authoritative policy pointer. */
    public void verifyProof(DocumentSchemaAdmission.Proof proof, Runnable control) throws InvalidProtocolBufferException {
        verifyProofInternal(proof, null, control);
    }

    /** Reserves only verification scratch; the caller owns the proof and this policy's bytes. */
    public void verifyProof(DocumentSchemaAdmission.Proof proof, DocumentAdmissionReservations reservations,
            Runnable control) throws InvalidProtocolBufferException {
        verifyProofInternal(proof, Objects.requireNonNull(reservations), control);
    }

    private void verifyProofInternal(DocumentSchemaAdmission.Proof proof, DocumentAdmissionReservations reservations,
            Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(proof); Objects.requireNonNull(control).run();
        requireAccount(proof.member());
        if (!sha256.equals(proof.policySha256()) || !policy.getValidationProfile().equals(proof.validationProfile())
                || policy.getRequireStructuredRoot() != proof.requireStructuredRoot() || !limits.equals(proof.limits()))
            throw new IllegalArgumentException("admission proof differs from policy snapshot");
        if (proof.roots().isEmpty()) throw new IllegalArgumentException("typed policy verdict requires a payload root");
        for (var root : proof.roots()) {
            control.run();
            var encoded = root.encoded();
            var bundle = reservations == null
                    ? DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(), encoded.sha256(), control)
                    : DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(), encoded.sha256(), reservations, control);
            for (var occurrence : bundle.getOccurrencesList()) {
                control.run();
                var boundary = occurrence.getSteps(occurrence.getStepsCount() - 1).getAnyBoundary();
                requireEligible(boundary.getTypeUrl(), boundary.getResolved().getSchema());
            }
        }
        control.run();
    }

    private void requireAccount(DocumentPublicationMember member) {
        Objects.requireNonNull(member);
        if (!policy.getAccountId().equals(member.getDestination().getAddress().getAccountId())
                || !policy.getAccountId().equals(member.getOwnership().getAccountId()))
            throw new IllegalArgumentException("publication member differs from policy account");
    }

    private void requireEligible(String url, PublicationSchemaCondition schema) {
        if (!policy.getAnyResolvedSchema() && !allowed.contains(new Eligible(url, schema)))
            throw new IllegalArgumentException("payload schema is not eligible under policy");
    }

    private static void requireSize(int bytes) {
        if (bytes < 1 || bytes > MAX_BYTES) throw new IllegalArgumentException("policy bytes exceed limit");
    }
}
