package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;

/** Exact expected SQL retention sets. It is not an admission record or an authorization grant. */
final class DocumentSchemaManifest {
    static final int MAX_BYTES = 16 * 1024 * 1024;
    private final String decision;
    private final String json;
    private DocumentSchemaManifest(String decision, String json) { this.decision = decision; this.json = json; }
    String decision() { return decision; }
    String json() { return json; }

    static DocumentSchemaManifest prepare(DocumentSchemaBatch batch, String memberId, DocumentCommitParts.Bound bound, Runnable control) {
        Objects.requireNonNull(batch); Objects.requireNonNull(memberId); Objects.requireNonNull(bound); active(control);
        var member = batch.command().intent().getMembersList().stream().filter(m -> m.getMemberId().equals(memberId))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown schema manifest member"));
        var physical = ListValue.newBuilder(); var expectedSlots = new HashSet<Integer>();
        long estimatedBytes = 256;
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            active(control);
            var part = member.getParts(ordinal);
            if (part.hasEmpty()) continue;
            expectedSlots.add(ordinal);
            var actual = bound.parts().get(new DocumentCommitParts.Slot(memberId, ordinal));
            var object = part.hasHistoricalReuse() ? part.getHistoricalReuse().getObject() : part.getReuse().getObject();
            long size = part.hasUpload() ? part.getUpload().getSizeBytes() : object.getSizeBytes();
            String sha = part.hasUpload() ? part.getUpload().getSha256() : object.getSha256();
            if (actual == null || actual.id() == null || actual.part() != part.getSlot().getPartValue()
                    || !part.getSlot().getSubKey().equals(actual.subKey()) || actual.size() != size || !sha.equals(actual.sha256())
                    || ((part.hasReuse() || part.hasHistoricalReuse()) && !UUID.fromString(object.getObjectId()).equals(actual.id())))
                throw new IllegalArgumentException("Schema manifest physical part differs from command");
            estimatedBytes = reserve(estimatedBytes, 256L + 6L * actual.subKey().getBytes(StandardCharsets.UTF_8).length);
            physical.addValues(object(Struct.newBuilder().putFields("ordinal", number(ordinal))
                    .putFields("part", number(actual.part())).putFields("sub_key", text(actual.subKey()))
                    .putFields("object_id", text(actual.id().toString())).putFields("size", number(size)).putFields("sha256", text(sha))));
        }
        var actualSlots = bound.parts().keySet().stream().filter(slot -> slot.member().equals(memberId))
                .map(DocumentCommitParts.Slot::ordinal).collect(java.util.stream.Collectors.toSet());
        if (!actualSlots.equals(expectedSlots)) throw new IllegalArgumentException("Schema manifest physical part set differs from command");
        var artifacts = ListValue.newBuilder(); var assets = ListValue.newBuilder(); var roots = ListValue.newBuilder();
        var proof = batch.proofs().get(memberId);
        if (proof != null) {
            for (var digest : proof.artifacts().keySet().stream().sorted().toList()) {
                active(control); estimatedBytes = reserve(estimatedBytes, 68); artifacts.addValues(text(digest));
            }
            for (var reference : proof.references().stream().sorted(Comparator
                    .comparing((DocumentSchemaAdmission.Reference r) -> sha(r.typeUrl()))
                    .thenComparing(DocumentSchemaAdmission.Reference::descriptorSha256)).toList()) {
                active(control);
                estimatedBytes = reserve(estimatedBytes, 512L + 6L * reference.typeUrl().getBytes(StandardCharsets.UTF_8).length);
                assets.addValues(object(Struct.newBuilder().putFields("type_url", text(reference.typeUrl()))
                        .putFields("type_url_sha256", text(sha(reference.typeUrl())))
                        .putFields("descriptor_sha256", text(reference.descriptorSha256()))
                        .putFields("metadata_sha256", text(reference.metadataSha256()))
                        .putFields("metadata_codec", text(reference.metadataCodec())).putFields("metadata_version", number(reference.metadataVersion()))
                        .putFields("source_sha256", reference.sourceSha256().map(DocumentSchemaManifest::text)
                                .orElseGet(() -> Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()))));
            }
            for (var root : proof.roots().stream().sorted(Comparator.comparingInt(DocumentSchemaAdmission.RootEvidence::ordinal)
                    .thenComparing(DocumentSchemaAdmission.RootEvidence::locatorSha256)).toList()) {
                active(control);
                estimatedBytes = reserve(estimatedBytes, 512);
                var fragment = bound.parts().get(new DocumentCommitParts.Slot(memberId, root.ordinal()));
                roots.addValues(object(Struct.newBuilder().putFields("ordinal", number(root.ordinal()))
                        .putFields("locator_sha256", text(root.locatorSha256())).putFields("fragment_sha256", text(fragment.sha256()))
                        .putFields("fragment_size", number(fragment.size())).putFields("evidence_codec", text(root.encoded().codec()))
                        .putFields("evidence_version", number(root.encoded().version())).putFields("evidence_sha256", text(root.encoded().sha256()))));
            }
        }
        var manifest = Struct.newBuilder().putFields("version", number(1)).putFields("parts", list(physical))
                .putFields("artifacts", list(artifacts)).putFields("assets", list(assets)).putFields("roots", list(roots)).build();
        final String encoded;
        try { encoded = JsonFormat.printer().omittingInsignificantWhitespace().print(manifest); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new IllegalArgumentException("Cannot encode schema manifest", invalid); }
        if (encoded.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) throw new IllegalArgumentException("Schema manifest exceeds byte limit");
        active(control);
        return new DocumentSchemaManifest(proof == null ? "OPAQUE" : "TYPED", encoded);
    }

    private static Value object(Struct.Builder value) { return Value.newBuilder().setStructValue(value).build(); }
    private static Value list(ListValue.Builder value) { return Value.newBuilder().setListValue(value).build(); }
    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
    // Decimal strings preserve exact SQL integer identities without JSON floating-point conversion.
    private static Value number(long value) { return text(Long.toString(value)); }
    // Includes worst-case JSON escaping before constructing the encoded value.
    private static long reserve(long used, long additional) {
        if (additional > MAX_BYTES - used) throw new IllegalArgumentException("Schema manifest exceeds allocation budget");
        return used + additional;
    }
    private static String sha(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable", impossible); }
    }
    private static void active(Runnable control) {
        Objects.requireNonNull(control);
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Schema manifest interrupted");
        control.run();
    }
}
