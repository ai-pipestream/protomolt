package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Internal content check against the sole canonical command. Not an admission
 * receipt: the caller must separately prove authorization, selected upload and
 * retained physical identities, descriptor evidence and commit-time fences.
 */
final class DocumentCommandContent {
    private final DocumentPublicationCommand command;
    private final DocumentPublicationMember member;
    private final DocumentRevisionAssembly.Result assembly;
    private final Optional<RepositoryAnyResolution> structuredResolution;

    private DocumentCommandContent(DocumentPublicationCommand command, DocumentPublicationMember member,
            DocumentRevisionAssembly.Result assembly, Optional<RepositoryAnyResolution> structuredResolution) {
        this.command = command; this.member = member; this.assembly = assembly;
        this.structuredResolution = structuredResolution;
    }

    DocumentPublicationCommand command() { return command; }
    DocumentPublicationMember member() { return member; }
    DocumentRevisionAssembly.Result assembly() { return assembly; }
    /** Root structured_data observation only; no nested resolution, validation or retention is implied. */
    Optional<RepositoryAnyResolution> structuredResolution() { return structuredResolution; }

    /**
     * Ordinals index the complete member, including EMPTY slots. The host supplies
     * policy, bounded materialized bytes and their memory reservation. Schema-required
     * policy fails closed even if the caller omitted structured_schema.
     * The host supplies a stable bounded map; snapshot copying is not an allocation
     * bound for a hostile map that grows during traversal.
     */
    static DocumentCommandContent check(DocumentPublicationCommand command, String memberId,
            Map<Integer, ByteString> materialized, boolean schemaRequired, DocumentRevisionAssembly.Limits limits,
            Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(materialized); Objects.requireNonNull(control);
        command.requireExecutionSupported();
        return checkContent(command, memberId, materialized, schemaRequired, limits, control);
    }

    /** Explicit raw historical path. Does not load retained schemas or grant typed admission. */
    static DocumentCommandContent checkHistorical(DocumentPublicationCommand command, String memberId,
            Map<Integer, ByteString> materialized, boolean schemaRequired, DocumentRevisionAssembly.Limits limits,
            java.util.List<DocumentHistoricalReferenceAdmission.Prepared> historical, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(materialized); Objects.requireNonNull(control);
        var references = DocumentHistoricalReferenceAdmission.requireComplete(command, historical, control);
        var result = checkContent(command, memberId, materialized, schemaRequired, limits, control);
        DocumentHistoricalReferenceAdmission.requireComplete(command, references, control);
        return result;
    }

    private static DocumentCommandContent checkContent(DocumentPublicationCommand command, String memberId,
            Map<Integer, ByteString> materialized, boolean schemaRequired, DocumentRevisionAssembly.Limits limits,
            Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(limits);
        control.run();
        var member = command.intent().getMembersList().stream().filter(m -> m.getMemberId().equals(memberId))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown publication member"));
        if (schemaRequired || member.hasStructuredSchema())
            throw new UnsupportedOperationException("Typed content requires a checked schema batch");
        var expected = new HashSet<Integer>();
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++)
            if (!member.getParts(ordinal).hasEmpty()) expected.add(ordinal);
        if (materialized.size() > member.getPartsCount() || materialized.size() > limits.maxFragments())
            throw new IllegalArgumentException("Materialized fragment count exceeds bound");
        var bytesByOrdinal = Map.copyOf(materialized);
        if (!expected.equals(bytesByOrdinal.keySet())) throw new IllegalArgumentException("Materialized slots differ from command");
        if (bytesByOrdinal.size() > limits.maxFragments()) throw new IllegalArgumentException("Materialized fragment count exceeds bound");
        long total = 0;
        for (var bytes : bytesByOrdinal.values()) {
            control.run();
            if (bytes.size() > limits.maxBytes() - total) throw new IllegalArgumentException("Materialized bytes exceed aggregate bound");
            total += bytes.size();
        }
        var fragments = new ArrayList<DocumentRevisionAssembly.Fragment>();
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            control.run();
            var part = member.getParts(ordinal);
            if (part.hasEmpty()) continue;
            var bytes = bytesByOrdinal.get(ordinal);
            long size;
            String digest;
            switch (part.getContentCase()) {
                case UPLOAD -> { size = part.getUpload().getSizeBytes(); digest = part.getUpload().getSha256(); }
                case REUSE -> { size = part.getReuse().getObject().getSizeBytes(); digest = part.getReuse().getObject().getSha256(); }
                case HISTORICAL_REUSE -> { size = part.getHistoricalReuse().getObject().getSizeBytes(); digest = part.getHistoricalReuse().getObject().getSha256(); }
                default -> throw new IllegalArgumentException("Materialized fragment has no declared payload");
            }
            if (bytes.size() != size || !sha256(bytes, control).equals(digest))
                throw new IllegalArgumentException("Materialized bytes differ from command declaration");
            fragments.add(new DocumentRevisionAssembly.Fragment(part.getSlot().getPart(), part.getSlot().getSubKey(), bytes));
        }
        var assembly = DocumentRevisionAssembly.assemble(fragments, member.getDestination().getAddress().getDocId(), limits, control);
        if (!assembly.document().hasOwnership() || !assembly.document().getOwnership().equals(member.getOwnership()))
            throw new IllegalArgumentException("Decoded ownership differs from command");
        Optional<RepositoryAnyResolution> resolution = Optional.empty();
        if (assembly.document().hasStructuredData()) {
            var any = assembly.document().getStructuredData();
            String url = any.getTypeUrl();
            if (url.codePointCount(0, url.length()) > 4096)
                throw new IllegalArgumentException("Opaque structured data type URL exceeds observation bound");
            resolution = Optional.of(RepositoryAnyResolution.newBuilder().setTypeUrl(url)
                    .setValueSha256(sha256(any.getValue(), control)).setValueSizeBytes(any.getValue().size())
                    .setNotAttempted(true).build());
        }
        control.run();
        return new DocumentCommandContent(command, member, assembly, resolution);
    }

    /** The checked batch already verifies the command, policy and immutable runtime proof. */
    static DocumentCommandContent fromSchema(DocumentSchemaBatch batch, String memberId, Runnable control) {
        Objects.requireNonNull(batch); Objects.requireNonNull(control); control.run();
        var proof = batch.proofs().get(memberId);
        if (proof == null) throw new IllegalArgumentException("Typed content requires a checked member proof");
        var fragments = new ArrayList<DocumentRevisionAssembly.Fragment>();
        for (var ordinal : proof.fragments().keySet().stream().sorted().toList()) {
            control.run();
            var part = proof.member().getParts(ordinal);
            fragments.add(new DocumentRevisionAssembly.Fragment(part.getSlot().getPart(), part.getSlot().getSubKey(), proof.fragments().get(ordinal)));
        }
        // The full occurrence-specific retained evidence is authoritative. Do not
        // label a typed document with the legacy opaque not-attempted observation.
        return new DocumentCommandContent(batch.command(), proof.member(),
                new DocumentRevisionAssembly.Result(proof.document(), fragments), Optional.empty());
    }

    static String sha256(ByteString bytes, Runnable control) {
        try {
            var hash = java.security.MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) { control.run(); hash.update(buffer); }
            return java.util.HexFormat.of().formatHex(hash.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
