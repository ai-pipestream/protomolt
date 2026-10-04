package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/**
 * Internal content check against the sole canonical command. Not an admission
 * receipt: the caller must separately prove authorization, selected upload and
 * retained physical identities, descriptor evidence and commit-time fences.
 */
final class DocumentCommandContent {
    private final DocumentPublicationCommand command;
    private final DocumentPublicationMember member;
    private final DocumentRevisionAssembly.Result assembly;

    private DocumentCommandContent(DocumentPublicationCommand command, DocumentPublicationMember member,
            DocumentRevisionAssembly.Result assembly) {
        this.command = command; this.member = member; this.assembly = assembly;
    }

    DocumentPublicationCommand command() { return command; }
    DocumentPublicationMember member() { return member; }
    DocumentRevisionAssembly.Result assembly() { return assembly; }

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
        Objects.requireNonNull(limits);
        control.run();
        var member = command.intent().getMembersList().stream().filter(m -> m.getMemberId().equals(memberId))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown publication member"));
        if (schemaRequired || member.hasStructuredSchema())
            throw new UnsupportedOperationException("Typed schema validation and retention are not yet available");
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
            long size = part.hasUpload() ? part.getUpload().getSizeBytes() : part.getReuse().getObject().getSizeBytes();
            String digest = part.hasUpload() ? part.getUpload().getSha256() : part.getReuse().getObject().getSha256();
            if (bytes.size() != size || !sha256(bytes, control).equals(digest))
                throw new IllegalArgumentException("Materialized bytes differ from command declaration");
            fragments.add(new DocumentRevisionAssembly.Fragment(part.getSlot().getPart(), part.getSlot().getSubKey(), bytes));
        }
        var assembly = DocumentRevisionAssembly.assemble(fragments, member.getDestination().getAddress().getDocId(), limits, control);
        if (!assembly.document().hasOwnership() || !assembly.document().getOwnership().equals(member.getOwnership()))
            throw new IllegalArgumentException("Decoded ownership differs from command");
        control.run();
        return new DocumentCommandContent(command, member, assembly);
    }

    private static String sha256(ByteString bytes, Runnable control) {
        try {
            var hash = java.security.MessageDigest.getInstance("SHA-256");
            for (var buffer : bytes.asReadOnlyByteBufferList()) { control.run(); hash.update(buffer); }
            return java.util.HexFormat.of().formatHex(hash.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
