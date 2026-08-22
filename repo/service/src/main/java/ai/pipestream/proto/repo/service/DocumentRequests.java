package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.NodeAddress;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static ai.pipestream.proto.repo.service.GrpcErrors.invalidArgument;

/**
 * Reading the arguments of a document request: the small conversions every RPC does before
 * it can start work, each of which refuses by naming the field it read rather than the
 * value it found.
 */
final class DocumentRequests {

    private DocumentRequests() {
    }

    /** The parts as a set, refusing the unspecified and unrecognized members. */
    static Set<DocumentPart> partsOrThrow(List<DocumentPart> parts, String field) {
        Set<DocumentPart> out = EnumSet.noneOf(DocumentPart.class);
        for (DocumentPart part : parts) {
            if (part == DocumentPart.DOCUMENT_PART_UNSPECIFIED
                    || part == DocumentPart.UNRECOGNIZED) {
                throw invalidArgument(field + " must not contain DOCUMENT_PART_UNSPECIFIED");
            }
            out.add(part);
        }
        return out;
    }

    /**
     * An address is all four segments or it is not an address. A partly-filled one would
     * hash to a node id that addresses nothing, so it is refused here rather than stored.
     */
    static NodeAddress validateAddress(NodeAddress address, String field) {
        if (address.getDocId().isBlank() || address.getGraphAddressId().isBlank()
                || address.getAccountId().isBlank() || address.getGraphId().isBlank()) {
            throw invalidArgument(
                    field + " requires doc_id, graph_address_id, account_id and graph_id");
        }
        return address;
    }

    /** An address rendered for an error message. */
    static String describe(NodeAddress address) {
        return "doc_id=" + address.getDocId()
                + ", graph_address_id=" + address.getGraphAddressId()
                + ", account_id=" + address.getAccountId()
                + ", graph_id=" + address.getGraphId();
    }

    static UUID parseUuid(String raw, String field) {
        if (raw == null || raw.isBlank()) {
            throw invalidArgument(field + " is required");
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            throw invalidArgument(field + " must be a UUID (got \"" + raw + "\")");
        }
    }

    /** A list page's continuation token, which is a row offset; absent means the first page. */
    static long parseContinuationToken(String token) {
        if (token == null || token.isBlank()) {
            return 0;
        }
        long offset;
        try {
            offset = Long.parseLong(token.trim());
        } catch (NumberFormatException e) {
            throw invalidArgument(
                    "continuation_token must be a row offset (got \"" + token + "\")");
        }
        if (offset < 0) {
            throw invalidArgument("continuation_token must be a non-negative row offset");
        }
        return offset;
    }

    /** A proto3 string field read as an optional column value. */
    static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /**
     * Whether a failure was, underneath, an object that is not there. The walk stops on a
     * repeated cause so a cyclic chain cannot hang the handler.
     */
    static boolean hasNotFoundCause(Throwable t) {
        Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Throwable cur = t; cur != null && seen.add(cur); cur = cur.getCause()) {
            if (cur instanceof BlobStore.BlobNotFoundException) {
                return true;
            }
        }
        return false;
    }

    static Timestamp timestampNow() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }
}
