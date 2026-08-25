package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.formats.Formats;
import ai.pipestream.proto.repo.archive.v1.EntryAddress;
import ai.pipestream.proto.repo.archive.v1.RenditionDescriptor;

import static ai.pipestream.proto.repo.service.GrpcErrors.invalidArgument;

/**
 * Request validation for the archive surface, mirroring the validate.v1
 * annotations on the archive protos the way the rest of the service
 * validates in code. Every refusal names the offending field.
 */
final class ArchiveRequests {

    /** Default page size when a listing names none. */
    static final int DEFAULT_PAGE = 100;

    /** Hard page-size ceiling. */
    static final int MAX_PAGE = 1000;

    private ArchiveRequests() {
    }

    /** Validates the three segments of an entry address. */
    static EntryAddress address(boolean present, EntryAddress address) {
        if (!present) {
            throw invalidArgument("address is required");
        }
        accountId(address.getAccountId());
        archiveName(address.getArchive(), "address.archive");
        if (address.getEntryId().isBlank()) {
            throw invalidArgument("address.entry_id is required");
        }
        if (address.getEntryId().length() > 500) {
            throw invalidArgument("address.entry_id exceeds 500 characters");
        }
        return address;
    }

    /** Validates an account id (1..200 characters). */
    static String accountId(String accountId) {
        if (accountId.isBlank()) {
            throw invalidArgument("account_id is required");
        }
        if (accountId.length() > 200) {
            throw invalidArgument("account_id exceeds 200 characters");
        }
        return accountId;
    }

    /** Validates an archive name (slug, at most 100 characters). */
    static String archiveName(String name, String field) {
        if (name.isBlank()) {
            throw invalidArgument(field + " is required");
        }
        if (name.length() > 100 || !Formats.isSlug(name)) {
            throw invalidArgument(field + " must be a slug of at most 100 characters");
        }
        return name;
    }

    /** Validates a rendition descriptor's name, sub key, media type, and subject. */
    static RenditionDescriptor rendition(boolean present, RenditionDescriptor descriptor) {
        if (!present) {
            throw invalidArgument("rendition is required");
        }
        renditionName(descriptor.getName(), "rendition.name");
        if (!descriptor.getSubKey().isBlank()) {
            if (descriptor.getSubKey().length() > 200
                    || !Formats.isPathSafeName(descriptor.getSubKey())) {
                throw invalidArgument(
                        "rendition.sub_key must be path-safe and at most 200 characters");
            }
        }
        if (!descriptor.getMediaType().isBlank()) {
            if (descriptor.getMediaType().length() > 200
                    || !Formats.isMediaType(descriptor.getMediaType())) {
                throw invalidArgument("rendition.media_type must be a valid media type");
            }
        }
        if (descriptor.getSchemaSubject().length() > 300) {
            throw invalidArgument("rendition.schema_subject exceeds 300 characters");
        }
        return descriptor;
    }

    /** Validates a bare rendition name (path-safe, at most 200 characters). */
    static String renditionName(String name, String field) {
        if (name.isBlank()) {
            throw invalidArgument(field + " is required");
        }
        if (name.length() > 200 || !Formats.isPathSafeName(name)) {
            throw invalidArgument(field + " must be path-safe and at most 200 characters");
        }
        return name;
    }

    /** Validates an optional expected SHA-256. */
    static String sha256(String value, String field) {
        if (!value.isBlank() && !Formats.isSha256Hex(value)) {
            throw invalidArgument(field + " must be a lowercase hex SHA-256");
        }
        return value;
    }

    /** Clamps a listing page size onto [1, MAX_PAGE] with the default for 0. */
    static int page(int limit) {
        if (limit < 0 || limit > MAX_PAGE) {
            throw invalidArgument("limit must be between 0 and " + MAX_PAGE);
        }
        return limit == 0 ? DEFAULT_PAGE : limit;
    }

    /** Parses a continuation token (a zero-based offset) back to its offset. */
    static long offset(String continuationToken) {
        if (continuationToken.isBlank()) {
            return 0;
        }
        try {
            long offset = Long.parseLong(continuationToken);
            if (offset < 0) {
                throw new NumberFormatException("negative");
            }
            return offset;
        } catch (NumberFormatException e) {
            throw invalidArgument("continuation_token is not from this listing");
        }
    }

    /** Bounds a free-text field, refusing by name when it overflows. */
    static String bounded(String value, int max, String field) {
        if (value.length() > max) {
            throw invalidArgument(field + " exceeds " + max + " characters");
        }
        return value;
    }
}
