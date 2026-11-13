package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.mcp.transport.ResourceAccessDeniedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;
import java.util.Objects;

/**
 * One independently mountable collection of MCP resources.
 *
 * <p>Authorization mirrors the transport contract (and the action catalog): a
 * collection declares the one scope a caller must hold to list or read it through
 * {@link #requiredScope()}; a collection that declares none is refused to scoped
 * callers, and only a server-describing bootstrap may opt into
 * {@link #authenticatedOnly()} instead.</p>
 */
public interface McpResources {

    /** A bounded resource page and an opaque continuation cursor, when more rows remain. */
    record Page(ArrayNode resources, String nextCursor) {
        public Page {
            Objects.requireNonNull(resources, "resources");
            if (nextCursor != null && nextCursor.isBlank()) {
                throw new IllegalArgumentException("next cursor must not be blank");
            }
        }
    }

    /**
     * The scope a caller must hold to list or read this collection, from the closed
     * vocabulary; empty when the collection declares none.
     */
    default String requiredScope() {
        return "";
    }

    /**
     * Whether this collection only describes the server to its caller and therefore
     * stays behind authentication alone, with no scope.
     */
    default boolean authenticatedOnly() {
        return false;
    }

    /** Whether {@code caller} may see this collection at all: list, templates, reads. */
    default boolean visibleTo(Caller caller) {
        Objects.requireNonNull(caller, "caller");
        if (caller.unrestricted() || authenticatedOnly()) {
            return true;
        }
        String scope = requiredScope();
        return !scope.isBlank() && caller.holds(scope);
    }

    /**
     * Whether this collection serves the namespace {@code uri} addresses, judged from
     * the URI alone and without touching any store, so a read by a caller who may not
     * see the collection is refused before anything is read. Every collection declares
     * it: a refusal never depends on whether the addressed resource exists.
     */
    boolean owns(String uri);

    /** Lists the resources served by this collection. */
    ArrayNode list(ObjectMapper mapper);

    /** Lists URI templates for resources that are addressable but intentionally not enumerated. */
    default ArrayNode templates(ObjectMapper mapper) {
        return mapper.createArrayNode();
    }

    /**
     * Returns one bounded page. The default keeps existing providers source-compatible while
     * allowing providers with a cheap page primitive to avoid materializing their full index.
     */
    default Page page(ObjectMapper mapper, String cursor, int pageSize) {
        var page = ai.protomolt.proto.mcp.transport.McpResources.page(
                mapper, () -> list(mapper), cursor, pageSize);
        return new Page(page.resources(), page.nextCursor());
    }

    /** Returns one bounded page of URI templates. */
    default Page pageTemplates(ObjectMapper mapper, String cursor, int pageSize) {
        var page = ai.protomolt.proto.mcp.transport.McpResources.page(
                mapper, () -> templates(mapper), cursor, pageSize);
        return new Page(page.resources(), page.nextCursor());
    }

    /** Reads one URI, or empty when this collection does not own it. */
    Optional<ObjectNode> read(ObjectMapper mapper, String uri);

    /** {@link #list(ObjectMapper)} as {@code caller} sees it: empty when not visible. */
    default ArrayNode list(ObjectMapper mapper, Caller caller) {
        return visibleTo(caller) ? list(mapper) : mapper.createArrayNode();
    }

    /**
     * {@link #templates(ObjectMapper)} as {@code caller} sees it: empty when not visible.
     */
    default ArrayNode templates(ObjectMapper mapper, Caller caller) {
        return visibleTo(caller) ? templates(mapper) : mapper.createArrayNode();
    }

    /**
     * {@link #page(ObjectMapper, String, int)} as {@code caller} sees it: an empty
     * final page when not visible, so neither rows nor a continuation leak.
     */
    default Page page(ObjectMapper mapper, String cursor, int pageSize, Caller caller) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive");
        }
        return visibleTo(caller)
                ? page(mapper, cursor, pageSize)
                : new Page(mapper.createArrayNode(), null);
    }

    /**
     * {@link #pageTemplates(ObjectMapper, String, int)} as {@code caller} sees it: an
     * empty final page when not visible, so neither templates nor a continuation leak.
     */
    default Page pageTemplates(ObjectMapper mapper, String cursor, int pageSize, Caller caller) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive");
        }
        return visibleTo(caller)
                ? pageTemplates(mapper, cursor, pageSize)
                : new Page(mapper.createArrayNode(), null);
    }

    /**
     * {@link #read(ObjectMapper, String)} as {@code caller}: refused by name, before
     * any contents are returned, when this collection owns {@code uri} and the caller
     * may not see the collection.
     *
     * @throws ResourceAccessDeniedException when the caller lacks the required scope
     */
    default Optional<ObjectNode> read(ObjectMapper mapper, String uri, Caller caller) {
        Objects.requireNonNull(caller, "caller");
        if (visibleTo(caller)) {
            return read(mapper, uri);
        }
        // Ownership comes from the URI alone, so the refusal reads nothing and says
        // nothing about whether the addressed resource exists.
        if (owns(uri)) {
            throw new ResourceAccessDeniedException(caller, requiredScope());
        }
        return Optional.empty();
    }
}
