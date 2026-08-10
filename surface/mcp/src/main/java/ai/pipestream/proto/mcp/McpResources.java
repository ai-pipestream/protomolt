package ai.pipestream.proto.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;
import java.util.Objects;

/** One independently mountable collection of MCP resources. */
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

    /** Lists the resources served by this collection. */
    ArrayNode list(ObjectMapper mapper);

    /**
     * Returns one bounded page. The default keeps existing providers source-compatible while
     * allowing providers with a cheap page primitive to avoid materializing their full index.
     */
    default Page page(ObjectMapper mapper, String cursor, int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive");
        }
        int offset = 0;
        if (cursor != null && !cursor.isBlank()) {
            try {
                offset = Integer.parseInt(cursor);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("resource cursor is invalid");
            }
        }
        if (offset < 0) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        ArrayNode all = list(mapper);
        if (offset > all.size()) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        int end = Math.min(offset + pageSize, all.size());
        ArrayNode page = mapper.createArrayNode();
        for (int i = offset; i < end; i++) {
            page.add(all.get(i));
        }
        return new Page(page, end < all.size() ? Integer.toString(end) : null);
    }

    /** Reads one URI, or empty when this collection does not own it. */
    Optional<ObjectNode> read(ObjectMapper mapper, String uri);
}
