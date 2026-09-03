package ai.protomolt.proto.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Combines registry, service-workspace, and future resource collections in stable order. */
public final class CompositeResources implements McpResources {

    private final List<McpResources> delegates;

    public CompositeResources(List<McpResources> delegates) {
        this.delegates = List.copyOf(delegates);
        this.delegates.forEach(delegate -> Objects.requireNonNull(delegate, "resource delegate"));
    }

    /** Creates a composite after removing null optional collections. */
    public static McpResources of(McpResources... resources) {
        List<McpResources> present = Arrays.stream(resources).filter(Objects::nonNull).toList();
        return present.isEmpty() ? null
                : present.size() == 1 ? present.getFirst() : new CompositeResources(present);
    }

    @Override
    public ArrayNode list(ObjectMapper mapper) {
        ArrayNode result = mapper.createArrayNode();
        delegates.forEach(delegate -> delegate.list(mapper).forEach(result::add));
        return result;
    }

    @Override
    public ArrayNode templates(ObjectMapper mapper) {
        ArrayNode result = mapper.createArrayNode();
        delegates.forEach(delegate -> delegate.templates(mapper).forEach(result::add));
        return result;
    }

    @Override
    public Page page(ObjectMapper mapper, String cursor, int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive");
        }
        Position position = decode(cursor);
        if (cursor != null && position.delegateIndex() >= delegates.size()) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        ArrayNode result = mapper.createArrayNode();
        int delegateIndex = position.delegateIndex();
        String delegateCursor = position.delegateCursor();
        while (delegateIndex < delegates.size() && result.size() < pageSize) {
            Page page = delegates.get(delegateIndex).page(mapper, delegateCursor,
                    pageSize - result.size());
            page.resources().forEach(result::add);
            if (page.nextCursor() != null) {
                return new Page(result, encode(delegateIndex, page.nextCursor()));
            }
            delegateIndex++;
            delegateCursor = null;
        }
        return new Page(result, null);
    }

    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
        for (McpResources delegate : delegates) {
            Optional<ObjectNode> found = delegate.read(mapper, uri);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private static String encode(int delegateIndex, String cursor) {
        return delegateIndex + ":" + (cursor == null ? "" : cursor);
    }

    private static Position decode(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return new Position(0, null);
        }
        int separator = cursor.indexOf(':');
        if (separator <= 0 || separator == cursor.length() - 1) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        try {
            int delegate = Integer.parseInt(cursor.substring(0, separator));
            if (delegate < 0) {
                throw new NumberFormatException();
            }
            String child = cursor.substring(separator + 1);
            return new Position(delegate, child.isBlank() ? null : child);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
    }

    private record Position(int delegateIndex, String delegateCursor) {
    }
}
