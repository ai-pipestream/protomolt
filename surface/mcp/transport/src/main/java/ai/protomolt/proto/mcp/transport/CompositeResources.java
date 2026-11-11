package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.Caller;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
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

    /**
     * A composite declares no scope of its own: each delegate's declaration applies.
     * The composite is visible when any delegate is, and the caller-aware overloads
     * serve only the visible delegates.
     */
    @Override
    public boolean visibleTo(Caller caller) {
        Objects.requireNonNull(caller, "caller");
        return delegates.stream().anyMatch(delegate -> delegate.visibleTo(caller));
    }

    @Override
    public boolean owns(String uri) {
        return delegates.stream().anyMatch(delegate -> delegate.owns(uri));
    }

    @Override
    public ArrayNode list(ObjectMapper mapper) {
        ArrayNode result = mapper.createArrayNode();
        delegates.forEach(delegate -> delegate.list(mapper).forEach(result::add));
        return result;
    }

    @Override
    public ArrayNode list(ObjectMapper mapper, Caller caller) {
        ArrayNode result = mapper.createArrayNode();
        delegates.forEach(delegate -> delegate.list(mapper, caller).forEach(result::add));
        return result;
    }

    @Override
    public ArrayNode templates(ObjectMapper mapper) {
        ArrayNode result = mapper.createArrayNode();
        delegates.forEach(delegate -> delegate.templates(mapper).forEach(result::add));
        return result;
    }

    @Override
    public ArrayNode templates(ObjectMapper mapper, Caller caller) {
        ArrayNode result = mapper.createArrayNode();
        delegates.forEach(
                delegate -> delegate.templates(mapper, caller).forEach(result::add));
        return result;
    }

    @Override
    public Page page(ObjectMapper mapper, String cursor, int pageSize) {
        return pageOver(delegates, mapper, cursor, pageSize,
                (delegate, delegateCursor, remaining) ->
                        delegate.page(mapper, delegateCursor, remaining));
    }

    @Override
    public Page page(ObjectMapper mapper, String cursor, int pageSize, Caller caller) {
        return scopedPage(mapper, cursor, pageSize, caller,
                (delegate, delegateCursor, remaining) ->
                        delegate.page(mapper, delegateCursor, remaining, caller));
    }

    @Override
    public Page pageTemplates(ObjectMapper mapper, String cursor, int pageSize) {
        return pageOver(delegates, mapper, cursor, pageSize,
                (delegate, delegateCursor, remaining) ->
                        delegate.pageTemplates(mapper, delegateCursor, remaining));
    }

    @Override
    public Page pageTemplates(ObjectMapper mapper, String cursor, int pageSize, Caller caller) {
        return scopedPage(mapper, cursor, pageSize, caller,
                (delegate, delegateCursor, remaining) ->
                        delegate.pageTemplates(mapper, delegateCursor, remaining, caller));
    }

    private Page scopedPage(ObjectMapper mapper, String cursor, int pageSize, Caller caller,
                            DelegatePager pager) {
        // The cursor indexes the caller-visible delegate list and carries the set of
        // delegates that were visible when it was issued. Scopes resolve per request,
        // so that set can change between pages; a cursor from a different set is
        // refused instead of silently skipping or repeating rows.
        Objects.requireNonNull(caller, "caller");
        if (delegates.size() > Long.SIZE) {
            throw new IllegalStateException("a scoped resource cursor covers at most "
                    + Long.SIZE + " collections");
        }
        List<McpResources> visible = new ArrayList<>();
        long mask = 0;
        for (int i = 0; i < delegates.size(); i++) {
            if (delegates.get(i).visibleTo(caller)) {
                visible.add(delegates.get(i));
                mask |= 1L << i;
            }
        }
        String stamp = Long.toHexString(mask);
        String inner = cursor;
        if (cursor != null && !cursor.isBlank()) {
            int dot = cursor.indexOf('.');
            if (dot <= 0) {
                throw new IllegalArgumentException("resource cursor is invalid");
            }
            if (!cursor.substring(0, dot).equals(stamp)) {
                throw new IllegalArgumentException("resource cursor was issued for a"
                        + " different set of visible collections; list again without a"
                        + " cursor");
            }
            inner = cursor.substring(dot + 1);
        }
        Page page = pageOver(visible, mapper, inner, pageSize, pager);
        return page.nextCursor() == null ? page
                : new Page(page.resources(), stamp + "." + page.nextCursor());
    }

    private Page pageOver(List<McpResources> served, ObjectMapper mapper, String cursor,
                          int pageSize, DelegatePager pager) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive");
        }
        Position position = decode(cursor);
        if (cursor != null && position.delegateIndex() >= served.size()) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        ArrayNode result = mapper.createArrayNode();
        int delegateIndex = position.delegateIndex();
        String delegateCursor = position.delegateCursor();
        while (delegateIndex < served.size() && result.size() < pageSize) {
            McpResources delegate = served.get(delegateIndex);
            Page page = pager.page(delegate, delegateCursor, pageSize - result.size());
            page.resources().forEach(result::add);
            if (page.nextCursor() != null) {
                return new Page(result, encode(delegateIndex, page.nextCursor()));
            }
            delegateIndex++;
            delegateCursor = null;
        }
        return new Page(result, null);
    }

    @FunctionalInterface
    private interface DelegatePager {
        Page page(McpResources delegate, String cursor, int pageSize);
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

    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri, Caller caller) {
        // A delegate that owns the URI but is not visible to the caller refuses by
        // name; the refusal propagates instead of falling through to later delegates.
        for (McpResources delegate : delegates) {
            Optional<ObjectNode> found = delegate.read(mapper, uri, caller);
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
