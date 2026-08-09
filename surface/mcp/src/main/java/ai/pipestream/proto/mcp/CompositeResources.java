package ai.pipestream.proto.mcp;

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
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
        for (McpResources delegate : delegates) {
            Optional<ObjectNode> found = delegate.read(mapper, uri);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }
}
