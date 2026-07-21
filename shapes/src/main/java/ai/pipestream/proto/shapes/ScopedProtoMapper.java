package ai.pipestream.proto.shapes;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Objects;

/**
 * Applies text mapping rules whose source paths are scoped: {@code target.path = name.path}
 * reads from the scope entry {@code name}, and a bare {@code target = name} copies the whole
 * entry. The rule syntax is otherwise exactly the {@code map-message} surface — {@code =},
 * {@code +=}, and {@code -target.to.clear} — so a scoped ruleset reads like an ordinary one
 * with named sources instead of an implicit single message.
 *
 * <p>A source path that resolves to nothing (an unset optional hop) skips its rule rather
 * than failing — join semantics for absent values; an unknown scope name is always an
 * error. A source may also be a literal ({@code "text"}, a number, {@code true},
 * {@code false}, or {@code null}, which clears its target), as in the unscoped dialect.</p>
 */
public final class ScopedProtoMapper {

    private final ProtoFieldMapperImpl mapper;

    public ScopedProtoMapper(DescriptorRegistry registry) {
        this.mapper = new ProtoFieldMapperImpl(Objects.requireNonNull(registry, "registry"));
    }

    /** The underlying single-message mapper, for CEL rule application over the same registry. */
    public ProtoFieldMapperImpl fieldMapper() {
        return mapper;
    }

    public void map(MessageScope scope, Message.Builder target, List<String> rules)
            throws MappingException {
        for (String rule : rules) {
            apply(scope, target, rule);
        }
    }

    private void apply(MessageScope scope, Message.Builder target, String rule)
            throws MappingException {
        String trimmed = rule.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        if (trimmed.startsWith("-")) {
            mapper.clearField(target, trimmed.substring(1).trim());
            return;
        }
        boolean append = trimmed.contains("+=");
        String[] sides = trimmed.split(append ? "\\+=" : "=", 2);
        if (sides.length != 2 || sides[0].isBlank() || sides[1].isBlank()) {
            throw new MappingException("Rule is not 'target = source.path', "
                    + "'target += source.path', or '-target'", rule);
        }
        String source = sides[1].trim();
        String targetPath = sides[0].trim();
        Object value;
        if (Literals.isLiteral(source)) {
            value = Literals.valueOf(source);
            if (value == null) {
                mapper.clearField(target, targetPath); // the 'null' literal clears
                return;
            }
        } else {
            value = resolve(scope, source, rule);
            if (value == null) {
                return; // absent optional source: skip, do not fail the join
            }
        }
        // Repeated values land item by item: the type converter works on elements.
        if (value instanceof List<?> items) {
            if (!append) {
                mapper.clearField(target, targetPath);
            }
            for (Object item : items) {
                mapper.appendValue(target, targetPath, item);
            }
            return;
        }
        if (append) {
            mapper.appendValue(target, targetPath, value);
        } else {
            mapper.setValue(target, targetPath, value);
        }
    }

    /** Resolves a scoped source path: the first segment names the entry, the rest walks it. */
    public Object resolve(MessageScope scope, String scopedPath, String context)
            throws MappingException {
        int dot = scopedPath.indexOf('.');
        String name = dot < 0 ? scopedPath : scopedPath.substring(0, dot);
        Message source = scope.get(name);
        if (source == null) {
            throw new MappingException("Unknown source '" + name + "'; the scope has "
                    + scope.names(), context);
        }
        if (dot < 0) {
            return source;
        }
        return mapper.getValue(source, scopedPath.substring(dot + 1));
    }
}
