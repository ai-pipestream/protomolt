package ai.protomolt.proto.mapper;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Package-private path segmentation, literal parsing, {@link Any} message conversion, and
 * parent container descent helper for {@link ProtoFieldMapperImpl}.
 */
final class FieldPathResolver {

    private static final String ANY_FULL_NAME = Any.getDescriptor().getFullName();

    private FieldPathResolver() {}

    /**
     * Splits {@code path} on {@code '.'} without regex, matching {@link String#split(String)}
     * (limit {@code 0}) semantics: trailing empty segments are discarded unless the input is
     * itself empty, in which case a single empty segment {@code [""]} is returned.
     */
    static String[] splitPath(String path) {
        if (path.isEmpty()) {
            return new String[]{""};
        }
        List<String> parts = new ArrayList<>();
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            if (dot < 0) {
                parts.add(path.substring(start));
                break;
            }
            parts.add(path.substring(start, dot));
            start = dot + 1;
        }
        int end = parts.size();
        while (end > 0 && parts.get(end - 1).isEmpty()) {
            end--;
        }
        return parts.subList(0, end).toArray(String[]::new);
    }

    /**
     * Evaluates {@code trimmedPath} as a literal ({@code null}, {@code true}, {@code false},
     * double-quoted string, integer, or decimal), returning {@link LiteralResult#NOT_A_LITERAL}
     * when {@code trimmedPath} is a field path rather than a literal.
     */
    static LiteralResult tryParseLiteral(String trimmedPath, String rule) throws MappingException {
        switch (trimmedPath) {
            case "null" -> {
                return LiteralResult.of(null);
            }
            case "true" -> {
                return LiteralResult.of(true);
            }
            case "false" -> {
                return LiteralResult.of(false);
            }
            default -> {
            }
        }
        if (trimmedPath.startsWith("\"") && trimmedPath.endsWith("\"")) {
            if (trimmedPath.length() == 1) {
                throw new MappingException("Invalid empty quoted string literal", rule);
            }
            return LiteralResult.of(trimmedPath.substring(1, trimmedPath.length() - 1));
        }
        if (isNumericLiteral(trimmedPath)) {
            try {
                if (trimmedPath.indexOf('.') >= 0) {
                    return LiteralResult.of(Double.parseDouble(trimmedPath));
                }
                return LiteralResult.of(Long.parseLong(trimmedPath));
            } catch (NumberFormatException ignored) {
                // Out-of-range numeric token falls through to path resolution.
            }
        }
        return LiteralResult.NOT_A_LITERAL;
    }

    /**
     * Matches an optional leading {@code '-'}, one or more ASCII digits, and an optional
     * {@code '.'} followed by one or more ASCII digits (equivalent to {@code -?\d+(\.\d+)?}).
     */
    static boolean isNumericLiteral(String value) {
        int length = value.length();
        if (length == 0) {
            return false;
        }
        int index = value.charAt(0) == '-' ? 1 : 0;
        int intDigits = 0;
        while (index < length && isAsciiDigit(value.charAt(index))) {
            intDigits++;
            index++;
        }
        if (intDigits == 0) {
            return false;
        }
        if (index == length) {
            return true;
        }
        if (value.charAt(index) != '.') {
            return false;
        }
        index++;
        int fracDigits = 0;
        while (index < length && isAsciiDigit(value.charAt(index))) {
            fracDigits++;
            index++;
        }
        return fracDigits > 0 && index == length;
    }

    private static boolean isAsciiDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }

    /**
     * Looks up field {@code name} on {@code descriptor}, throwing {@link MappingException} when
     * absent.
     */
    static FieldDescriptor findField(Descriptor descriptor, String name, String fullPath)
            throws MappingException {
        FieldDescriptor fd = descriptor.findFieldByName(name);
        if (fd == null) {
            throw new MappingException(
                    "Field '" + name + "' not found in message '" + descriptor.getName() + "'",
                    fullPath);
        }
        return fd;
    }

    /**
     * Detects {@code google.protobuf.Any} values by descriptor full name, so that
     * {@code DynamicMessage} instances of {@code Any} are recognised alongside concrete
     * {@link Any} objects.
     */
    static boolean isAnyMessage(Object value) {
        return value instanceof MessageOrBuilder messageOrBuilder
                && messageOrBuilder.getDescriptorForType().getFullName().equals(ANY_FULL_NAME);
    }

    /**
     * Converts a value recognised by {@link #isAnyMessage(Object)} into a concrete {@link Any}.
     */
    static Any toAny(Object value, String rule) throws MappingException {
        if (value instanceof Any any) {
            return any;
        }
        Message message = value instanceof Message.Builder builder ? builder.build() : (Message) value;
        try {
            return Any.parseFrom(message.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to convert dynamic google.protobuf.Any message", e, rule);
        }
    }

    /**
     * Reports whether {@code fd} is a message field whose message type is
     * {@code google.protobuf.Any}.
     */
    static boolean isAnyField(FieldDescriptor fd) {
        return fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && fd.getMessageType().getFullName().equals(ANY_FULL_NAME);
    }

    /**
     * Walks the parent segments of {@code path}, returning the builder that owns the final segment.
     * Once traversal reaches a {@code google.protobuf.Struct} field, the remaining segments are
     * Struct keys and are returned for the caller to resolve, mirroring the read-path semantics.
     *
     * @param materializeParents when {@code true}, unset intermediate parents are created;
     *        when {@code false} (clear operations), an unset parent returns {@code null}
     *        so the caller can treat the operation as a no-op.
     */
    static PathResolutionResult resolvePathToFinalContainer(
            Message.Builder root,
            String path,
            String rule,
            boolean materializeParents) throws MappingException {
        String[] parts = splitPath(path);
        Message.Builder currentBuilder = root;
        List<Message.Builder> parents = new ArrayList<>();
        List<FieldDescriptor> descent = new ArrayList<>();

        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (StructValueCodec.isStructDescriptor(currentBuilder.getDescriptorForType())) {
                return new PathResolutionResult(
                        currentBuilder,
                        Arrays.copyOfRange(parts, i, parts.length),
                        parents,
                        descent);
            }
            FieldDescriptor fd = findField(currentBuilder.getDescriptorForType(), part, rule);

            if (fd.isRepeated() || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new MappingException(
                        "Path '" + path + "' attempts to traverse through non-singular message field '" + part + "'",
                        rule);
            }
            if (!materializeParents && !currentBuilder.hasField(fd)) {
                return null;
            }
            // Detached descent: DynamicMessage builders do not support getFieldBuilder,
            // so every level descends on a copy; commit() folds the chain back.
            parents.add(currentBuilder);
            descent.add(fd);
            currentBuilder = ((Message) currentBuilder.getField(fd)).toBuilder();
        }
        return new PathResolutionResult(
                currentBuilder,
                new String[]{parts[parts.length - 1]},
                parents,
                descent);
    }

    /**
     * Outcome of attempting to parse a source token as a literal value.
     */
    record LiteralResult(boolean matched, Object value) {
        static final LiteralResult NOT_A_LITERAL = new LiteralResult(false, null);

        static LiteralResult of(Object value) {
            return new LiteralResult(true, value);
        }
    }

    /**
     * Resolved parent container and leaf key/field for a write, append, or clear operation.
     */
    static final class PathResolutionResult {
        final Object container;
        final String finalPathPart;
        /** When the container is a Struct: the chain of struct keys (ending with {@link #finalPathPart}). */
        final String[] structKeys;
        /** Ancestors of a detached container, root first, with the field descended at each. */
        private final List<Message.Builder> parents;
        private final List<FieldDescriptor> descent;

        PathResolutionResult(
                Object container,
                String[] structKeys,
                List<Message.Builder> parents,
                List<FieldDescriptor> descent) {
            this.container = container;
            this.structKeys = structKeys;
            this.finalPathPart = structKeys[structKeys.length - 1];
            this.parents = parents;
            this.descent = descent;
        }

        /**
         * Folds a detached nested container back into its ancestors. Descent is done on
         * builder copies because {@code DynamicMessage.Builder} does not support
         * {@code getFieldBuilder}; every mutation therefore commits explicitly. A
         * top-level container has no ancestors and commits as a no-op.
         */
        void commit() {
            if (parents.isEmpty()) {
                return;
            }
            Message.Builder child = (Message.Builder) container;
            for (int i = parents.size() - 1; i >= 0; i--) {
                parents.get(i).setField(descent.get(i), child.build());
                child = parents.get(i);
            }
        }
    }
}
