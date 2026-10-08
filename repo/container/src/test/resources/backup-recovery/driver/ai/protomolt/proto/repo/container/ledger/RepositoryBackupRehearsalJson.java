package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Small private JSON documents (manifests, identity records) over protobuf Struct; no new wire
 * contract. Ported from the draft PR #413 recovery harness (verification/repository-recovery).
 */
final class RepositoryBackupRehearsalJson {
    private RepositoryBackupRehearsalJson() {}

    @SuppressWarnings("unchecked")
    static Value value(Object object) {
        if (object == null) return Value.newBuilder().setNullValueValue(0).build();
        if (object instanceof String s) return Value.newBuilder().setStringValue(s).build();
        if (object instanceof Boolean b) return Value.newBuilder().setBoolValue(b).build();
        if (object instanceof Number n) {
            // Keep identifiers exact: large longs are strings, small ones numbers.
            if (n instanceof Long l && (l > 9_007_199_254_740_991L || l < -9_007_199_254_740_991L))
                return Value.newBuilder().setStringValue(Long.toString(l)).build();
            return Value.newBuilder().setNumberValue(n.doubleValue()).build();
        }
        if (object instanceof Map<?, ?> map) {
            var struct = Struct.newBuilder();
            ((Map<String, Object>) map).forEach((key, item) -> struct.putFields(key, value(item)));
            return Value.newBuilder().setStructValue(struct).build();
        }
        if (object instanceof List<?> list) {
            var values = ListValue.newBuilder();
            list.forEach(item -> values.addValues(value(item)));
            return Value.newBuilder().setListValue(values).build();
        }
        throw new IllegalArgumentException("Unsupported JSON value " + object.getClass());
    }

    static Object plain(Value value) {
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case NUMBER_VALUE -> value.getNumberValue();
            case STRUCT_VALUE -> {
                var map = new LinkedHashMap<String, Object>();
                value.getStructValue().getFieldsMap().forEach((key, item) -> map.put(key, plain(item)));
                yield map;
            }
            case LIST_VALUE -> {
                var list = new ArrayList<>();
                value.getListValue().getValuesList().forEach(item -> list.add(plain(item)));
                yield list;
            }
            default -> null;
        };
    }

    static String write(Map<String, Object> document) {
        try { return JsonFormat.printer().print(value(document).getStructValue()); }
        catch (InvalidProtocolBufferException failure) { throw new IllegalStateException(failure); }
    }

    static void write(Path file, Map<String, Object> document) {
        try { Files.writeString(file, write(document) + "\n", StandardCharsets.UTF_8); }
        catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot write " + file, failure); }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> read(Path file) {
        try {
            var struct = Struct.newBuilder();
            JsonFormat.parser().merge(Files.readString(file, StandardCharsets.UTF_8), struct);
            return (Map<String, Object>) plain(Value.newBuilder().setStructValue(struct).build());
        } catch (IOException failure) { throw new RepositoryBackupRehearsalFailure("Cannot read " + file, failure); }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof Map<?, ?>)) throw new RepositoryBackupRehearsalFailure("Missing object '" + key + "'");
        return (Map<String, Object>) value;
    }

    static String string(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (!(value instanceof String s) || s.isEmpty()) throw new RepositoryBackupRehearsalFailure("Missing string '" + key + "'");
        return s;
    }

    static long number(Map<String, Object> parent, String key) {
        var value = parent.get(key);
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) return Long.parseLong(s);
        throw new RepositoryBackupRehearsalFailure("Missing number '" + key + "'");
    }

    /** The JSON round-trip shape of a value, so in-memory records compare equal to re-read ones. */
    static Object normalize(Object object) { return plain(value(object)); }
}
