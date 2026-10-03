package ai.protomolt.proto.repo.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Strict environment decoding; duplicate keys and non-string targets are configuration errors. */
final class RemoteBucketBindings {
    private RemoteBucketBindings() {}

    static Map<String, String> parse(String json) {
        if (json == null || json.isBlank()) return Map.of();
        var mapper = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        try {
            var root = mapper.readTree(json);
            if (!root.isObject()) throw new IllegalArgumentException("Remote bucket bindings must be a JSON object");
            var result = new HashMap<String, String>();
            var targets = new java.util.HashSet<String>();
            var fields = root.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                if (field.getKey().isBlank() || !field.getValue().isTextual()
                        || field.getValue().textValue().isBlank()
                        || !targets.add(field.getValue().textValue()))
                    throw new IllegalArgumentException("Remote bucket bindings must be nonblank and one-to-one");
                result.put(field.getKey(), field.getValue().textValue());
            }
            return Map.copyOf(result);
        } catch (IOException invalid) {
            // Do not echo environment contents in configuration errors.
            throw new IllegalArgumentException("Invalid remote bucket bindings JSON");
        }
    }
}
