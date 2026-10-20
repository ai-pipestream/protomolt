package ai.protomolt.proto.serve;

import ai.protomolt.proto.inference.spi.CredentialResolutionException;
import ai.protomolt.proto.inference.spi.CredentialResolver;
import ai.protomolt.proto.inference.spi.InferenceCatalog;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the inference facade from {@code --inference-model} specs
 * ({@code id|provider|endpoint[|backend[|labels[|capabilities[|credential-ref]]]]}).
 * Supported capability tokens are {@code streaming}, {@code thinking}, and
 * {@code structured-output}. The credential reference is an opaque pointer
 * (e.g. {@code env:OPENAI_TOKEN}) resolved host-side at request time, never
 * credential material; a malformed reference fails startup. Empty specs mean
 * inference is not configured (null; the verbs answer {@code unavailable}).
 * A bad spec or an unknown provider fails startup loud. A model the server
 * cannot execute must never sit in the catalog looking runnable.
 */
final class InferenceMount {

    private InferenceMount() {
    }

    /**
     * Parses every spec into a catalog entry and returns the engines over them, or null when
     * no spec was given. Diagnostics redact the credential reference field.
     */
    static InferenceEngines engines(List<String> specs) {
        if (specs == null || specs.isEmpty()) {
            return null;
        }
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog);
        for (String spec : specs) {
            List<String> parts = fields(spec, '|');
            String displaySpec = redactCredentialReference(spec);
            if (parts.size() < 3 || parts.size() > 7
                    || parts.get(0).isBlank() || parts.get(1).isBlank() || parts.get(2).isBlank()) {
                throw new IllegalArgumentException("bad --inference-model spec '" + displaySpec
                        + "' (want id|provider|endpoint[|backend[|labels[|capabilities"
                        + "[|credential-ref]]]])");
            }
            ModelEntry.Builder entry = ModelEntry.newBuilder()
                    .setId(parts.get(0).trim())
                    .setProvider(parts.get(1).trim())
                    .setEndpoint(parts.get(2).trim());
            if (parts.size() >= 4 && !parts.get(3).isBlank()) {
                entry.setBackendModel(parts.get(3).trim());
            }
            if (parts.size() >= 5 && !parts.get(4).isBlank()) {
                for (String label : fieldsDropTrailingEmpty(parts.get(4), ',')) {
                    int colon = label.indexOf(':');
                    if (colon < 0 || label.substring(0, colon).isBlank()) {
                        throw new IllegalArgumentException("bad label '" + label
                                + "' in --inference-model spec '" + displaySpec
                                + "' (want k:v)");
                    }
                    entry.putLabels(label.substring(0, colon).trim(),
                            label.substring(colon + 1).trim());
                }
            }
            if (parts.size() >= 6 && !parts.get(5).isBlank()) {
                ModelCapabilities.Builder capabilities = ModelCapabilities.newBuilder();
                for (String capability : fieldsDropTrailingEmpty(parts.get(5), ',')) {
                    switch (capability.trim()) {
                        case "streaming" -> capabilities.setStreaming(true);
                        case "thinking" -> capabilities.setThinking(true);
                        case "structured-output" -> capabilities.setStructuredOutput(true);
                        default -> throw new IllegalArgumentException("unknown capability '"
                                + capability + "' in --inference-model spec '"
                                + displaySpec + "'");
                    }
                }
                entry.setCapabilities(capabilities);
            }
            if (parts.size() == 7 && !parts.get(6).isBlank()) {
                String credentialRef = parts.get(6).trim();
                try {
                    CredentialResolver.checkFormat(credentialRef);
                } catch (CredentialResolutionException e) {
                    throw new IllegalArgumentException("bad credential reference in "
                            + "--inference-model spec '" + displaySpec + "': "
                            + e.getMessage(), e);
                }
                entry.setCredentialRef(credentialRef);
            }
            engines.register(entry.build());
        }
        return engines;
    }

    /** Keeps a secret-sensitive credential reference out of launcher diagnostics. */
    private static String redactCredentialReference(String spec) {
        List<String> parts = fields(spec, '|');
        if (parts.size() < 7) {
            return spec;
        }
        List<String> redacted = new ArrayList<>(parts);
        redacted.set(6, "<credential-ref>");
        for (int i = 7; i < redacted.size(); i++) {
            redacted.set(i, "<redacted>");
        }
        return String.join("|", redacted);
    }

    /**
     * Splits {@code value} on the literal separator character, keeping every field including
     * empty and trailing ones (the semantics of {@code String.split} with limit -1, without
     * regex).
     */
    private static List<String> fields(String value, char separator) {
        List<String> fields = new ArrayList<>();
        int start = 0;
        int at;
        while ((at = value.indexOf(separator, start)) >= 0) {
            fields.add(value.substring(start, at));
            start = at + 1;
        }
        fields.add(value.substring(start));
        return fields;
    }

    /**
     * Splits on the literal separator character, dropping trailing empty fields (the semantics
     * of {@code String.split} with limit 0, without regex). An empty input yields a single
     * empty field, as {@code "".split} does.
     */
    private static List<String> fieldsDropTrailingEmpty(String value, char separator) {
        if (value.isEmpty()) {
            return List.of("");
        }
        List<String> fields = fields(value, separator);
        int last = fields.size();
        while (last > 0 && fields.get(last - 1).isEmpty()) {
            last--;
        }
        return fields.subList(0, last);
    }
}
