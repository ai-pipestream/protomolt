package ai.pipestream.proto.openapi;

import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiToken;
import ai.pipestream.proto.rest.ProtoRestExposed;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds an OpenAPI 3.x document from registered {@link ProtoRestMethod}s
 * (and optionally their protobuf descriptors).
 *
 * <p>Codegen-friendly: emit JSON once at build/startup and serve {@code /openapi.json}.
 * Honors {@link ai.pipestream.proto.rest.ProtoApiToken} / {@link ProtoRestExposed} when present.
 */
public final class ProtoOpenApiGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoOpenApiGenerator.class);

    public static final String DEFAULT_SECURITY_SCHEME = "ApiToken";

    private final ObjectMapper mapper;
    private final String title;
    private final String version;
    private final String serverUrl;
    private final String pathPrefix;

    public ProtoOpenApiGenerator() {
        this("Protobuf REST Gateway", "1.0.0", "/", "/grpc-json");
    }

    public ProtoOpenApiGenerator(String title, String version, String serverUrl, String pathPrefix) {
        this.title = Objects.requireNonNull(title, "title");
        this.version = Objects.requireNonNull(version, "version");
        this.serverUrl = Objects.requireNonNull(serverUrl, "serverUrl");
        this.pathPrefix = normalizePrefix(pathPrefix);
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    public Map<String, Object> generate(ProtoRestMethodRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("openapi", "3.0.3");
        doc.put("info", Map.of(
                "title", title,
                "version", version,
                "description", "JSON/REST surface generated from protobuf service descriptors"));
        doc.put("servers", List.of(Map.of("url", serverUrl)));

        Map<String, Object> components = new LinkedHashMap<>();
        Map<String, Object> schemas = new LinkedHashMap<>();
        Map<String, Object> securitySchemes = new LinkedHashMap<>();
        Map<String, Object> paths = new LinkedHashMap<>();

        for (ProtoRestMethod method : registry.all()) {
            String path = resolvePath(method);
            @SuppressWarnings("unchecked")
            Map<String, Object> pathItem = (Map<String, Object>) paths.computeIfAbsent(path, k -> new LinkedHashMap<>());

            String[] httpMethods = resolveHttpMethods(method);

            Descriptor requestDesc = resolveRequestDescriptor(method);
            Descriptor responseDesc = resolveResponseDescriptor(method);
            if (requestDesc != null) {
                addMessageSchema(schemas, requestDesc, new LinkedHashSet<>());
            }
            if (responseDesc != null) {
                addMessageSchema(schemas, responseDesc, new LinkedHashSet<>());
            }

            for (String httpMethod : httpMethods) {
                Map<String, Object> operation = new LinkedHashMap<>();
                operation.put("operationId", method.serviceName() + "_" + method.methodName());

                String summary = method.summary()
                        .or(() -> method.exposed().map(ProtoRestExposed::summary).filter(s -> !s.isBlank()))
                        .orElse(method.serviceName() + "." + method.methodName());
                operation.put("summary", summary);

                method.description()
                        .or(() -> method.exposed().map(ProtoRestExposed::description).filter(s -> !s.isBlank()))
                        .ifPresent(d -> operation.put("description", d));

                if (requestDesc != null) {
                    operation.put("requestBody", Map.of(
                            "required", true,
                            "content", Map.of(
                                    "application/json", Map.of(
                                            "schema", ref(requestDesc.getFullName())))));
                }

                Map<String, Object> responses = new LinkedHashMap<>();
                Map<String, Object> ok = new LinkedHashMap<>();
                ok.put("description", "Successful response");
                if (responseDesc != null) {
                    ok.put("content", Map.of(
                            "application/json", Map.of(
                                    "schema", ref(responseDesc.getFullName()))));
                }
                responses.put("200", ok);
                responses.put("400", Map.of("description", "Malformed JSON / bad request"));
                responses.put("401", Map.of("description", "Missing or invalid API token"));
                responses.put("404", Map.of("description", "Service or method not found"));
                operation.put("responses", responses);

                method.apiToken().ifPresent(token -> {
                    String schemeName = ensureSecurityScheme(securitySchemes, token);
                    if (token.required()) {
                        operation.put("security", List.of(Map.of(schemeName, List.of())));
                    }
                });

                pathItem.put(httpMethod.toLowerCase(), operation);
            }
        }

        components.put("schemas", schemas);
        if (!securitySchemes.isEmpty()) {
            components.put("securitySchemes", securitySchemes);
        }
        doc.put("components", components);
        doc.put("paths", paths);
        return doc;
    }

    public String generateJson(ProtoRestMethodRegistry registry) {
        try {
            return mapper.writeValueAsString(generate(registry));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OpenAPI document", e);
        }
    }

    private String resolvePath(ProtoRestMethod method) {
        if (method.path().isPresent() && !method.path().get().isBlank()) {
            String custom = method.path().get();
            return custom.startsWith("/") ? custom : pathPrefix + "/" + custom;
        }
        if (method.exposed().isPresent() && !method.exposed().get().path().isBlank()) {
            String custom = method.exposed().get().path();
            return custom.startsWith("/") ? custom : pathPrefix + "/" + custom;
        }
        return pathPrefix + "/" + method.serviceName() + "/" + method.methodName();
    }

    private static String[] resolveHttpMethods(ProtoRestMethod method) {
        if (method.httpMethods() != null && method.httpMethods().length > 0) {
            return method.httpMethods();
        }
        return method.exposed()
                .map(ProtoRestExposed::httpMethods)
                .filter(m -> m.length > 0)
                .orElse(new String[]{"POST"});
    }

    private static Descriptor resolveRequestDescriptor(ProtoRestMethod method) {
        if (method.methodDescriptor() != null) {
            return method.methodDescriptor().getInputType();
        }
        return method.requestType()
                .map(c -> {
                    try {
                        return (Descriptor) c.getMethod("getDescriptor").invoke(null);
                    } catch (ReflectiveOperationException e) {
                        LOG.warn("Could not resolve request descriptor for {}.{}: {}.getDescriptor() failed; "
                                        + "the operation is emitted without a requestBody schema",
                                method.serviceName(), method.methodName(), c.getName(), e);
                        return null;
                    }
                })
                .orElse(null);
    }

    private static Descriptor resolveResponseDescriptor(ProtoRestMethod method) {
        MethodDescriptor md = method.methodDescriptor();
        return md == null ? null : md.getOutputType();
    }

    private static String ensureSecurityScheme(Map<String, Object> schemes, ApiTokenRequirement token) {
        Map<String, Object> scheme = new LinkedHashMap<>();
        if (token.scheme() == ProtoApiToken.Scheme.HTTP) {
            scheme.put("type", "http");
            scheme.put("scheme", token.httpScheme());
        } else {
            scheme.put("type", "apiKey");
            scheme.put("name", token.name());
            scheme.put("in", token.in().name().toLowerCase());
        }
        if (!token.description().isBlank()) {
            scheme.put("description", token.description());
        }
        // Each distinct token config gets its own scheme; identical configs share one.
        for (Map.Entry<String, Object> existing : schemes.entrySet()) {
            if (existing.getValue().equals(scheme)) {
                return existing.getKey();
            }
        }
        String name = schemes.isEmpty()
                ? DEFAULT_SECURITY_SCHEME
                : DEFAULT_SECURITY_SCHEME + "_" + (schemes.size() + 1);
        schemes.put(name, scheme);
        return name;
    }

    private static Map<String, Object> ref(String fullName) {
        return Map.of("$ref", "#/components/schemas/" + schemaKey(fullName));
    }

    private static String schemaKey(String fullName) {
        return fullName.replace('.', '_');
    }

    private static void addMessageSchema(
            Map<String, Object> schemas,
            Descriptor descriptor,
            Set<String> visiting) {
        String key = schemaKey(descriptor.getFullName());
        if (schemas.containsKey(key) || !visiting.add(descriptor.getFullName())) {
            return;
        }

        // Well-known types are (de)serialized by JsonFormat as special JSON forms, never as
        // objects of their internal fields; the document must describe the JsonFormat form.
        Map<String, Object> wellKnown = wellKnownSchema(descriptor);
        if (wellKnown != null) {
            schemas.put(key, wellKnown);
            visiting.remove(descriptor.getFullName());
            return;
        }

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("title", descriptor.getFullName());
        Map<String, Object> properties = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();

        for (FieldDescriptor field : descriptor.getFields()) {
            String jsonName = field.getJsonName();
            properties.put(jsonName, fieldSchema(field, schemas, visiting));
            if (field.isRequired()) {
                required.add(jsonName);
            }
        }
        schema.put("properties", properties);
        if (!required.isEmpty()) {
            schema.put("required", required);
        }
        schemas.put(key, schema);
        visiting.remove(descriptor.getFullName());
    }

    private static Map<String, Object> fieldSchema(
            FieldDescriptor field,
            Map<String, Object> schemas,
            Set<String> visiting) {
        if (field.isMapField()) {
            FieldDescriptor value = field.getMessageType().findFieldByName("value");
            Map<String, Object> mapSchema = new LinkedHashMap<>();
            mapSchema.put("type", "object");
            mapSchema.put("additionalProperties", value == null
                    ? Map.of("type", "string")
                    : scalarOrMessage(value, schemas, visiting));
            return mapSchema;
        }
        Map<String, Object> base = scalarOrMessage(field, schemas, visiting);
        if (field.isRepeated()) {
            Map<String, Object> array = new LinkedHashMap<>();
            array.put("type", "array");
            array.put("items", base);
            return array;
        }
        return base;
    }

    private static Map<String, Object> scalarOrMessage(
            FieldDescriptor field,
            Map<String, Object> schemas,
            Set<String> visiting) {
        return switch (field.getJavaType()) {
            case MESSAGE -> {
                Map<String, Object> wellKnown = wellKnownSchema(field.getMessageType());
                if (wellKnown != null) {
                    yield wellKnown;
                }
                addMessageSchema(schemas, field.getMessageType(), visiting);
                yield ref(field.getMessageType().getFullName());
            }
            case ENUM -> enumSchema(field.getEnumType());
            case INT -> Map.of("type", "integer", "format", "int32");
            // JsonFormat prints 64-bit integers as JSON strings (proto3 JSON spec).
            case LONG -> Map.of("type", "string", "format", isUnsigned64(field) ? "uint64" : "int64");
            case FLOAT -> Map.of("type", "number", "format", "float");
            case DOUBLE -> Map.of("type", "number", "format", "double");
            case BOOLEAN -> Map.of("type", "boolean");
            case BYTE_STRING -> Map.of("type", "string", "format", "byte");
            case STRING -> Map.of("type", "string");
        };
    }

    private static boolean isUnsigned64(FieldDescriptor field) {
        return field.getType() == FieldDescriptor.Type.UINT64
                || field.getType() == FieldDescriptor.Type.FIXED64;
    }

    /**
     * Schema for the canonical proto3 JSON encoding of an enum: JsonFormat prints declared
     * values as their names but unrecognized values as bare numbers, and accepts both forms.
     */
    private static Map<String, Object> enumSchema(EnumDescriptor enumType) {
        if ("google.protobuf.NullValue".equals(enumType.getFullName())) {
            // JsonFormat prints NullValue as JSON null.
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("nullable", true);
            schema.put("enum", Collections.singletonList(null));
            return schema;
        }
        List<String> names = enumType.getValues().stream()
                .map(EnumValueDescriptor::getName)
                .toList();
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("anyOf", List.of(
                schemaOf("type", "string", "enum", names),
                schemaOf("type", "integer", "format", "int32")));
        return schema;
    }

    /**
     * JsonFormat form of a well-known type, or {@code null} for ordinary messages.
     * Mirrors the table in {@code ProtoJsonSchemaGenerator.messageRef}, spelled in
     * OpenAPI 3.0 vocabulary ({@code nullable} instead of {@code type: "null"}).
     */
    private static Map<String, Object> wellKnownSchema(Descriptor type) {
        return switch (type.getFullName()) {
            case "google.protobuf.Timestamp" -> schemaOf("type", "string", "format", "date-time");
            case "google.protobuf.Duration" ->
                    schemaOf("type", "string", "pattern", "^-?[0-9]+(\\.[0-9]{1,9})?s$");
            case "google.protobuf.Struct" -> schemaOf("type", "object");
            // google.protobuf.Value is any JSON value: the empty schema.
            case "google.protobuf.Value" -> new LinkedHashMap<>();
            case "google.protobuf.ListValue" ->
                    schemaOf("type", "array", "items", new LinkedHashMap<>());
            case "google.protobuf.FieldMask" -> schemaOf("type", "string");
            case "google.protobuf.Any" -> schemaOf(
                    "type", "object",
                    "properties", schemaOf("@type", schemaOf("type", "string")));
            case "google.protobuf.StringValue" -> nullable(schemaOf("type", "string"));
            case "google.protobuf.BytesValue" ->
                    nullable(schemaOf("type", "string", "format", "byte"));
            case "google.protobuf.BoolValue" -> nullable(schemaOf("type", "boolean"));
            case "google.protobuf.Int32Value" ->
                    nullable(schemaOf("type", "integer", "format", "int32"));
            case "google.protobuf.UInt32Value" ->
                    nullable(schemaOf("type", "integer", "minimum", 0L));
            // JsonFormat prints 64-bit wrapper values as JSON strings (proto3 JSON spec).
            case "google.protobuf.Int64Value" ->
                    nullable(schemaOf("type", "string", "format", "int64"));
            case "google.protobuf.UInt64Value" ->
                    nullable(schemaOf("type", "string", "format", "uint64"));
            case "google.protobuf.FloatValue" ->
                    nullable(schemaOf("type", "number", "format", "float"));
            case "google.protobuf.DoubleValue" ->
                    nullable(schemaOf("type", "number", "format", "double"));
            default -> null;
        };
    }

    private static Map<String, Object> nullable(Map<String, Object> schema) {
        schema.put("nullable", true);
        return schema;
    }

    private static Map<String, Object> schemaOf(Object... pairs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }

    private static String normalizePrefix(String pathPrefix) {
        if (pathPrefix == null || pathPrefix.isBlank() || "/".equals(pathPrefix)) {
            return "";
        }
        String p = pathPrefix.startsWith("/") ? pathPrefix : "/" + pathPrefix;
        while (p.endsWith("/") && p.length() > 1) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }
}
