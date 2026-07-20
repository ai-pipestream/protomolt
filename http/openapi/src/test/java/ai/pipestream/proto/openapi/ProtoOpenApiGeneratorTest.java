package ai.pipestream.proto.openapi;

import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiToken;
import ai.pipestream.proto.rest.ProtoRestMethod;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DurationProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.Struct;
import com.google.protobuf.StructProto;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.WrappersProto;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoOpenApiGeneratorTest {

    @Test
    void generatesOpenApiWithSecurityAndSchemas() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("EchoService", "Echo", req -> req)
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.apiKeyHeader("api_token"))
                .summary("Echo")
                .build());

        ProtoOpenApiGenerator generator = new ProtoOpenApiGenerator(
                "Test API", "0.1.0", "http://localhost:8080", "/grpc-json");
        Map<String, Object> doc = generator.generate(registry);

        assertThat(doc.get("openapi")).isEqualTo("3.0.3");
        assertThat(doc).containsKey("paths");
        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) doc.get("paths");
        assertThat(paths).containsKey("/grpc-json/EchoService/Echo");

        @SuppressWarnings("unchecked")
        Map<String, Object> components = (Map<String, Object>) doc.get("components");
        assertThat(components).containsKeys("schemas", "securitySchemes");
        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = (Map<String, Object>) components.get("schemas");
        assertThat(schemas).containsKey("google_protobuf_Struct");

        String json = generator.generateJson(registry);
        assertThat(json).contains("\"openapi\" : \"3.0.3\"").contains("ApiToken");
    }

    @Test
    void specAndRouterShareOneRouteAndVerbModel() {
        // The published contract must describe exactly what the hosts serve: the
        // canonical {prefix}/{service}/{method} path, and the verbs the gateway itself
        // will enforce via allowedHttpVerbs() - POST when nothing is declared.
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("Plain", "Go", req -> req)
                .requestType(Struct.class)
                .build());
        registry.register(ProtoRestMethod.builder("Declared", "Fetch", req -> req)
                .requestType(Struct.class)
                .httpMethods("GET", "POST")
                .build());

        Map<String, Object> doc = new ProtoOpenApiGenerator(
                "Test API", "0.1.0", "http://localhost:8080", "/grpc-json")
                .generate(registry);
        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) doc.get("paths");

        assertThat(paths.keySet()).containsExactlyInAnyOrder(
                "/grpc-json/Plain/Go", "/grpc-json/Declared/Fetch");
        for (ProtoRestMethod method : registry.all()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> item = (Map<String, Object>) paths.get(
                    "/grpc-json/" + method.serviceName() + "/" + method.methodName());
            assertThat(item.keySet())
                    .as("spec verbs for %s must be what the gateway enforces", method.routeKey())
                    .containsExactlyInAnyOrderElementsOf(method.allowedHttpVerbs().stream()
                            .map(verb -> verb.toLowerCase(Locale.ROOT)).toList());
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> plain = (Map<String, Object>) paths.get("/grpc-json/Plain/Go");
        assertThat(plain.keySet()).containsExactly("post");
    }

    /**
     * operationId must be unique across the whole document — client generators key their
     * method names off it, and a collision silently drops one of the operations.
     */
    @Test
    void operationIdsAreUniqueWhenAMethodDeclaresSeveralVerbs() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("Plain", "Go", req -> req)
                .requestType(Struct.class)
                .build());
        registry.register(ProtoRestMethod.builder("Declared", "Fetch", req -> req)
                .requestType(Struct.class)
                .httpMethods("GET", "POST")
                .build());

        Map<String, Object> doc = new ProtoOpenApiGenerator().generate(registry);

        assertThat(operationIdsOf(doc))
                .doesNotHaveDuplicates()
                .containsExactlyInAnyOrder("Plain_Go", "Declared_Fetch_get", "Declared_Fetch_post");
    }

    /**
     * Path-item verb keys are protocol tokens, not display text: a locale-sensitive
     * lowercase turns a declared verb such as LINK into "lınk" under a Turkish default
     * locale, which no OpenAPI consumer recognizes.
     */
    @Test
    void verbKeysAreLowercasedIndependentlyOfTheDefaultLocale() {
        Locale original = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr"));
        try {
            ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
            registry.register(ProtoRestMethod.builder("Linker", "Bind", req -> req)
                    .requestType(Struct.class)
                    .httpMethods("LINK")
                    .build());

            Map<String, Object> doc = new ProtoOpenApiGenerator().generate(registry);
            @SuppressWarnings("unchecked")
            Map<String, Object> paths = (Map<String, Object>) doc.get("paths");
            @SuppressWarnings("unchecked")
            Map<String, Object> item = (Map<String, Object>) paths.get("/grpc-json/Linker/Bind");
            assertThat(item.keySet()).containsExactly("link");
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    void registersDistinctSecuritySchemePerTokenConfig() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("BearerService", "Ping", req -> req)
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.bearer())
                .build());
        registry.register(ProtoRestMethod.builder("QueryKeyService", "Ping", req -> req)
                .requestType(Struct.class)
                .apiToken(new ApiTokenRequirement(
                        "api_token",
                        ProtoApiToken.In.QUERY,
                        ProtoApiToken.Scheme.API_KEY,
                        null,
                        true,
                        "API access token"))
                .build());
        // Same config as BearerService — must reuse its scheme, not mint a third.
        registry.register(ProtoRestMethod.builder("BearerTwinService", "Ping", req -> req)
                .requestType(Struct.class)
                .apiToken(ApiTokenRequirement.bearer())
                .build());

        Map<String, Object> doc = new ProtoOpenApiGenerator().generate(registry);

        @SuppressWarnings("unchecked")
        Map<String, Object> components = (Map<String, Object>) doc.get("components");
        @SuppressWarnings("unchecked")
        Map<String, Object> schemes = (Map<String, Object>) components.get("securitySchemes");
        assertThat(schemes.keySet()).containsExactlyInAnyOrder("ApiToken", "ApiToken_2");

        String bearerRef = securityRefOf(doc, "/grpc-json/BearerService/Ping");
        String queryRef = securityRefOf(doc, "/grpc-json/QueryKeyService/Ping");
        assertThat(bearerRef).isNotEqualTo(queryRef);
        assertThat((Map<String, Object>) schemes.get(bearerRef))
                .containsEntry("type", "http")
                .containsEntry("scheme", "bearer");
        assertThat((Map<String, Object>) schemes.get(queryRef))
                .containsEntry("type", "apiKey")
                .containsEntry("name", "api_token")
                .containsEntry("in", "query");
        // Identical configs share one scheme instead of minting a third.
        assertThat(securityRefOf(doc, "/grpc-json/BearerTwinService/Ping")).isEqualTo(bearerRef);
    }

    @Test
    void topLevelWellKnownRequestTypesUseJsonFormatForms() {
        // JsonFormat serves these as a string / primitive, never as their internal fields.
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("Clock", "now", req -> req)
                .requestType(Timestamp.class)
                .build());
        registry.register(ProtoRestMethod.builder("Counter", "get", req -> req)
                .requestType(UInt64Value.class)
                .build());

        Map<String, Object> schemas = schemasOf(new ProtoOpenApiGenerator().generate(registry));

        assertThat(schemaOf(schemas, "google_protobuf_Timestamp"))
                .containsEntry("type", "string")
                .containsEntry("format", "date-time")
                .doesNotContainKey("properties");
        assertThat(schemaOf(schemas, "google_protobuf_UInt64Value"))
                .containsEntry("type", "string")
                .containsEntry("format", "uint64")
                .containsEntry("nullable", true)
                .doesNotContainKey("properties");
    }

    @Test
    void mapFieldsDoNotEmitSyntheticEntrySchemas() throws Exception {
        Map<String, Object> schemas = schemasOf(new ProtoOpenApiGenerator().generate(wktRegistry()));

        assertThat(schemas.keySet()).noneMatch(key -> key.endsWith("Entry"));
        Map<String, Object> counts = propertyOf(schemas, "ai_pipestream_test_WktDoc", "counts");
        assertThat(counts).containsEntry("type", "object");
        @SuppressWarnings("unchecked")
        Map<String, Object> values = (Map<String, Object>) counts.get("additionalProperties");
        // int64 map values are printed as JSON strings by JsonFormat
        assertThat(values).containsEntry("type", "string").containsEntry("format", "int64");
    }

    @Test
    void wellKnownTypeFieldsDescribeJsonFormatEncoding() throws Exception {
        Map<String, Object> doc = new ProtoOpenApiGenerator().generate(wktRegistry());
        Map<String, Object> schemas = schemasOf(doc);

        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "created"))
                .containsEntry("type", "string").containsEntry("format", "date-time");
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "ttl"))
                .containsEntry("type", "string")
                .hasEntrySatisfying("pattern", p -> assertThat((String) p).endsWith("s$"));
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "payload"))
                .containsEntry("type", "object").doesNotContainKey("properties");
        // google.protobuf.Value is any JSON value: the empty schema
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "dynamic")).isEmpty();
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "items"))
                .containsEntry("type", "array");
        Map<String, Object> any = propertyOf(schemas, "ai_pipestream_test_WktDoc", "extra");
        assertThat(any).containsEntry("type", "object");
        @SuppressWarnings("unchecked")
        Map<String, Object> anyProps = (Map<String, Object>) any.get("properties");
        assertThat(anyProps).containsKey("@type");
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "mask"))
                .containsEntry("type", "string");
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "maybeName"))
                .containsEntry("type", "string").containsEntry("nullable", true);
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "maybeCount"))
                .containsEntry("type", "string")
                .containsEntry("format", "int64")
                .containsEntry("nullable", true);
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "maybeFlag"))
                .containsEntry("type", "boolean").containsEntry("nullable", true);
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "maybeRatio"))
                .containsEntry("type", "number")
                .containsEntry("format", "double")
                .containsEntry("nullable", true);
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "maybeSize"))
                .containsEntry("type", "integer")
                .containsEntry("minimum", 0L)
                .containsEntry("nullable", true);
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "maybeBlob"))
                .containsEntry("type", "string")
                .containsEntry("format", "byte")
                .containsEntry("nullable", true);
        // JsonFormat prints NullValue as JSON null
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "nothing"))
                .containsEntry("nullable", true);
        // scalar 64-bit integers stay strings
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "big"))
                .containsEntry("type", "string").containsEntry("format", "int64");
        assertThat(propertyOf(schemas, "ai_pipestream_test_WktDoc", "huge"))
                .containsEntry("type", "string").containsEntry("format", "uint64");
        // no internal-field object schemas leak into components for well-known types
        assertThat(schemas.keySet()).noneMatch(key -> key.startsWith("google_protobuf_"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void enumsAcceptNamesOrNumbers() throws Exception {
        Map<String, Object> schemas = schemasOf(new ProtoOpenApiGenerator().generate(wktRegistry()));

        Map<String, Object> color = propertyOf(schemas, "ai_pipestream_test_WktDoc", "color");
        List<Map<String, Object>> anyOf = (List<Map<String, Object>>) color.get("anyOf");
        assertThat(anyOf).hasSize(2);
        assertThat(anyOf.get(0)).containsEntry("type", "string");
        assertThat((List<Object>) anyOf.get(0).get("enum"))
                .containsExactly("COLOR_UNSPECIFIED", "RED", "BLUE");
        // JsonFormat prints unrecognized enum values as numbers and accepts numbers
        assertThat(anyOf.get(1)).containsEntry("type", "integer");
    }

    /** Every operationId in the document, in path/verb order. */
    @SuppressWarnings("unchecked")
    private static List<String> operationIdsOf(Map<String, Object> doc) {
        List<String> ids = new java.util.ArrayList<>();
        ((Map<String, Object>) doc.get("paths")).values().forEach(item ->
                ((Map<String, Object>) item).values().forEach(operation ->
                        ids.add((String) ((Map<String, Object>) operation).get("operationId"))));
        return ids;
    }

    @SuppressWarnings("unchecked")
    private static String securityRefOf(Map<String, Object> doc, String path) {
        Map<String, Object> paths = (Map<String, Object>) doc.get("paths");
        Map<String, Object> pathItem = (Map<String, Object>) paths.get(path);
        assertThat(pathItem).as(path).isNotNull();
        Map<String, Object> operation = (Map<String, Object>) pathItem.get("post");
        java.util.List<Map<String, Object>> security =
                (java.util.List<Map<String, Object>>) operation.get("security");
        assertThat(security).as(path + " security").hasSize(1);
        return security.getFirst().keySet().iterator().next();
    }

    /** Registry with one method whose request/response is a message using every well-known type. */
    private static ProtoRestMethodRegistry wktRegistry() throws Exception {
        MethodDescriptor method = wktMethodDescriptor();
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("WktService", "Save", req -> req)
                .methodDescriptor(method)
                .build());
        return registry;
    }

    private static MethodDescriptor wktMethodDescriptor() throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder()
                .setName("WktDoc")
                .addNestedType(DescriptorProto.newBuilder()
                        .setName("CountsEntry")
                        .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("key").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("value").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)));
        int number = 1;
        for (Map.Entry<String, String> field : Map.ofEntries(
                Map.entry("created", ".google.protobuf.Timestamp"),
                Map.entry("ttl", ".google.protobuf.Duration"),
                Map.entry("payload", ".google.protobuf.Struct"),
                Map.entry("dynamic", ".google.protobuf.Value"),
                Map.entry("items", ".google.protobuf.ListValue"),
                Map.entry("extra", ".google.protobuf.Any"),
                Map.entry("mask", ".google.protobuf.FieldMask"),
                Map.entry("maybe_name", ".google.protobuf.StringValue"),
                Map.entry("maybe_count", ".google.protobuf.Int64Value"),
                Map.entry("maybe_flag", ".google.protobuf.BoolValue"),
                Map.entry("maybe_ratio", ".google.protobuf.DoubleValue"),
                Map.entry("maybe_size", ".google.protobuf.UInt32Value"),
                Map.entry("maybe_blob", ".google.protobuf.BytesValue")).entrySet()) {
            message.addField(FieldDescriptorProto.newBuilder()
                    .setName(field.getKey()).setNumber(number++)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(field.getValue())
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        message.addField(FieldDescriptorProto.newBuilder()
                        .setName("nothing").setNumber(number++)
                        .setType(FieldDescriptorProto.Type.TYPE_ENUM)
                        .setTypeName(".google.protobuf.NullValue")
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("big").setNumber(number++)
                        .setType(FieldDescriptorProto.Type.TYPE_INT64)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("huge").setNumber(number++)
                        .setType(FieldDescriptorProto.Type.TYPE_UINT64)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("color").setNumber(number++)
                        .setType(FieldDescriptorProto.Type.TYPE_ENUM)
                        .setTypeName(".ai.pipestream.test.Color")
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("counts").setNumber(number)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".ai.pipestream.test.WktDoc.CountsEntry")
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED));

        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("wkt_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addDependency("google/protobuf/duration.proto")
                .addDependency("google/protobuf/struct.proto")
                .addDependency("google/protobuf/any.proto")
                .addDependency("google/protobuf/field_mask.proto")
                .addDependency("google/protobuf/wrappers.proto")
                .addEnumType(EnumDescriptorProto.newBuilder()
                        .setName("Color")
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("COLOR_UNSPECIFIED").setNumber(0))
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(1))
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(2)))
                .addMessageType(message)
                .addService(ServiceDescriptorProto.newBuilder()
                        .setName("WktService")
                        .addMethod(MethodDescriptorProto.newBuilder()
                                .setName("Save")
                                .setInputType(".ai.pipestream.test.WktDoc")
                                .setOutputType(".ai.pipestream.test.WktDoc")))
                .build();
        FileDescriptor descriptor = FileDescriptor.buildFrom(file, new FileDescriptor[]{
                TimestampProto.getDescriptor(),
                DurationProto.getDescriptor(),
                StructProto.getDescriptor(),
                AnyProto.getDescriptor(),
                FieldMaskProto.getDescriptor(),
                WrappersProto.getDescriptor()});
        return descriptor.findServiceByName("WktService").getMethods().getFirst();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> schemaOf(Map<String, Object> schemas, String schemaKey) {
        Map<String, Object> schema = (Map<String, Object>) schemas.get(schemaKey);
        assertThat(schema).as(schemaKey).isNotNull();
        return schema;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> schemasOf(Map<String, Object> doc) {
        Map<String, Object> components = (Map<String, Object>) doc.get("components");
        return (Map<String, Object>) components.get("schemas");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> propertyOf(
            Map<String, Object> schemas, String schemaKey, String property) {
        Map<String, Object> schema = (Map<String, Object>) schemas.get(schemaKey);
        assertThat(schema).as(schemaKey).isNotNull();
        Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
        return (Map<String, Object>) properties.get(property);
    }
}
