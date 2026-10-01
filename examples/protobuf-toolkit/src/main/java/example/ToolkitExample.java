package example;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.cel.CelMappingRule;
import ai.protomolt.proto.cel.CelProtoMapper;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ProtoRestMethod;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.SourceResolver;
import ai.protomolt.proto.registry.CompatibilityWriteGate;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import example.contract.Contact;
import example.contract.SourceContact;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** A standalone consumer: no server, container, provider account, or generated service stub. */
public final class ToolkitExample {
    private ToolkitExample() { }

    public static void main(String[] args) throws Exception {
        mappingAndSelection();
        projectionAndValidation();
        registry();
        openApi();
    }

    private static void mappingAndSelection() throws Exception {
        var input = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("Ada").build())
                .putFields("enabled", Value.newBuilder().setBoolValue(true).build());
        var mapper = new ProtoFieldMapperImpl(DescriptorRegistry.create());
        mapper.mapInPlace(input, List.of("copiedName = name"));
        var cel = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(Struct.getDescriptor()).addVar("input").build());
        new CelProtoMapper(mapper, cel).map(input, List.of(
                new CelMappingRule("input.enabled", "input.name", "selectedName"),
                new CelMappingRule("!input.enabled", "input.name", "skippedName")));
        require(input.getFieldsOrThrow("copiedName").getStringValue().equals("Ada"), "mapping");
        require(input.getFieldsOrThrow("selectedName").getStringValue().equals("Ada"), "selection");
        require(!input.containsFields("skippedName"), "false filter must not write");
        System.out.println("mapping: copied Ada; selector: selected Ada; false filter: skipped");
    }

    private static void projectionAndValidation() throws Exception {
        var source = SourceContact.newBuilder().setFullName("Ada Lovelace")
                .setEmail("ada@example.org").build();
        var projection = MessageProjection.forTarget(Contact.getDescriptor(),
                SourceResolver.of(SourceContact.getDescriptor())).orElseThrow();
        var projected = projection.project(source);
        var validator = ProtoValidator.forMessageType(Contact.getDescriptor());
        validator.validate(projected).throwIfInvalid();
        var contact = Contact.parseFrom(projected.toByteString());
        require(contact.getName().equals("Ada Lovelace"), "projection renamed full_name");
        require(validator.validate(contact.toBuilder().setEmail("broken")
                        .setConfirmedEmail("broken").build()).violations().stream()
                        .anyMatch(v -> v.ruleId().equals("string.email")),
                "invalid email must fail");
        require(!validator.validate(contact.toBuilder().setName("A").build()).valid(),
                "short name must fail");
        var mismatch = validator.validate(contact.toBuilder()
                .setConfirmedEmail("other@example.org").build());
        require(mismatch.violations().stream()
                .anyMatch(v -> v.ruleId().equals("contact.confirmed_email")), "cross-field rule must fail");
        System.out.println("projection: Ada Lovelace; validation: accepted valid contact");
        System.out.println("validation: rejected invalid email, short name, and mismatched confirmation");
    }

    private static void registry() throws Exception {
        String schema = "syntax = \"proto3\"; package example; message Entry { string id = 1; }";
        try (var store = new InMemorySchemaRegistryStore(new CompatibilityWriteGate())) {
            var first = store.register("entry", schema, List.of());
            var repeated = store.register("entry", schema, List.of());
            require(first.globalId() == repeated.globalId() && repeated.version() == 1,
                    "identical registration must reuse identity");
            var second = store.register("entry", schema.replace("string id = 1;",
                    "string id = 1; string label = 2;"), List.of());
            require(second.version() == 2, "compatible addition must create version 2");
            boolean rejected = false;
            try {
                store.register("entry", schema.replace("string id", "int32 id"), List.of());
            } catch (ai.protomolt.proto.registry.IncompatibleRegistrationException expected) {
                rejected = true;
            }
            require(rejected, "wire-incompatible field change must fail");
            require(store.versions("entry").equals(List.of(1, 2)), "rejection must preserve versions");
            System.out.println("registry: reused version 1; added version 2; rejected incompatible change");
        }
    }

    @SuppressWarnings("unchecked")
    private static void openApi() throws Exception {
        var methods = new ProtoRestMethodRegistry();
        methods.register(ProtoRestMethod.builder("ContactService", "Check", request -> request)
                .methodDescriptor(Contact.getDescriptor().getFile()
                        .findServiceByName("ContactService").findMethodByName("Check"))
                .build());
        var generator = new ProtoOpenApiGenerator();
        var document = generator.generate(methods);
        var components = (Map<String, Object>) document.get("components");
        var schemas = (Map<String, Object>) components.get("schemas");
        var contact = (Map<String, Object>) schemas.get("example_toolkit_v1_Contact");
        var properties = (Map<String, Object>) contact.get("properties");
        var name = (Map<String, Object>) properties.get("name");
        require(((Number) name.get("minLength")).intValue() == 2, "OpenAPI minimum name length");
        var email = (Map<String, Object>) properties.get("email");
        require("email".equals(email.get("format")), "OpenAPI email format");
        var rules = (List<Map<String, Object>>) contact.get("x-protomolt-cel");
        require(rules.stream().anyMatch(rule -> "contact.confirmed_email".equals(rule.get("id"))),
                "OpenAPI must disclose the runtime CEL rule");
        var output = Path.of("build", "contact-openapi.json");
        Files.createDirectories(output.getParent());
        Files.writeString(output, generator.generateJson(methods));
        System.out.println("OpenAPI: wrote " + output + "; name minLength = 2");
        System.out.println("Runtime validation remains required for the cross-field CEL rule.");
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
