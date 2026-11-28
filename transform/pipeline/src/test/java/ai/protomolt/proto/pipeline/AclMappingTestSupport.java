package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.DocumentSecurityTerms;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.IdentityLinks;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Shared descriptor, fixture, caller, and OpenSearch replay helpers for ACL mapping tests. */
final class AclMappingTestSupport {

    static final String ACCOUNT = "ACC";

    private AclMappingTestSupport() {
    }

    static DescriptorRegistry loadRegistry(String... descriptorResources) throws Exception {
        DescriptorRegistry registry = DescriptorRegistry.create(false);
        for (String resource : descriptorResources) {
            new GoogleDescriptorLoader(resource).loadDescriptors().forEach(registry::registerFile);
        }
        registry.registerFile(DocumentSecurity.getDescriptor().getFile());
        registry.registerFile(IdentityLink.getDescriptor().getFile());
        return registry;
    }

    static TypedEdge loadEdge(String resource) throws IOException {
        TypedEdge.Builder builder = TypedEdge.newBuilder();
        try (InputStream in = AclMappingTestSupport.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing resource " + resource);
            }
            TextFormat.merge(new String(in.readAllBytes(), StandardCharsets.UTF_8), builder);
        }
        return builder.build();
    }

    static List<DynamicMessage> readDelimited(Descriptor descriptor, String resource) throws IOException {
        List<DynamicMessage> messages = new ArrayList<>();
        try (InputStream in = AclMappingTestSupport.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing resource " + resource);
            }
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            while (builder.mergeDelimitedFrom(in)) {
                messages.add(builder.build());
                builder = DynamicMessage.newBuilder(descriptor);
            }
        }
        return messages;
    }

    static DynamicMessage parseText(Descriptor descriptor, String text) throws TextFormat.ParseException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        TextFormat.merge(text, builder);
        return builder.build();
    }

    static DocumentSecurity mapSecurity(
            DescriptorRegistry registry, TypedEdge edge, Map<String, DynamicMessage> values, String label)
            throws Exception {
        DynamicMessage mapped = EdgeMapping.map(
                registry.findDescriptorByFullName(edge.getProduceType()),
                values,
                edge.getRulesList(),
                edge.getCelRulesList(),
                registry,
                label);
        return DocumentSecurity.parseFrom(mapped.toByteString());
    }

    static List<IdentityLink> mapIdentityLinks(
            DescriptorRegistry registry, TypedEdge edge, Map<String, DynamicMessage> values, String label)
            throws Exception {
        return IdentityLinks.parseFrom(EdgeMapping.map(
                registry.findDescriptorByFullName(edge.getProduceType()),
                values,
                edge.getRulesList(),
                edge.getCelRulesList(),
                registry,
                label).toByteString()).getLinksList();
    }

    static IdentityLink mapSingleIdentityLink(
            DescriptorRegistry registry, TypedEdge edge, Map<String, DynamicMessage> values, String label)
            throws Exception {
        return IdentityLink.parseFrom(EdgeMapping.map(
                registry.findDescriptorByFullName(edge.getProduceType()),
                values,
                edge.getRulesList(),
                edge.getCelRulesList(),
                registry,
                label).toByteString());
    }

    static Principal principal(String spec) {
        int colon = spec.indexOf(':');
        return Principal.newBuilder()
                .setIdentityType(spec.substring(0, colon))
                .setIdentity(spec.substring(colon + 1))
                .build();
    }

    static RepositoryCaller caller(String... identities) {
        Set<Principal> principals = Arrays.stream(identities)
                .map(AclMappingTestSupport::principal)
                .collect(Collectors.toSet());
        return new RepositoryCaller("reader", false, Set.of(ACCOUNT), principals);
    }

    static boolean allows(RepositoryCaller caller, DocumentSecurity security) {
        return DocumentAccessPolicy.allows(caller, ACCOUNT, security, List.of(), Access.ACCESS_READ);
    }

    static boolean reads(DocumentSecurity security, String... identities) {
        return allows(caller(identities), security);
    }

    static Object field(Message message, String name) {
        return message.getField(message.getDescriptorForType().findFieldByName(name));
    }

    static Map<String, Map<String, Boolean>> writeAndVerifySearchFixture(
            Map<String, DocumentSecurity> documents,
            Map<String, Boolean> complete,
            Map<String, RepositoryCaller> callers,
            Path itemsFile,
            Path callersFile) throws IOException {
        StringBuilder items = new StringBuilder();
        Map<String, Map<String, Boolean>> decisions = new LinkedHashMap<>();
        callers.keySet().forEach(name -> decisions.put(name, new LinkedHashMap<>()));
        for (var doc : documents.entrySet()) {
            DocumentSecurityTerms terms = DocumentSecurityTerms.of(doc.getValue(), List.of());
            items.append("{\"_id\":").append(json(doc.getKey()))
                    .append(",\"acl_allow\":").append(json(terms.allow()))
                    .append(",\"acl_deny\":").append(json(terms.deny()))
                    .append(",\"acl_gates\":[");
            for (int i = 0; i < terms.gates().size(); i++) {
                DocumentSecurityTerms.Gate gate = terms.gates().get(i);
                items.append(i == 0 ? "" : ",")
                        .append("{\"source\":").append(json(gate.source()))
                        .append(",\"allow\":").append(json(gate.allow())).append('}');
            }
            Boolean itemComplete = complete.get(doc.getKey());
            assertThat(itemComplete).as("completeness of %s", doc.getKey()).isNotNull();
            items.append("],\"acl_complete\":").append(itemComplete).append("}\n");
            for (var callerEntry : callers.entrySet()) {
                boolean allowed = allows(callerEntry.getValue(), doc.getValue());
                assertThat(terms.reads(DocumentSecurityTerms.callerTerms(callerEntry.getValue().identities())))
                        .as("%s on %s", callerEntry.getKey(), doc.getKey())
                        .isEqualTo(allowed);
                decisions.get(callerEntry.getKey()).put(doc.getKey(), allowed);
            }
        }
        StringBuilder callerJson = new StringBuilder("[");
        int n = 0;
        for (var callerEntry : callers.entrySet()) {
            callerJson.append(n++ == 0 ? "\n" : ",\n")
                    .append("{\"caller\":").append(json(callerEntry.getKey()))
                    .append(",\"terms\":").append(json(List.copyOf(
                            DocumentSecurityTerms.callerTerms(callerEntry.getValue().identities()))))
                    .append(",\"reads\":{");
            int m = 0;
            for (var decision : decisions.get(callerEntry.getKey()).entrySet()) {
                callerJson.append(m++ == 0 ? "" : ",")
                        .append(json(decision.getKey())).append(':').append(decision.getValue());
            }
            callerJson.append("}}");
        }
        callerJson.append("\n]\n");
        Files.writeString(itemsFile, items);
        Files.writeString(callersFile, callerJson);
        assertThat(Files.readAllLines(itemsFile)).hasSize(documents.size());
        return decisions;
    }

    static boolean isComplete(Message message, String field) {
        return field(message, field).toString().equals("ACL_COMPLETENESS_COMPLETE");
    }

    static String json(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }

    static String json(List<String> values) {
        return values.stream().map(AclMappingTestSupport::json).collect(Collectors.joining(",", "[", "]"));
    }
}
