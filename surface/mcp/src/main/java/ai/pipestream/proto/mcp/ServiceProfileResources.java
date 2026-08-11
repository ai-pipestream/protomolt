package ai.pipestream.proto.mcp;

import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.grpc.workspace.ServiceDescriptorInspection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** Exposes registered service profiles and their method contracts as bounded MCP resources. */
public final class ServiceProfileResources implements McpResources {

    private static final String ROOT = "protomolt://services";

    private final ServiceProfileRepository repository;

    public ServiceProfileResources(ServiceProfileRepository repository) {
        this.repository = java.util.Objects.requireNonNull(repository, "repository");
    }

    @Override
    public ArrayNode list(ObjectMapper mapper) {
        ArrayNode resources = mapper.createArrayNode();
        resource(resources, ROOT, "services", "Registered gRPC service profiles");
        try {
            for (ServiceProfile profile : repository.list()) {
                resource(resources, serviceUri(profile.getName()), profile.getName(),
                        "Connection profile and reflected method contracts for " + profile.getName());
            }
        } catch (IOException ignored) {
            // Reading ROOT reports the repository problem without breaking resources/list.
        }
        return resources;
    }

    @Override
    public ArrayNode templates(ObjectMapper mapper) {
        ArrayNode templates = mapper.createArrayNode();
        template(templates, ROOT + "/{profile}", "service-profile",
                "One URL-encoded service profile and its reflected method contracts");
        template(templates, ROOT + "/{profile}/methods/{fullMethod}", "service-method",
                "One reflected gRPC method contract from a URL-encoded service profile");
        return templates;
    }

    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
        if (ROOT.equals(uri)) {
            ObjectNode document = mapper.createObjectNode();
            ArrayNode profiles = document.putArray("services");
            try {
                repository.list().forEach(profile -> profiles.add(summary(profile, mapper)));
            } catch (IOException e) {
                document.put("error", "service workspace could not be read");
            }
            return Optional.of(contents(mapper, uri, document));
        }
        if (!uri.startsWith(ROOT + "/")) {
            return Optional.empty();
        }
        String rest = uri.substring(ROOT.length() + 1);
        int methodMarker = rest.indexOf("/methods/");
        String encodedName = methodMarker < 0 ? rest : rest.substring(0, methodMarker);
        String name = decode(encodedName);
        try {
            Optional<ServiceProfile> found = repository.find(name);
            if (found.isEmpty()) {
                return Optional.empty();
            }
            ServiceProfile profile = found.get();
            ArrayNode services = ServiceDescriptorInspection.services(profile, repository, mapper);
            if (methodMarker < 0) {
                ObjectNode document = mapper.createObjectNode();
                document.set("profile", profileJson(profile, mapper));
                document.set("services", services);
                return Optional.of(contents(mapper, uri, document));
            }
            String wanted = decode(rest.substring(methodMarker + "/methods/".length()));
            for (var service : services) {
                for (var method : service.path("methods")) {
                    if (method.path("fullName").asText().equals(wanted)) {
                        ObjectNode document = mapper.createObjectNode();
                        document.put("serviceProfile", name);
                        document.set("method", method);
                        return Optional.of(contents(mapper, uri, document));
                    }
                }
            }
            return Optional.empty();
        } catch (IOException e) {
            ObjectNode document = mapper.createObjectNode();
            document.put("error", "service workspace resource could not be read");
            return Optional.of(contents(mapper, uri, document));
        }
    }

    private static ObjectNode summary(ServiceProfile profile, ObjectMapper mapper) {
        ObjectNode node = mapper.createObjectNode();
        node.put("name", profile.getName());
        node.put("description", profile.getDescription());
        node.put("descriptorFingerprint", profile.getSchemaSource().getDescriptorFingerprint());
        ArrayNode endpoints = node.putArray("endpoints");
        profile.getEndpointsList().forEach(endpoint -> endpoints.add(endpoint.getName()));
        return node;
    }

    private static ObjectNode profileJson(ServiceProfile profile, ObjectMapper mapper)
            throws IOException {
        return (ObjectNode) mapper.readTree(JsonFormat.printer().print(profile));
    }

    private static void resource(ArrayNode resources, String uri, String name, String description) {
        ObjectNode node = resources.addObject();
        node.put("uri", uri);
        node.put("name", name);
        node.put("description", description);
        node.put("mimeType", "application/json");
    }

    private static void template(ArrayNode templates, String uriTemplate, String name,
                                 String description) {
        ObjectNode node = templates.addObject();
        node.put("uriTemplate", uriTemplate);
        node.put("name", name);
        node.put("description", description);
        node.put("mimeType", "application/json");
    }

    private static ObjectNode contents(ObjectMapper mapper, String uri, ObjectNode document) {
        ObjectNode contents = mapper.createObjectNode();
        contents.put("uri", uri);
        contents.put("mimeType", "application/json");
        contents.put("text", document.toString());
        return contents;
    }

    private static String serviceUri(String name) {
        return ROOT + "/" + encode(name);
    }

    private static String methodUri(String name, String method) {
        return serviceUri(name) + "/methods/" + encode(method);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
