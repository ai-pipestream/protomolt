package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectionClient;
import ai.pipestream.proto.grpc.invoke.ReflectionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.ServiceProfileValidation;
import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.SchemaSource;
import ai.pipestream.proto.grpc.profile.v1.ServiceEndpoint;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.grpc.profile.v1.SourceKind;
import ai.pipestream.proto.grpc.profile.v1.Transport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;

import java.io.IOException;

/** Shared validation, persistence, reflection, and descriptor rendering for workspace actions. */
final class ServiceActionSupport {

    static final int DEFAULT_DEADLINE_MS = 15_000;
    static final int MAX_DEADLINE_MS = 60_000;
    static final String UNAVAILABLE_MESSAGE = "service workspace is not configured on this server "
            + "(start with --service-workspace <directory>)";

    private ServiceActionSupport() {
    }

    static ServiceProfileRepository requireRepository(ServiceProfileRepository repository)
            throws ActionException {
        if (repository == null) {
            throw new ActionException("unavailable", UNAVAILABLE_MESSAGE);
        }
        return repository;
    }

    static String requireString(ObjectNode input, String field) throws ActionException {
        JsonNode value = input.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw invalid("'" + field + "' must be a non-empty string", "/" + field);
        }
        return value.asText();
    }

    static int deadline(ObjectNode input) throws ActionException {
        JsonNode value = input.get("deadlineMs");
        if (value == null || value.isNull()) {
            return DEFAULT_DEADLINE_MS;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.asInt() <= 0
                || value.asInt() > MAX_DEADLINE_MS) {
            throw invalid("'deadlineMs' must be an integer from 1 to " + MAX_DEADLINE_MS,
                    "/deadlineMs");
        }
        return value.asInt();
    }

    static ServiceProfile parseProfile(ObjectNode input) throws ActionException {
        JsonNode node = input.get("profile");
        if (!(node instanceof ObjectNode)) {
            throw invalid("'profile' must be an object", "/profile");
        }
        ServiceProfile.Builder profile = ServiceProfile.newBuilder();
        try {
            JsonFormat.parser().merge(node.toString(), profile);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw invalid("'profile' is not valid service-profile JSON: " + e.getMessage(),
                    "/profile");
        }
        return profile.build();
    }

    static ServiceEndpoint endpoint(ServiceProfile profile, String requested) throws ActionException {
        if (profile.getEndpointsCount() == 0) {
            throw invalid("profile.endpoints must contain at least one endpoint", "/profile/endpoints");
        }
        if (requested == null || requested.isBlank()) {
            return profile.getEndpoints(0);
        }
        return profile.getEndpointsList().stream()
                .filter(endpoint -> endpoint.getName().equals(requested))
                .findFirst()
                .orElseThrow(() -> invalid("endpoint '" + requested + "' is not in profile '"
                        + profile.getName() + "'", "/endpoint"));
    }

    static ServiceProfile reflectAndStore(ServiceProfile profile, String endpointName, int deadlineMs,
                                          ServiceProfileRepository repository, ChannelFactory channels)
            throws ActionException, ReflectionException {
        try {
            ServiceProfileValidation.validateConnectionProfile(profile);
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/profile");
        }
        ServiceEndpoint endpoint = endpoint(profile, endpointName);
        if (!endpoint.getCredentialRef().isBlank() || !endpoint.getTrustRef().isBlank()
                || !endpoint.getClientCertificateRef().isBlank()) {
            throw new ActionException("unsupported-transport",
                    "reflection with credential, custom-trust, or client-certificate references "
                            + "requires a configured credential resolver");
        }
        String target = target(endpoint);
        boolean tls = endpoint.getTransport() == Transport.TRANSPORT_TLS;
        try {
            channels.validateTarget(target, tls);
            channels.validateDeadline(deadlineMs);
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), e.getMessage().contains("deadline")
                    ? "/deadlineMs" : "/profile/endpoints");
        }
        ManagedChannel channel;
        try {
            channel = channels.open(target, tls);
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/profile/endpoints");
        }
        ReflectionClient.Result reflected;
        try {
            reflected = channels.policy() == null
                    ? ReflectionClient.discover(channel, deadlineMs)
                    : ReflectionClient.discover(channel, deadlineMs, channels.policy());
        } finally {
            channel.shutdownNow();
        }

        byte[] descriptorBytes = reflected.descriptorSet().toByteArray();
        try {
            GoogleDescriptorLoader.fromDescriptorSet(reflected.descriptorSet());
        } catch (Exception e) {
            throw new ActionException("invalid-descriptor",
                    "reflected descriptor set could not be linked");
        }
        String fingerprint = ServiceProfileValidation.sha256(descriptorBytes);
        DescriptorArtifact artifact = DescriptorArtifact.newBuilder()
                .setFingerprint(fingerprint)
                .setDescriptorSet(com.google.protobuf.ByteString.copyFrom(descriptorBytes))
                .build();
        ServiceProfile updated = profile.toBuilder()
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_REFLECTION)
                        .setSourceRef("grpc-reflection:" + endpoint.getName())
                        .setDescriptorFingerprint(fingerprint)
                        .setDescriptorArtifactRef("sha256:" + fingerprint))
                .build();
        try {
            ServiceProfileValidation.validate(updated);
            repository.saveDescriptorArtifact(artifact);
            repository.save(updated);
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/profile");
        } catch (IOException e) {
            throw storage("save service profile '" + profile.getName() + "'", e);
        }
        return updated;
    }

    static ObjectNode profileJson(ServiceProfile profile, ObjectMapper mapper) {
        try {
            return (ObjectNode) mapper.readTree(JsonFormat.printer().print(profile));
        } catch (IOException e) {
            throw new IllegalStateException("failed to render service profile", e);
        }
    }

    static ObjectNode summary(ServiceProfile profile, ObjectMapper mapper) {
        ObjectNode result = mapper.createObjectNode();
        result.put("name", profile.getName());
        if (!profile.getDescription().isBlank()) {
            result.put("description", profile.getDescription());
        }
        ArrayNode endpoints = result.putArray("endpoints");
        profile.getEndpointsList().forEach(endpoint -> endpoints.add(endpoint.getName()));
        result.put("descriptorFingerprint",
                profile.getSchemaSource().getDescriptorFingerprint());
        return result;
    }

    static ArrayNode services(ServiceProfile profile, ServiceProfileRepository repository,
                              ObjectMapper mapper) throws ActionException {
        try {
            return ServiceDescriptorInspection.services(profile, repository, mapper);
        } catch (IOException e) {
            throw new ActionException("invalid-descriptor", e.getMessage());
        }
    }

    static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }

    static ObjectNode nameSchema() {
        ObjectNode schema = baseSchema();
        schema.putObject("properties").putObject("name")
                .put("type", "string")
                .put("pattern", "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");
        schema.putArray("required").add("name");
        schema.put("additionalProperties", false);
        return schema;
    }

    static ActionException invalid(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }

    static ActionException notFound(String name) {
        return new ActionException("not-found", "service profile '" + name + "' was not found");
    }

    static ActionException storage(String operation, IOException error) {
        return new ActionException("storage-error", "Failed to " + operation);
    }

    static String target(ServiceEndpoint endpoint) {
        String host = endpoint.getHost();
        if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
            host = "[" + host + "]";
        }
        return host + ":" + endpoint.getPort();
    }
}
