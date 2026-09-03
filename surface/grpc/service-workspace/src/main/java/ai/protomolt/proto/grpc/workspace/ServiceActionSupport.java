package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.ReflectionClient;
import ai.protomolt.proto.grpc.invoke.ReflectionException;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;

import java.io.IOException;
import java.util.Optional;

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
                                          ServiceProfileRepository repository,
                                          SchemaRegistryStore registry,
                                          ChannelFactory channels)
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
            if (supportsDescriptorSets(registry)) {
                registry.putDescriptorSet(fingerprint, artifact.getDescriptorSet());
            } else {
                repository.saveDescriptorArtifact(artifact);
            }
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

    /** One stored profile as the contract's summary of it. */
    static void writeSummary(Reply summary, ServiceProfile profile) {
        summary.set("name", profile.getName())
                .set("description", profile.getDescription())
                .set("descriptorFingerprint",
                        profile.getSchemaSource().getDescriptorFingerprint());
        profile.getEndpointsList().forEach(
                endpoint -> summary.add("endpoints", endpoint.getName()));
        summary.build();
    }

    static DescriptorArtifact descriptorArtifact(ServiceProfile profile,
                                                 ServiceProfileRepository repository,
                                                 SchemaRegistryStore registry)
            throws IOException {
        String fingerprint = profile.getSchemaSource().getDescriptorFingerprint();
        if (supportsDescriptorSets(registry)) {
            Optional<com.google.protobuf.ByteString> registered = registry.descriptorSet(fingerprint);
            if (registered.isPresent()) {
                DescriptorArtifact artifact = DescriptorArtifact.newBuilder()
                        .setFingerprint(fingerprint)
                        .setDescriptorSet(registered.get())
                        .build();
                ServiceProfileValidation.validate(artifact);
                return artifact;
            }
        }
        DescriptorArtifact legacy = repository.findDescriptorArtifact(fingerprint)
                .orElseThrow(() -> new IOException("descriptor artifact '" + fingerprint
                        + "' for service '" + profile.getName() + "' was not found"));
        ServiceProfileValidation.validate(legacy);
        if (supportsDescriptorSets(registry)) {
            registry.putDescriptorSet(fingerprint, legacy.getDescriptorSet());
        }
        return legacy;
    }

    private static boolean supportsDescriptorSets(SchemaRegistryStore registry) {
        return registry != null && registry.supportsDescriptorSets();
    }

    static void writeServices(Reply reply, String field, ServiceProfile profile,
                              ServiceProfileRepository repository,
                              SchemaRegistryStore registry) throws ActionException {
        try {
            ServiceDescriptorInspection.writeServices(reply, field, profile, repository, registry);
        } catch (IOException e) {
            throw new ActionException("invalid-descriptor", e.getMessage());
        } catch (IllegalArgumentException e) {
            throw new ActionException("invalid-descriptor", e.getMessage());
        }
    }

    static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
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
