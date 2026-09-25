package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workspace.EnvProfileCredentialResolver;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/** Host-owned connection to the fixed correction starter; persisted profiles contain no secrets. */
record CorrectionStarter(ServiceProfile profile, ProfileCredentialResolver credentials) {
    static final String SERVICE = "ai.protomolt.proto.correction.v1.CorrectionService/";
    private static final String REFERENCE = "env:PROTOMOLT_CORRECTION_API_TOKEN";
    private static final ObjectMapper JSON = new ObjectMapper();

    static CorrectionStarter fromEnvironment(Map<String, String> environment) {
        String target = environment.get("PROTOMOLT_CORRECTION_TARGET");
        if (target == null || target.isBlank()) return null;
        URI address;
        try { address = URI.create("grpc://" + target); }
        catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("PROTOMOLT_CORRECTION_TARGET must be host:port");
        }
        if (address.getHost() == null || address.getPort() < 1 || address.getPort() > 65535
                || address.getUserInfo() != null || !address.getPath().isEmpty()
                || address.getQuery() != null || address.getFragment() != null) {
            throw new IllegalArgumentException("PROTOMOLT_CORRECTION_TARGET must be host:port");
        }
        if (environment.getOrDefault("PROTOMOLT_CORRECTION_API_TOKEN", "").isBlank()) {
            throw new IllegalArgumentException("the correction starter requires its service token");
        }
        ServiceEndpoint endpoint = ServiceEndpoint.newBuilder().setName("default")
                .setHost(address.getHost()).setPort(address.getPort())
                .setTransport(Transport.TRANSPORT_PLAINTEXT).setCredentialRef(REFERENCE).build();
        ServiceProfile profile = ServiceProfile.newBuilder().setName("correction")
                .setDescription("Recorded correct-contact workflow")
                .addEndpoints(endpoint)
                .addMethodPolicies(MethodPolicy.newBuilder().setMethod(SERVICE + "RunCorrection")
                        .addOperation(Operation.OPERATION_MUTATING)
                        .setDeadline(Duration.newBuilder().setSeconds(330)))
                .addMethodPolicies(MethodPolicy.newBuilder().setMethod(SERVICE + "GetCorrection")
                        .addOperation(Operation.OPERATION_READ_ONLY)
                        .setDeadline(Duration.newBuilder().setSeconds(30)))
                .build();
        var binding = new EnvProfileCredentialResolver.Binding("correction", "default",
                endpoint.getHost(), endpoint.getPort(), endpoint.getTransport(), REFERENCE);
        return new CorrectionStarter(profile, new EnvProfileCredentialResolver(Set.of(binding),
                reference -> environment.get(reference.substring("env:".length()))));
    }

    void install(ActionCatalog catalog, ServiceProfileRepository repository) {
        if (repository == null) throw new IllegalStateException("the correction starter requires a service workspace");
        try {
            var existing = repository.find(profile.getName());
            if (existing.isPresent()) {
                if (!existing.get().getEndpointsList().equals(profile.getEndpointsList())
                        || !existing.get().getMethodPoliciesList().equals(profile.getMethodPoliciesList())) {
                    throw new IllegalStateException("stored correction profile differs from the configured starter");
                }
                catalog.execute("service-refresh", JSON.createObjectNode()
                        .put("name", "correction").put("endpoint", "default"));
                return;
            }
            ObjectNode request = JSON.createObjectNode();
            request.set("profile", JSON.readTree(JsonFormat.printer().print(profile)));
            request.put("endpoint", "default");
            catalog.execute("service-register", request);
        } catch (IOException | ActionException failed) {
            // Upstream transport descriptions may include private connection details.
            throw new IllegalStateException("could not register the correction starter; check service health and credentials");
        }
    }
}
