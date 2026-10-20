package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoApiTokenValidator;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * The JSON/REST surface of the serve wiring: every RPC registered as
 * {@code POST /grpc-json/ProtoMoltService/{Method}} behind the gateway. The families that
 * contribute verbs declare them on services of their own, so those are mounted alongside
 * ProtoMolt's; without them the same verb is on gRPC and MCP but missing from REST and from
 * the OpenAPI document. With caller stores mounted, a presented token resolves to a caller
 * (operator token first, then the policy chain); without them the shared secret alone gates.
 */
final class RestGatewayMount {

    private RestGatewayMount() {
    }

    /**
     * Registers the methods and builds the gateway that validates their tokens. Without an API
     * token the gateway is open; with one but no caller stores the shared secret gates; with
     * stores, a missing credential names the expected token and an unrecognized one is refused
     * as invalid.
     */
    static ProtoRestGateway mount(ActionContext context, ActionCatalog catalog, String apiToken,
                                  CallerResolver callers, List<ServiceDescriptor> contributed,
                                  Map<String, Map<String, String>> authorTaskBindings) {
        ProtoRestMethodRegistry methods = new ProtoRestMethodRegistry();
        Function<Map<String, String>, Caller> restCallers = callers == null
                ? null : restCallers(apiToken, callers);
        ProtoMoltRestMount.register(methods, catalog,
                apiToken == null ? null : ApiTokenRequirement.apiKeyHeader("api_token"),
                restCallers, contributed, authorTaskBindings);
        if (apiToken == null) {
            return new ProtoRestGateway(methods, context.transcoder());
        }
        if (restCallers == null) {
            return new ProtoRestGateway(methods, context.transcoder(),
                    ProtoApiTokenValidator.sharedSecret(apiToken));
        }
        Function<Map<String, String>, Caller> resolved = restCallers;
        return new ProtoRestGateway(methods, context.transcoder(),
                (tokenConfig, headers, query) -> {
                    String presented = presentedCredential(headers);
                    if (presented == null || presented.isBlank()) {
                        return Optional.of(
                                "Missing API token '" + tokenConfig.name() + "'");
                    }
                    return resolved.apply(headers) != null
                            ? Optional.empty()
                            : Optional.of("Invalid API token");
                });
    }

    /** The credential a REST request presents: {@code api_token} or a bearer authorization. */
    private static String presentedCredential(Map<String, String> headers) {
        String presented = headers.get("api_token");
        if (presented == null) {
            String authorization = headers.get("authorization");
            if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        return presented;
    }

    /** Header-to-caller resolution for the REST mount: operator token, else the policy. */
    private static Function<Map<String, String>, Caller> restCallers(
            String apiToken, CallerResolver resolver) {
        byte[] operator = apiToken.getBytes(StandardCharsets.UTF_8);
        return headers -> {
            String presented = presentedCredential(headers);
            if (presented == null || presented.isBlank()) {
                return null;
            }
            if (MessageDigest.isEqual(operator,
                    presented.getBytes(StandardCharsets.UTF_8))) {
                return Caller.operator();
            }
            return resolver.resolve(presented).orElse(null);
        };
    }
}
