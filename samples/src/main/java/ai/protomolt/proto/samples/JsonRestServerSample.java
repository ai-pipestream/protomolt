package ai.protomolt.proto.samples;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ProtoApiToken;
import ai.protomolt.proto.http.rest.ProtoApiTokenValidator;
import ai.protomolt.proto.http.rest.ProtoRestAnnotationRegistrar;
import ai.protomolt.proto.http.rest.ProtoRestExposed;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.server.ProtoToolsServerConfig;
import ai.protomolt.proto.server.jdk.JdkProtoRestServer;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

/**
 * Boots the JDK HttpServer JSON/REST gateway with an annotated echo service.
 *
 * <pre>
 * ./gradlew :samples:runJsonRestServer
 * curl -H 'api_token: secret' -H 'content-type: application/json' \\
 *   -d '{"name":"Ada"}' http://127.0.0.1:8080/grpc-json/Echo/echo
 * </pre>
 */
public final class JsonRestServerSample {

    @ProtoApiToken(name = "api_token")
    static final class EchoService {
        @ProtoRestExposed(summary = "Echo a greeting")
        public Struct echo(Struct request) {
            String name = request.getFieldsOrDefault("name", Value.getDefaultInstance()).getStringValue();
            if (name.isBlank()) {
                name = "world";
            }
            return Struct.newBuilder()
                    .putFields("message", Value.newBuilder().setStringValue("hello " + name).build())
                    .build();
        }
    }

    private JsonRestServerSample() {
    }

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        String token = args.length > 1 ? args[1] : "secret";

        DescriptorRegistry descriptors = DescriptorRegistry.create();
        ProtoRestMethodRegistry methods = new ProtoRestMethodRegistry();
        new ProtoRestAnnotationRegistrar(methods).register(new EchoService());

        ProtoRestGateway gateway = new ProtoRestGateway(
                methods,
                new ProtobufJsonTranscoder(descriptors),
                ProtoApiTokenValidator.sharedSecret(token));

        ProtoToolsServerConfig config = ProtoToolsServerConfig.defaults()
                .withHost("127.0.0.1")
                .withPort(port);

        JdkProtoRestServer server = new JdkProtoRestServer(
                config,
                gateway,
                new ProtoOpenApiGenerator("Echo sample", "0.1.0", "http://127.0.0.1:" + port, config.restPathPrefix()));

        int bound = server.start();
        System.out.printf("""
                Proto JSON/REST sample (%s) listening on http://127.0.0.1:%d
                  POST %s/Echo/echo   (header api_token: %s)
                  GET  %s
                  GET  %s
                Ctrl+C to stop.
                """,
                server.engineId(),
                bound,
                config.restPathPrefix(),
                token,
                config.openApiPath(),
                config.healthPath());

        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.close();
        }
    }
}
