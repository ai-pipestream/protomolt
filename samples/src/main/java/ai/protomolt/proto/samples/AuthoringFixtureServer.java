package ai.protomolt.proto.samples;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.nio.file.Path;

/** Runnable TCP fixture with reflection for the external workflow authoring demo. */
public final class AuthoringFixtureServer {
    private AuthoringFixtureServer() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: AuthoringFixtureServer <port> <records-directory>");
        }
        int port = Integer.parseInt(args[0]);
        if (port < 0 || port > 65535) throw new IllegalArgumentException("invalid port");
        FileSystemFixtureRecordRepository records =
                new FileSystemFixtureRecordRepository(Path.of(args[1]));
        Server server = ServerBuilder.forPort(port)
                .addService(new AuthoringFixtureService(records))
                .addService(ProtoReflectionServiceV1.newInstance())
                .build().start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        System.out.println("AuthoringFixtureService listening on port " + server.getPort());
        server.awaitTermination();
    }
}
