package ai.protomolt.proto.http.connect.conformance;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.http.connect.ConnectLimits;
import com.sun.net.httpserver.HttpServer;
import connectrpc.conformance.v1.ConformanceServiceGrpc;
import connectrpc.conformance.v1.ServerCompat.ServerCompatRequest;
import connectrpc.conformance.v1.ServerCompat.ServerCompatResponse;
import io.grpc.ServerInterceptors;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * The server under test for the Connect conformance runner ({@code connectconformance --mode
 * server}): reads one size-prefixed {@link ServerCompatRequest} from standard input, starts
 * {@link ConnectGateway} over {@link ConformanceService} on a loopback port, answers a
 * size-prefixed {@link ServerCompatResponse} on standard output, and serves until the runner
 * ends the process.
 */
public final class ConformanceServer {

    /** The bound the gateway applies to request and response messages. */
    static final int MESSAGE_BYTES = 16 * 1024 * 1024;

    private ConformanceServer() {
    }

    /**
     * Runs the server.
     *
     * @param args unused
     * @throws Exception when the request cannot be read or the server cannot start
     */
    public static void main(String[] args) throws Exception {
        ServerCompatRequest request = readRequest();
        if (request.getUseTls()) {
            throw new IllegalArgumentException("the conformance configuration declares no TLS support");
        }
        int limit = request.getMessageReceiveLimit() == 0 ? MESSAGE_BYTES : request.getMessageReceiveLimit();
        String service = ConformanceServiceGrpc.SERVICE_NAME;
        ConnectGateway gateway = new ConnectGateway(
                List.of(ServerInterceptors.intercept(new ConformanceService().bindService(),
                        new ConformanceService.CallMetadata())),
                Set.of(service + "/Unary", service + "/ServerStream", service + "/IdempotentUnary",
                        service + "/Unimplemented", service + "/ClientStream", service + "/BidiStream"),
                // The suite sends no credentials; every call runs with process authority.
                headers -> Optional.of(AuthenticatedCaller.unbound(Caller.operator())),
                new ConnectLimits(limit, MESSAGE_BYTES),
                // The runner calls procedures at the server root.
                "");
        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/", gateway);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.start();
        writeResponse(ServerCompatResponse.newBuilder()
                .setHost("127.0.0.1")
                .setPort(http.getAddress().getPort())
                .build());
        new CountDownLatch(1).await();
    }

    private static ServerCompatRequest readRequest() throws IOException {
        DataInputStream in = new DataInputStream(System.in);
        int length = in.readInt();
        return ServerCompatRequest.parseFrom(in.readNBytes(length));
    }

    private static void writeResponse(ServerCompatResponse response) throws IOException {
        byte[] bytes = response.toByteArray();
        DataOutputStream out = new DataOutputStream(System.out);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }
}
