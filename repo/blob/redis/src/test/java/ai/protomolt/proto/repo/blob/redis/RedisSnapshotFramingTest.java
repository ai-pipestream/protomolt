package ai.protomolt.proto.repo.blob.redis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Malformed wire replies must be refused before allocating the advertised metadata. */
class RedisSnapshotFramingTest {
    @Test void rejectsOversizedBulkBeforeReadingItsBody() throws Exception {
        refused("*4\r\n:1\r\n$2147483647\r\n", "Redis metadata exceeds its bound");
    }

    @Test void rejectsInvalidAttributePairCount() throws Exception {
        refused("*4\r\n:1\r\n$0\r\n\r\n$0\r\n\r\n*513\r\n", "Invalid Redis snapshot attributes");
    }

    @Test void rejectsWrongTupleShape() throws Exception {
        refused("*5\r\n", "Invalid Redis snapshot metadata");
    }

    private static void refused(String reply, String message) throws Exception {
        try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                var transfer = new RedisTransfer(new URI("redis://localhost:" + server.getLocalPort()), null, ignored -> {});
                var peer = server.accept()) {
            peer.getOutputStream().write(reply.getBytes(StandardCharsets.US_ASCII));
            peer.getOutputStream().flush();
            assertThatThrownBy(transfer::metadataReply).isInstanceOf(IOException.class).hasMessage(message);
        }
    }
}
