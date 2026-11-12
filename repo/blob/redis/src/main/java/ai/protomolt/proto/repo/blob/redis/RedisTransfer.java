package ai.protomolt.proto.repo.blob.redis;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.DefaultJedisSocketFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.SslOptions;
import redis.clients.jedis.util.JedisURIHelper;
import redis.clients.jedis.util.RedisInputStream;
import redis.clients.jedis.util.RedisOutputStream;

/**
 * One RESP2 transfer on a dedicated Jedis-configured socket. Jedis's ordinary command
 * API requires a byte array for every bulk argument and reply; this boundary streams
 * only the object bulk value. Control replies use bounded RESP2 parsing.
 * Closing an incomplete transfer abandons the socket, never drains or pools it.
 */
final class RedisTransfer implements AutoCloseable {
    private final Socket socket;
    private final java.util.function.Consumer<RedisTransfer> onClose;
    private final RedisInputStream input;
    private final RedisOutputStream output;

    RedisTransfer(URI uri, java.util.function.Consumer<RedisTransfer> onClose) throws IOException {
        this.onClose = onClose;
        var builder = DefaultJedisClientConfig.builder();
        if (JedisURIHelper.isRedisSSLScheme(uri)) builder.sslOptions(SslOptions.defaults());
        var config = builder.build();
        socket = new DefaultJedisSocketFactory(new HostAndPort(uri.getHost(),
                uri.getPort() < 0 ? Protocol.DEFAULT_PORT : uri.getPort()), config).createSocket();
        try {
            input = new RedisInputStream(socket.getInputStream(), 8192);
            output = new RedisOutputStream(socket.getOutputStream(), 8192);
            String password = JedisURIHelper.getPassword(uri);
            if (password != null) {
                String user = JedisURIHelper.getUser(uri);
                command(user == null ? List.of(bytes("AUTH"), bytes(password))
                        : List.of(bytes("AUTH"), bytes(user), bytes(password)));
                requireOk();
            }
            int database = JedisURIHelper.getDBIndex(uri);
            if (database != 0) {
                command(List.of(bytes("SELECT"), bytes(Integer.toString(database))));
                requireOk();
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { socket.close(); } catch (IOException close) { failure.addSuppressed(close); }
            throw failure;
        }
    }

    void command(List<byte[]> arguments) throws IOException {
        begin(arguments.size());
        for (byte[] argument : arguments) bulk(argument);
        output.flush();
    }

    void begin(int count) throws IOException {
        output.write((byte) '*');
        output.writeIntCrLf(count);
    }

    void bulk(byte[] value) throws IOException {
        beginBulk(value.length);
        output.write(value);
        endBulk();
    }

    void beginBulk(int length) throws IOException {
        output.write((byte) '$');
        output.writeIntCrLf(length);
    }

    void write(byte[] buffer, int length) throws IOException { output.write(buffer, 0, length); }
    void endBulk() throws IOException { output.writeCrLf(); }

    long integerReply() throws IOException {
        output.flush();
        return number(':');
    }

    /** Reads only the array header and status; the payload remains on the socket. */
    long readStatus() throws IOException { return readStatus(2); }

    long readStatus(int successFields) throws IOException {
        long count = number('*');
        if (count != 1 && count != successFields) throw new IOException("Invalid Redis stream response shape");
        long status = number(':');
        if ((status == 1) != (count == successFields)) throw new IOException("Invalid Redis stream response status");
        return status;
    }

    /** Lua bounds this metadata tuple before returning it; the following body is not decoded here. */
    List<?> metadataReply() throws IOException {
        Object reply = Protocol.read(input);
        if (!(reply instanceof List<?> fields) || fields.size() != 4)
            throw new IOException("Invalid Redis snapshot metadata");
        return fields;
    }

    InputStream payload(long maxLength) throws IOException {
        long length = number('$');
        if (length < 0 || length > maxLength) throw new IOException("Invalid Redis stream payload length");
        return new InputStream() {
            private long remaining = length;
            private boolean ended;
            private boolean closed;

            @Override public int read() throws IOException {
                if (!ready()) return -1;
                try {
                    int value = input.readByte() & 0xff;
                    remaining--;
                    return value;
                } catch (RuntimeException failure) { throw failed(failure); }
            }

            @Override public int read(byte[] buffer, int offset, int count) throws IOException {
                Objects.checkFromIndexSize(offset, count, buffer.length);
                if (count == 0) return 0;
                if (!ready()) return -1;
                try {
                    int read = input.read(buffer, offset, (int) Math.min(remaining, count));
                    remaining -= read;
                    return read;
                } catch (RuntimeException failure) { throw failed(failure); }
            }

            private boolean ready() throws IOException {
                if (closed) throw new IOException("Redis object stream is closed");
                if (ended) return false;
                if (remaining > 0) return true;
                try {
                    if (input.readByte() != '\r' || input.readByte() != '\n')
                        throw new IOException("Invalid Redis object stream terminator");
                    ended = true;
                    RedisTransfer.this.close();
                    return false;
                } catch (IOException | RuntimeException failure) { throw failed(failure); }
            }

            private IOException failed(Exception failure) {
                var result = new IOException("Redis object transfer failed", failure);
                try { close(); } catch (IOException close) { result.addSuppressed(close); }
                return result;
            }

            @Override public void close() throws IOException {
                if (!closed) {
                    closed = true;
                    RedisTransfer.this.close();
                }
            }
        };
    }

    // Only framing integers are parsed here. Bound their length before allocating or
    // consuming a bulk value; Redis errors retain their message without request secrets.
    private long number(char marker) throws IOException {
        byte actual = input.readByte();
        if (actual == '-') throw new IOException("Redis transfer refused: " + line(1024));
        if (actual != marker) throw new IOException("Unexpected Redis stream response");
        String text = line(20);
        try { return Long.parseLong(text); }
        catch (NumberFormatException failure) { throw new IOException("Invalid Redis stream length", failure); }
    }

    private String line(int limit) throws IOException {
        var result = new StringBuilder();
        for (int i = 0; i <= limit; i++) {
            byte value = input.readByte();
            if (value == '\r') {
                if (input.readByte() != '\n') throw new IOException("Invalid Redis response terminator");
                return result.toString();
            }
            if (i == limit) throw new IOException("Redis response header exceeds its bound");
            result.append((char) (value & 0xff));
        }
        throw new IOException("Invalid Redis response header");
    }

    private void requireOk() throws IOException {
        byte marker = input.readByte();
        if (marker == '-') throw new IOException("Redis transfer setup refused: " + line(1024));
        if (marker != '+' || !line(2).equals("OK"))
            throw new IOException("Redis transfer setup failed");
    }

    @Override public void close() throws IOException {
        try { socket.close(); } finally { onClose.accept(this); }
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
