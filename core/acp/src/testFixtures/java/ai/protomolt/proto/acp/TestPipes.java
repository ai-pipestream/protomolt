package ai.protomolt.proto.acp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An in-memory full-duplex byte pipe for driving an agent over streams in tests.
 * {@link java.io.PipedInputStream} reports "Write end dead" when the thread that last wrote is
 * no longer alive, which virtual-thread writers are after every message, so the tests use this
 * instead: a growable buffer with wait/notify, with no notion of writer threads.
 */
public final class TestPipes {

    /** One end of the pair: {@code in} carries what the other end wrote, and vice versa. */
    public record End(InputStream in, OutputStream out) {
    }

    /** A connected pair of ends; what one end writes, the other reads. */
    public static End[] pair() {
        Pipe aToB = new Pipe();
        Pipe bToA = new Pipe();
        return new End[]{new End(aToB.input(), bToA.output()), new End(bToA.input(), aToB.output())};
    }

    private TestPipes() {
    }

    private static final class Pipe {
        private byte[] buffer = new byte[8192];
        private int head;
        private int size;
        private boolean closed;

        private final InputStream input = new InputStream() {
            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                synchronized (Pipe.this) {
                    while (size == 0) {
                        if (closed) {
                            return -1;
                        }
                        try {
                            Pipe.this.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("interrupted reading from the pipe", e);
                        }
                    }
                    int n = Math.min(len, size);
                    System.arraycopy(buffer, head, b, off, n);
                    head += n;
                    size -= n;
                    if (size == 0) {
                        head = 0;
                    }
                    return n;
                }
            }

            @Override
            public void close() {
                synchronized (Pipe.this) {
                    closed = true;
                    Pipe.this.notifyAll();
                }
            }
        };

        private final OutputStream output = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                write(new byte[]{(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                synchronized (Pipe.this) {
                    if (closed) {
                        throw new IOException("pipe closed");
                    }
                    if (head > 0 && head + size + len > buffer.length) {
                        System.arraycopy(buffer, head, buffer, 0, size);
                        head = 0;
                    }
                    if (size + len > buffer.length) {
                        byte[] grown = new byte[Math.max(buffer.length * 2, size + len)];
                        System.arraycopy(buffer, head, grown, 0, size);
                        buffer = grown;
                        head = 0;
                    }
                    System.arraycopy(b, off, buffer, head + size, len);
                    size += len;
                    Pipe.this.notifyAll();
                }
            }

            @Override
            public void close() {
                synchronized (Pipe.this) {
                    closed = true;
                    Pipe.this.notifyAll();
                }
            }
        };

        InputStream input() {
            return input;
        }

        OutputStream output() {
            return output;
        }
    }
}
