package ai.protomolt.proto.repo.blob.redis;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLSocketFactory;
import redis.clients.jedis.SslOptions;

/**
 * One TLS context for every dedicated transfer socket of a store.
 * Jedis 8.0.2 {@code createSslSocket} calls {@link SslOptions#createSslContext()}
 * on each socket when {@code sslOptions} is set, so callers pass
 * {@link #socketFactory()} and leave Jedis {@code sslOptions} unset.
 */
final class RedisTls {
    private static final AtomicInteger CONTEXTS = new AtomicInteger();
    private final SSLSocketFactory socketFactory;

    private RedisTls(SSLSocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    /** Builds the context from {@link SslOptions#defaults()} and retains its socket factory. */
    static RedisTls open() {
        try {
            SSLSocketFactory factory = SslOptions.defaults().createSslContext().getSocketFactory();
            CONTEXTS.incrementAndGet();
            return new RedisTls(factory);
        } catch (IOException | GeneralSecurityException failure) {
            throw new IllegalStateException("Redis TLS context failed", failure);
        }
    }

    SSLSocketFactory socketFactory() { return socketFactory; }

    /** Contexts successfully built by {@link #open()}. Plaintext stores do not increment it. */
    static int contextsCreated() { return CONTEXTS.get(); }
}
