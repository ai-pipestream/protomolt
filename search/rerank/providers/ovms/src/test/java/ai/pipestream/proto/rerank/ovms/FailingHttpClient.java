package ai.pipestream.proto.rerank.ovms;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An {@link HttpClient} that never reaches the network: {@link #send} either throws a fixed
 * {@link IOException} or a fixed {@link InterruptedException}, so tests can cover the provider's
 * failure wrapping without a socket. All other methods throw {@link UnsupportedOperationException}.
 */
final class FailingHttpClient extends HttpClient {

    private final IOException ioFailure;
    private final InterruptedException interruption;

    private FailingHttpClient(IOException ioFailure, InterruptedException interruption) {
        this.ioFailure = ioFailure;
        this.interruption = interruption;
    }

    static FailingHttpClient failingWith(IOException failure) {
        return new FailingHttpClient(failure, null);
    }

    static FailingHttpClient interrupting() {
        return new FailingHttpClient(null, new InterruptedException("interrupted while sending"));
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request,
            HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException {
        if (ioFailure != null) {
            throw ioFailure;
        }
        throw interruption;
    }

    @Override
    public Optional<CookieHandler> cookieHandler() {
        return Optional.empty();
    }

    @Override
    public Optional<Duration> connectTimeout() {
        return Optional.empty();
    }

    @Override
    public Redirect followRedirects() {
        return Redirect.NEVER;
    }

    @Override
    public Optional<ProxySelector> proxy() {
        return Optional.empty();
    }

    @Override
    public SSLContext sslContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SSLParameters sslParameters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Authenticator> authenticator() {
        return Optional.empty();
    }

    @Override
    public Version version() {
        return Version.HTTP_1_1;
    }

    @Override
    public Optional<Executor> executor() {
        return Optional.empty();
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
            HttpResponse.BodyHandler<T> responseBodyHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
            HttpResponse.BodyHandler<T> responseBodyHandler,
            HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
        throw new UnsupportedOperationException();
    }
}
