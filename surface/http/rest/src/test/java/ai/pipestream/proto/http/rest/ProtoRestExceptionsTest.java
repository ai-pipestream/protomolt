package ai.pipestream.proto.http.rest;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The exception vocabulary is the host's status-code mapping surface: messages name the
 * failing route, and structured accessors ({@code allowedMethods}, {@code maxRequestBytes})
 * feed response headers.
 */
class ProtoRestExceptionsTest {

    @Test
    void requestTooLargeCarriesTheConfiguredLimit() {
        RequestTooLargeException e = new RequestTooLargeException(1_048_576L);

        assertThat(e).isInstanceOf(ProtoRestException.class);
        assertThat(e.maxRequestBytes()).isEqualTo(1_048_576L);
        assertThat(e).hasMessageContaining("1048576");
    }

    @Test
    void httpMethodNotAllowedExposesAnImmutableAllowList() {
        HttpMethodNotAllowedException e =
                new HttpMethodNotAllowedException("GET", List.of("POST", "PUT"));

        assertThat(e.allowedMethods()).containsExactly("POST", "PUT");
        assertThat(e).hasMessageContaining("GET").hasMessageContaining("POST, PUT");
        assertThatThrownBy(() -> e.allowedMethods().add("DELETE"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void notFoundExceptionsNameTheMissingRoute() {
        assertThat(new ServiceNotFoundException("Billing"))
                .hasMessage("Service not found: Billing");
        assertThat(new MethodNotFoundException("Billing", "Charge"))
                .hasMessage("Method not found: Billing/Charge");
    }

    @Test
    void baseExceptionCarriesMessageAndCause() {
        RuntimeException cause = new RuntimeException("root");

        assertThat(new ProtoRestException("boom")).hasMessage("boom").hasNoCause();
        assertThat(new ProtoRestException("boom", cause)).hasMessage("boom").hasCause(cause);
    }

    @Test
    void malformedRequestSupportsBothConstructors() {
        RuntimeException cause = new RuntimeException("decode");

        assertThat(new MalformedRequestException("bad query"))
                .hasMessage("bad query")
                .hasNoCause();
        assertThat(new MalformedRequestException("bad query", cause))
                .hasMessage("bad query")
                .hasCause(cause);
    }

    @Test
    void unauthorizedAndInvocationExceptionsCarryTheirReasons() {
        assertThat(new UnauthorizedProtoRestException("no token")).hasMessage("no token");

        RuntimeException cause = new RuntimeException("io");
        assertThat(new ProtoRestInvocationException("failed")).hasMessage("failed").hasNoCause();
        assertThat(new ProtoRestInvocationException("failed", cause))
                .hasMessage("failed")
                .hasCause(cause);
    }
}
