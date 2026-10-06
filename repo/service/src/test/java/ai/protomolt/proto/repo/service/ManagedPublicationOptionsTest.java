package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ManagedPublicationOptionsTest {
    @Test void deliveryCapacityCannotExcludeValidMaximumRequestsAtConstruction() {
        var minimum=DocumentPublicationGrpcService.MAX_CALL_RESERVATION_BYTES;
        assertThatThrownBy(() -> new ManagedPublicationOptions.Transport(
                auth -> new RepositoryCaller(auth.caller().name(),auth.caller().unrestricted()),minimum-1,1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("one maximum request and response");
        assertThatCode(() -> new ManagedPublicationOptions.Transport(
                auth -> new RepositoryCaller(auth.caller().name(),auth.caller().unrestricted()),minimum,1))
                .doesNotThrowAnyException();
    }

    @Test void libraryOptionsDoNotInferRecoveryOrTransportAuthority() {
        var caller=new RepositoryCaller("host",true);
        var options=new ManagedPublicationOptions(Path.of("bundle"),Duration.ofMinutes(5),Duration.ofSeconds(5),
                (account,principal,operation) -> caller);
        assertThat(options.transport()).isEmpty();
        assertThat(options.recoveryAuthority()).isEmpty();
        var selected=options.withRecovery((account,principal,operation) -> caller);
        assertThat(selected.journaled().recovery()).isNotNull();
        assertThat(selected.journaled().transport()).isNull();
        assertThat(options.recoveryAuthority()).isEmpty();
    }
}
