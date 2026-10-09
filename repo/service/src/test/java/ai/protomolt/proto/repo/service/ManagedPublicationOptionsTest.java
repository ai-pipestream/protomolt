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
        assertThat(options.historical()).isEmpty();
        var selected=options.withRecovery((account,principal,operation) -> caller);
        assertThat(selected.journaled().recovery()).isNotNull();
        assertThat(selected.journaled().transport()).isNull();
        assertThat(options.recoveryAuthority()).isEmpty();
    }

    @Test void historicalPublicationRequiresExplicitCapacityAndRecoveryAuthority() {
        var caller = new RepositoryCaller("host", true);
        var options = new ManagedPublicationOptions(Path.of("unopened-bundle"), Duration.ofMinutes(5), Duration.ofSeconds(5),
                (account, principal, operation) -> caller);
        assertThatThrownBy(() -> options.withHistoricalPublication(2))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit recovery authority");
        var recovery = options.withRecovery((account, principal, operation) -> caller);
        for (int invalid : new int[] {0, -1}) assertThatThrownBy(() -> recovery.withHistoricalPublication(invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("capacity must be positive");
        assertThatThrownBy(() -> new ManagedPublicationOptions(options.assessmentRuntimeBundle(), options.assessmentRetention(),
                options.minimumAssessmentRemaining(), options.drainAuthority(), java.util.Optional.empty(), java.util.Optional.empty(),
                java.util.Optional.of(new ManagedPublicationOptions.Historical(2))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit recovery authority");
        var historical = recovery.withHistoricalPublication(2);
        var transported = historical.withTransport(new ManagedPublicationOptions.Transport(auth -> caller,
                DocumentPublicationGrpcService.MAX_CALL_RESERVATION_BYTES, 1));
        assertThat(transported.historical().orElseThrow().generationCapacity()).isEqualTo(2);
        assertThat(transported.withRecovery((account, principal, operation) -> caller).journaled().historical())
                .isEqualTo(historical.historical());
        assertThat(options.historical()).isEmpty();
        assertThat(recovery.historical()).isEmpty();
    }
}
