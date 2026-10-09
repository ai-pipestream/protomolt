package ai.protomolt.proto.repo.spi;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PhysicalObjectIdentityTest {
    private static final String EMPTY_SHA256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private PhysicalObjectLocation location() {
        return new PhysicalObjectLocation(UUID.randomUUID(), "generation-1", "realm-1", "objects", "content/1");
    }

    @Test void exactCoordinatesAndOpaqueVersionsSurviveWithoutNormalization() {
        var location = new PhysicalObjectLocation(UUID.randomUUID(), "generation-1", "realm-1",
                "objects", " path/%2F/../résumé ");
        var identity = new PhysicalObjectIdentity(location, Optional.of(" version/+= "), 0,
                EMPTY_SHA256, "application/octet-stream");
        assertThat(identity.location().key()).isEqualTo(" path/%2F/../résumé ");
        assertThat(identity.providerVersion()).contains(" version/+= ");
        assertThat(identity.sizeBytes()).isZero();
    }

    @Test void equalDigestsDoNotCollapseDifferentPhysicalIdentities() {
        var first = new PhysicalObjectIdentity(location(), Optional.empty(), 0, EMPTY_SHA256, "text/plain");
        var second = new PhysicalObjectIdentity(location(), Optional.empty(), 0, EMPTY_SHA256, "text/plain");
        assertThat(first).isNotEqualTo(second);
        assertThat(first.sha256()).isEqualTo(second.sha256());
    }

    @Test void absentVersionIsExplicitAndBlankVersionIsInvalid() {
        assertThat(new PhysicalObjectIdentity(location(), Optional.empty(), 0, EMPTY_SHA256,
                "text/plain").providerVersion()).isEmpty();
        assertThatThrownBy(() -> new PhysicalObjectIdentity(location(), null, 0, EMPTY_SHA256,
                "text/plain")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new PhysicalObjectIdentity(location(), Optional.of(" "), 0,
                EMPTY_SHA256, "text/plain")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void rejectsMalformedMeasurementsAndUnknownCoordinates() {
        for (String digest : new String[] {"", "a".repeat(63), "a".repeat(65), "A".repeat(64), "g".repeat(64)}) {
            assertThatThrownBy(() -> new PhysicalObjectIdentity(location(), Optional.empty(), 0,
                    digest, "text/plain")).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new PhysicalObjectIdentity(location(), Optional.empty(), -1,
                EMPTY_SHA256, "text/plain")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PhysicalObjectIdentity(location(), Optional.empty(), 0,
                EMPTY_SHA256, " ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PhysicalObjectLocation(UUID.randomUUID(), "", "realm", "objects", "key"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PhysicalObjectLocation(UUID.randomUUID(), "generation", "realm", "objects", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
