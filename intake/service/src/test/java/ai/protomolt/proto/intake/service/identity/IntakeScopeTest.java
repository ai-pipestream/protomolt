package ai.protomolt.proto.intake.service.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.Test;

class IntakeScopeTest {

    @Test
    void blankAccountIsRejectedLoudly() {
        assertThatThrownBy(() -> new IntakeScope(" ", Set.of(), Set.of(), Set.of(), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("accountId");
    }

    @Test
    void negativePayloadCapIsRejected() {
        assertThatThrownBy(() -> new IntakeScope("a", Set.of(), Set.of(), Set.of(), -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPayloadBytes");
    }

    @Test
    void emptySetsMeanUnrestrictedWithinTheAccount() {
        IntakeScope scope = IntakeScope.unrestricted("acct");
        assertThat(scope.allowsDatasource("any-ds")).isTrue();
        assertThat(scope.allowsDrive("any-drive")).isTrue();
        assertThat(scope.allowsMimeType("application/pdf")).isTrue();
        assertThat(scope.allowsMimeType(null)).isTrue();
        assertThat(scope.allowsPayloadSize(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void restrictionsNarrowEachAxisIndependently() {
        IntakeScope scope =
                new IntakeScope(
                        "acct", Set.of("ds-1"), Set.of("intake"), Set.of("text/plain"), 100L);
        assertThat(scope.allowsDatasource("ds-1")).isTrue();
        assertThat(scope.allowsDatasource("ds-2")).isFalse();
        assertThat(scope.allowsDrive("intake")).isTrue();
        assertThat(scope.allowsDrive("pipeline")).isFalse();
        assertThat(scope.allowsMimeType("text/plain")).isTrue();
        assertThat(scope.allowsMimeType("application/pdf")).isFalse();
        // A content-type-restricted key demands a declared type.
        assertThat(scope.allowsMimeType(null)).isFalse();
        assertThat(scope.allowsPayloadSize(100L)).isTrue();
        assertThat(scope.allowsPayloadSize(101L)).isFalse();
    }

    @Test
    void setsAreDefensivelyCopied() {
        java.util.HashSet<String> mutable = new java.util.HashSet<>(Set.of("ds-1"));
        IntakeScope scope = new IntakeScope("acct", mutable, Set.of(), Set.of(), 0L);
        mutable.add("ds-2");
        assertThat(scope.allowsDatasource("ds-2")).isFalse();
    }
}
