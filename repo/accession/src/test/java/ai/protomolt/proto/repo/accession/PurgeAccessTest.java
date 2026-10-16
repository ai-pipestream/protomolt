package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveRejection;
import io.grpc.Status;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PurgeAccessTest {
    private final RepositoryCaller member = new RepositoryCaller("member", false, Set.of("account"), Set.of());

    @Test void processAuthorityAndUnboundCallersNeverReachHostPolicy() {
        var calls = new AtomicInteger();
        var access = new PurgeAccess((caller, account) -> {
            calls.incrementAndGet();
            return new PurgeAuthorization.Decision(true, true);
        });
        denied(access, new RepositoryCaller("process", true));
        denied(access, new RepositoryCaller("unbound", false));
        denied(access, new RepositoryCaller("other", false, Set.of("other-account"), Set.of()));
        assertThat(calls.get()).isZero();
    }

    @Test void administratorAndAccountWritePermissionAreBothRequired() {
        for (boolean administrator : new boolean[]{false, true}) {
            for (boolean write : new boolean[]{false, true}) {
                var access = new PurgeAccess((caller, account) -> {
                    assertThat(caller).isSameAs(member);
                    assertThat(account).isEqualTo("account");
                    return new PurgeAuthorization.Decision(administrator, write);
                });
                if (administrator && write) assertThatCode(() -> access.require(member, "account")).doesNotThrowAnyException();
                else denied(access, member);
            }
        }
    }

    @Test void missingPolicyOrDecisionNeverAuthorizes() {
        assertThatNullPointerException().isThrownBy(() -> new PurgeAccess(null));
        assertThatNullPointerException().isThrownBy(() -> new PurgeAccess((c, a) -> null).require(member, "account"));
    }

    @Test void hostFailurePropagatesUnchanged() {
        var failure = new IllegalStateException("directory unavailable");
        var access = new PurgeAccess((c, a) -> { throw failure; });
        assertThatThrownBy(() -> access.require(member, "account")).isSameAs(failure);
    }

    private static void denied(PurgeAccess access, RepositoryCaller caller) {
        var failure = catchThrowableOfType(() -> access.require(caller, "account"), AccessionException.class);
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(failure.detail()).isPresent();
        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
    }
}
