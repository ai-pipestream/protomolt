package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class UploadCommitGateTest {
    @Test
    void aClaimedCommitCannotBeWithdrawn() {
        var gate = new UploadCommitGate();
        assertThat(gate.claimCommit()).isTrue();
        assertThat(gate.withdraw()).isFalse();
        assertThat(gate.claimCommit()).isFalse();
    }

    @Test
    void aWithdrawnCallCannotClaimItsCommit() {
        var gate = new UploadCommitGate();
        assertThat(gate.withdraw()).isTrue();
        assertThat(gate.claimCommit()).isFalse();
        assertThat(gate.withdraw()).isFalse();
    }

    @Test
    void exactlyOneOfAConcurrentClaimAndWithdrawalWins() throws Exception {
        try (var threads = Executors.newFixedThreadPool(2)) {
            for (int round = 0; round < 10_000; round++) {
                var gate = new UploadCommitGate();
                var start = new CyclicBarrier(2);
                var claimed = threads.submit(() -> {
                    start.await();
                    return gate.claimCommit();
                });
                var withdrawn = threads.submit(() -> {
                    start.await();
                    return gate.withdraw();
                });
                assertThat(claimed.get(10, TimeUnit.SECONDS) ^ withdrawn.get(10, TimeUnit.SECONDS))
                        .as("round %d", round).isTrue();
            }
        }
    }
}
