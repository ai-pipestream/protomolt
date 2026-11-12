package ai.protomolt.proto.repo.blob.cache;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheSpoolTest {
    @Test void ceilingIsCheckedBeforeWritingAndFinishedBytesStayExact() throws Exception {
        try (var spool = new CacheSpool(3)) {
            spool.write(new byte[]{1, 2}, 0, 2);
            assertThatThrownBy(() -> spool.write(new byte[]{3, 4}, 0, 2))
                    .isInstanceOf(IOException.class).hasMessageContaining("ceiling");
            assertThat(spool.size()).isEqualTo(2);
            spool.write(new byte[]{3}, 0, 1);
            spool.finish();
            assertThat(spool.sha()).isEqualTo("039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81");
            try (var input = spool.ownedStream()) { assertThat(input.readAllBytes()).containsExactly(1, 2, 3); }
            assertThatThrownBy(spool::open).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void cancellationClosesTheFileBeforeTheBodyHasBeenConsumed() throws Exception {
        var spool = new CacheSpool(128 * 1024);
        spool.write(new byte[128 * 1024], 0, 128 * 1024);
        spool.finish();
        var input = spool.ownedStream();
        assertThat(input.read()).isZero();
        input.close();
        input.close();
        assertThatThrownBy(input::read).isInstanceOf(IOException.class);
        spool.close();
    }

    @Test void emptySpoolRequiresFinishingAndHasTheEmptyDigest() throws Exception {
        try (var spool = new CacheSpool(0)) {
            assertThatThrownBy(spool::open).isInstanceOf(IllegalStateException.class);
            spool.finish();
            assertThat(spool.sha()).isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
            try (var input = spool.open()) { assertThat(input.read()).isEqualTo(-1); }
        }
    }
}
