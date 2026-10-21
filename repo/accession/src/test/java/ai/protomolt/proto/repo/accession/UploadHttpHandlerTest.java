package ai.protomolt.proto.repo.accession;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class UploadHttpHandlerTest {
    @Test
    void parsesQueryStringsWithoutRegex() {
        assertThat(UploadHttpHandler.parseQuery(null)).isEmpty();
        assertThat(UploadHttpHandler.parseQuery("   ")).isEmpty();
        assertThat(UploadHttpHandler.parseQuery("&&account_id=acct-1&&flag&account_id=acct-2&name=a%20b&"))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "account_id", "acct-1",
                        "flag", "",
                        "name", "a b"));
    }
}
