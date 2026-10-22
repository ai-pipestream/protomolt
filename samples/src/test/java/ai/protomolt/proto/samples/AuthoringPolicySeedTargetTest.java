package ai.protomolt.proto.samples;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free fixture-target shape, checked against the former
 * {@code [A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}} full match.
 */
class AuthoringPolicySeedTargetTest {

    @Test
    void hostPortShapeMatchesTheFormerPattern() {
        for (String target : new String[]{
                "localhost:8080", "a:1", "a.b-c:65535", "0ops:12345", "A:99999",
                "", ":80", "host:", "host", "host:123456", "host:1a",
                "-host:80", ".host:80", "host:80:81", "ho_st:80", "h st:80",
                "host:-1", "hóst:80", "host:０80", "a:0", "a:00000"}) {
            assertThat(AuthoringPolicySeed.isHostPortShape(target))
                    .as("target '%s'", target)
                    .isEqualTo(target.matches("[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}"));
        }
    }
}
