package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.StringGauntlet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
    }

    @Test
    void constAcceptsExactValue() {
        assertValid(StringGauntlet.newBuilder().setExact("fixed").build());
    }

    @Test
    void constRejectsOtherValues() {
        assertViolation(StringGauntlet.newBuilder().setExact("wrong").build(),
                "exact", "string.const");
    }

    @Test
    void lenCountsCodePoints() {
        assertValid(StringGauntlet.newBuilder().setThree("abc").build());
        // Three astral code points are six UTF-16 units but still length 3.
        assertValid(StringGauntlet.newBuilder().setThree("🎉🎉🎉").build());
        assertViolation(StringGauntlet.newBuilder().setThree("ab").build(),
                "three", "string.len");
        assertViolation(StringGauntlet.newBuilder().setThree("abcd").build(),
                "three", "string.len");
    }

    @Test
    void affixRules() {
        assertValid(StringGauntlet.newBuilder().setAffixed("premidfix").build());
        assertViolation(StringGauntlet.newBuilder().setAffixed("midfix").build(),
                "affixed", "string.prefix");
        assertViolation(StringGauntlet.newBuilder().setAffixed("premido").build(),
                "affixed", "string.suffix");
        assertViolation(StringGauntlet.newBuilder().setAffixed("prefix").build(),
                "affixed", "string.contains");
        assertViolation(StringGauntlet.newBuilder().setAffixed("premidbadfix").build(),
                "affixed", "string.not_contains");
    }

    @Test
    void inAcceptsListedAndRejectsOthers() {
        assertValid(StringGauntlet.newBuilder().setChoice("red").build());
        assertValid(StringGauntlet.newBuilder().setChoice("green").build());
        assertViolation(StringGauntlet.newBuilder().setChoice("blue").build(),
                "choice", "string.in");
    }

    @Test
    void notInRejectsListed() {
        assertValid(StringGauntlet.newBuilder().setVeto("fine").build());
        assertViolation(StringGauntlet.newBuilder().setVeto("nope").build(),
                "veto", "string.not_in");
    }

    @Test
    void uuidFormat() {
        assertValid(StringGauntlet.newBuilder()
                .setId("123e4567-e89b-12d3-a456-426614174000").build());
        assertViolation(StringGauntlet.newBuilder().setId("not-a-uuid").build(),
                "id", "string.uuid");
        assertViolation(StringGauntlet.newBuilder()
                        .setId("123e4567-e89b-12d3-a456-42661417400z").build(),
                "id", "string.uuid");
    }

    @Test
    void hostnameFormat() {
        assertValid(StringGauntlet.newBuilder().setHost("example.com").build());
        assertValid(StringGauntlet.newBuilder().setHost("localhost").build());
        assertViolation(StringGauntlet.newBuilder().setHost("-bad-.com").build(),
                "host", "string.hostname");
        assertViolation(StringGauntlet.newBuilder().setHost("under_score.com").build(),
                "host", "string.hostname");
        // A purely numeric final label is an IP, not a hostname.
        assertViolation(StringGauntlet.newBuilder().setHost("1.2.3.4").build(),
                "host", "string.hostname");
    }

    @Test
    void uriFormatRequiresAbsoluteUri() {
        assertValid(StringGauntlet.newBuilder().setLink("https://example.com/x?q=1").build());
        assertViolation(StringGauntlet.newBuilder().setLink("/relative/path").build(),
                "link", "string.uri");
        assertViolation(StringGauntlet.newBuilder().setLink("not a uri").build(),
                "link", "string.uri");
    }

    @Test
    void ipFormatAcceptsBothFamilies() {
        assertValid(StringGauntlet.newBuilder().setAddr("10.0.0.1").build());
        assertValid(StringGauntlet.newBuilder().setAddr("::1").build());
        assertViolation(StringGauntlet.newBuilder().setAddr("999.1.1.1").build(),
                "addr", "string.ip");
    }

    @Test
    void ipv4Format() {
        assertValid(StringGauntlet.newBuilder().setAddr4("192.168.0.1").build());
        assertViolation(StringGauntlet.newBuilder().setAddr4("::1").build(),
                "addr4", "string.ipv4");
        // Leading zeros are rejected (octal ambiguity).
        assertViolation(StringGauntlet.newBuilder().setAddr4("01.2.3.4").build(),
                "addr4", "string.ipv4");
        assertViolation(StringGauntlet.newBuilder().setAddr4("1.2.3").build(),
                "addr4", "string.ipv4");
    }

    @Test
    void ipv6Format() {
        assertValid(StringGauntlet.newBuilder().setAddr6("2001:db8::1").build());
        assertValid(StringGauntlet.newBuilder().setAddr6("fe80::").build());
        assertValid(StringGauntlet.newBuilder().setAddr6("::ffff:192.0.2.1").build());
        assertValid(StringGauntlet.newBuilder().setAddr6("1:2:3:4:5:6:7:8").build());
        assertViolation(StringGauntlet.newBuilder().setAddr6("12345::").build(),
                "addr6", "string.ipv6");
        // Seven groups without "::" is one group short.
        assertViolation(StringGauntlet.newBuilder().setAddr6("1:2:3:4:5:6:7").build(),
                "addr6", "string.ipv6");
        assertViolation(StringGauntlet.newBuilder().setAddr6("1::2::3").build(),
                "addr6", "string.ipv6");
    }

    @Test
    void unsetStringFieldsAreSkipped() {
        assertValid(StringGauntlet.getDefaultInstance());
    }
}
