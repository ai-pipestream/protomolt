package ai.pipestream.proto.validate.protovalidate;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import ai.pipestream.proto.validate.protovalidate.testdata.AnnotatedUser;
import ai.pipestream.proto.validate.protovalidate.testdata.Plan;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates protovalidate-annotated messages through the standard validator with
 * {@link ProtovalidateRuleSource} in the chain — the interop the module exists for.
 */
class ProtovalidateRuleSourceTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtovalidateRuleSource()));

    private static AnnotatedUser.Builder validUser() {
        return AnnotatedUser.newBuilder()
                .setName("ada")
                .setPlan(Plan.PLAN_FREE)
                .setCreated(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond() - 60));
    }

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    @Test
    void validUserPasses() {
        ValidationResult result = VALIDATOR.validate(validUser().build());
        assertThat(result.valid())
                .as("expected no violations, got %s", result.violations())
                .isTrue();
    }

    @Test
    void requiredAndStringRules() {
        assertViolation(AnnotatedUser.newBuilder()
                        .setPlan(Plan.PLAN_FREE)
                        .setCreated(Timestamp.newBuilder().setSeconds(1000)).build(),
                "name", "required");
        assertViolation(validUser().setName("ab").build(), "name", "string.min_len");
    }

    @Test
    void emailWellKnownFormat() {
        assertViolation(validUser().setEmail("nope").build(), "email", "string.email");
    }

    @Test
    void unsignedAndSignedIntegerVariants() {
        // -1 as uint32 is 4294967295 and must fail lte: 150.
        assertViolation(validUser().setAge(-1).build(), "age", "uint32.lte");
        assertViolation(validUser().setKarma(-500).build(), "karma", "sint32.gte");
        assertViolation(validUser().setQuota(2000).build(), "quota", "fixed64.lt");
    }

    @Test
    void doubleFinite() {
        assertViolation(validUser().setScore(Double.NaN).build(), "score", "double.finite");
    }

    @Test
    void repeatedRules() {
        assertViolation(validUser().addTags("ab").addTags("ab").build(),
                "tags", "repeated.unique");
        assertViolation(validUser().addAllTags(List.of("aa", "bb", "cc", "dd")).build(),
                "tags", "repeated.max_items");
        assertViolation(validUser().addTags("x").build(), "tags[0]", "string.min_len");
    }

    @Test
    void mapKeyAndValueRules() {
        assertViolation(validUser().putLimits("a", 1).build(),
                "limits[\"a\"]#key", "string.min_len");
        assertViolation(validUser().putLimits("ab", -1).build(),
                "limits[\"ab\"]", "int32.gte");
    }

    @Test
    void enumDefinedOnly() {
        assertViolation(validUser().setPlanValue(9).build(), "plan", "enum.defined_only");
    }

    @Test
    void timestampAndDurationRules() {
        assertViolation(validUser().setCreated(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond() + 86400)).build(),
                "created", "timestamp.lt_now");
        assertViolation(validUser().setTtl(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(7200)).build(),
                "ttl", "duration.gte_lte");
    }

    @Test
    void celRulesTranslateVerbatim() {
        assertViolation(validUser().setNickname("has space").build(),
                "nickname", "nickname.no_spaces");
        assertViolation(validUser().setPlan(Plan.PLAN_PAID).build(),
                "AnnotatedUser", "user.paid_needs_email");
    }

    @Test
    void ignoreAlwaysDropsTheFieldEntirely() {
        // min_len: 100 would fail, but IGNORE_ALWAYS wins.
        ValidationResult result = VALIDATOR.validate(validUser().setIgnored("x").build());
        assertThat(result.violations()).noneMatch(v -> v.path().equals("ignored"));
    }

    @Test
    void serviceLoaderDiscoversTheDialect() {
        List<ValidationRuleSource> defaults = ValidationRuleSources.defaults();
        assertThat(defaults)
                .anyMatch(s -> s instanceof ProtovalidateRuleSource);
        // The default chain therefore validates buf-annotated messages out of the box.
        ProtoValidator validator = ProtoValidator.create();
        assertThat(validator.validate(validUser().setName("ab").build()).violations())
                .anyMatch(v -> v.ruleId().equals("string.min_len"));
    }
}
