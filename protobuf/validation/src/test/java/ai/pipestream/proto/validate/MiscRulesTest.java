package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.MiscGauntlet;
import ai.pipestream.proto.validate.testdata.Rank;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Bool, bytes, and enum rules. */
class MiscRulesTest {

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

    private static ByteString bytes(int... values) {
        byte[] raw = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            raw[i] = (byte) values[i];
        }
        return ByteString.copyFrom(raw);
    }

    @Test
    void boolConstOnExplicitPresenceField() {
        assertValid(MiscGauntlet.newBuilder().setAccepted(true).build());
        // `optional bool` has explicit presence, so false is present and checkable.
        assertViolation(MiscGauntlet.newBuilder().setAccepted(false).build(),
                "accepted", "bool.const");
        assertValid(MiscGauntlet.getDefaultInstance());
    }

    @Test
    void bytesLengthBounds() {
        assertValid(MiscGauntlet.newBuilder().setPayload(bytes(1, 2, 3)).build());
        assertViolation(MiscGauntlet.newBuilder().setPayload(bytes(1)).build(),
                "payload", "bytes.min_len");
        assertViolation(MiscGauntlet.newBuilder().setPayload(bytes(1, 2, 3, 4, 5)).build(),
                "payload", "bytes.max_len");
    }

    @Test
    void bytesAffixRules() {
        assertValid(MiscGauntlet.newBuilder().setFramed(bytes(0x01, 0x42, 0xff)).build());
        assertViolation(MiscGauntlet.newBuilder().setFramed(bytes(0x02, 0x42, 0xff)).build(),
                "framed", "bytes.prefix");
        assertViolation(MiscGauntlet.newBuilder().setFramed(bytes(0x01, 0x42, 0x00)).build(),
                "framed", "bytes.suffix");
        assertViolation(MiscGauntlet.newBuilder().setFramed(bytes(0x01, 0x00, 0xff)).build(),
                "framed", "bytes.contains");
    }

    @Test
    void bytesExactLen() {
        assertValid(MiscGauntlet.newBuilder().setToken(bytes(9, 9, 9)).build());
        assertViolation(MiscGauntlet.newBuilder().setToken(bytes(9, 9)).build(),
                "token", "bytes.len");
    }

    @Test
    void enumDefinedOnlyRejectsUnknownNumbers() {
        assertValid(MiscGauntlet.newBuilder().setRank(Rank.RANK_BRONZE).build());
        // Proto3 enums are open: 9 is carried but not declared.
        assertViolation(MiscGauntlet.newBuilder().setRankValue(9).build(),
                "rank", "enum.defined_only");
    }

    @Test
    void enumNotIn() {
        assertViolation(MiscGauntlet.newBuilder().setRank(Rank.RANK_BANNED).build(),
                "rank", "enum.not_in");
    }

    @Test
    void enumConst() {
        assertValid(MiscGauntlet.newBuilder().setFixedRank(Rank.RANK_SILVER).build());
        assertViolation(MiscGauntlet.newBuilder().setFixedRank(Rank.RANK_BRONZE).build(),
                "fixed_rank", "enum.const");
    }

    @Test
    void enumIn() {
        assertValid(MiscGauntlet.newBuilder().setListedRank(Rank.RANK_BRONZE).build());
        assertViolation(MiscGauntlet.newBuilder().setListedRank(Rank.RANK_BANNED).build(),
                "listed_rank", "enum.in");
    }
}
