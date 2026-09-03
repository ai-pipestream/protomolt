package ai.protomolt.proto.validate.model;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The format enums' wiring: stable violation rule ids (with their {@code _empty} companions) and
 * delegation to the formats library. Detailed format semantics are covered by the formats
 * module's own tests; here a smoke case per accessor proves the enum is wired to a real check.
 */
class FormatEnumsTest {

    @Test
    void stringFormatRuleIds() {
        assertThat(StringFormat.EMAIL.ruleId()).isEqualTo("string.email");
        assertThat(StringFormat.HOSTNAME.ruleId()).isEqualTo("string.hostname");
        assertThat(StringFormat.ADDRESS.ruleId()).isEqualTo("string.address");
        assertThat(StringFormat.UUID.ruleId()).isEqualTo("string.uuid");
        assertThat(StringFormat.TUUID.ruleId()).isEqualTo("string.tuuid");
        assertThat(StringFormat.ULID.ruleId()).isEqualTo("string.ulid");
        assertThat(StringFormat.URI.ruleId()).isEqualTo("string.uri");
        assertThat(StringFormat.URI_REF.ruleId()).isEqualTo("string.uri_ref");
        assertThat(StringFormat.IP.ruleId()).isEqualTo("string.ip");
        assertThat(StringFormat.IPV4.ruleId()).isEqualTo("string.ipv4");
        assertThat(StringFormat.IPV6.ruleId()).isEqualTo("string.ipv6");
        assertThat(StringFormat.IP_PREFIX.ruleId()).isEqualTo("string.ip_prefix");
        assertThat(StringFormat.IPV4_PREFIX.ruleId()).isEqualTo("string.ipv4_prefix");
        assertThat(StringFormat.IPV6_PREFIX.ruleId()).isEqualTo("string.ipv6_prefix");
        assertThat(StringFormat.IP_WITH_PREFIXLEN.ruleId()).isEqualTo("string.ip_with_prefixlen");
        assertThat(StringFormat.IPV4_WITH_PREFIXLEN.ruleId()).isEqualTo("string.ipv4_with_prefixlen");
        assertThat(StringFormat.IPV6_WITH_PREFIXLEN.ruleId()).isEqualTo("string.ipv6_with_prefixlen");
        assertThat(StringFormat.PROTOBUF_FQN.ruleId()).isEqualTo("string.protobuf_fqn");
        assertThat(StringFormat.PROTOBUF_DOT_FQN.ruleId()).isEqualTo("string.protobuf_dot_fqn");
        assertThat(StringFormat.HOST_AND_PORT.ruleId()).isEqualTo("string.host_and_port");
    }

    @Test
    void stringFormatEmptyCompanions() {
        for (StringFormat format : StringFormat.values()) {
            assertThat(format.emptyRuleId()).isEqualTo(format.ruleId() + "_empty");
            assertThat(format.emptyMessage()).isEqualTo("value is empty");
            assertThat(format.defaultMessage()).isNotBlank();
        }
        // Every rule id is distinct, so violations never alias between formats.
        assertThat(EnumSet.allOf(StringFormat.class).stream().map(StringFormat::ruleId))
                .doesNotHaveDuplicates();
    }

    @Test
    void stringFormatDelegationSmoke() {
        assertThat(StringFormat.EMAIL.matches("user@example.com")).isTrue();
        assertThat(StringFormat.EMAIL.matches("nope")).isFalse();
        assertThat(StringFormat.UUID.matches("123e4567-e89b-12d3-a456-426614174000")).isTrue();
        assertThat(StringFormat.UUID.matches("nope")).isFalse();
        assertThat(StringFormat.IP.matches("::1")).isTrue();
        assertThat(StringFormat.IPV4.matches("127.0.0.1")).isTrue();
        assertThat(StringFormat.IPV4.matches("::1")).isFalse();
        assertThat(StringFormat.IPV6.matches("::1")).isTrue();
        assertThat(StringFormat.IPV6.matches("127.0.0.1")).isFalse();
        assertThat(StringFormat.HOSTNAME.matches("example.com")).isTrue();
        assertThat(StringFormat.HOSTNAME.matches("-bad-.com")).isFalse();
    }

    @Test
    void bytesFormatPackedLengths() {
        assertThat(BytesFormat.IP.matches(4)).isTrue();
        assertThat(BytesFormat.IP.matches(16)).isTrue();
        assertThat(BytesFormat.IP.matches(5)).isFalse();
        assertThat(BytesFormat.IPV4.matches(4)).isTrue();
        assertThat(BytesFormat.IPV4.matches(16)).isFalse();
        assertThat(BytesFormat.IPV6.matches(16)).isTrue();
        assertThat(BytesFormat.IPV6.matches(4)).isFalse();
        assertThat(BytesFormat.UUID.matches(16)).isTrue();
        assertThat(BytesFormat.UUID.matches(15)).isFalse();
    }

    @Test
    void bytesFormatRuleIds() {
        assertThat(BytesFormat.IP.ruleId()).isEqualTo("bytes.ip");
        assertThat(BytesFormat.IPV4.ruleId()).isEqualTo("bytes.ipv4");
        assertThat(BytesFormat.IPV6.ruleId()).isEqualTo("bytes.ipv6");
        assertThat(BytesFormat.UUID.ruleId()).isEqualTo("bytes.uuid");
        for (BytesFormat format : BytesFormat.values()) {
            assertThat(format.emptyRuleId()).isEqualTo(format.ruleId() + "_empty");
            assertThat(format.defaultMessage()).isNotBlank();
        }
    }
}
