package ai.protomolt.proto.kafka.serde;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The documented defaults are the contract: a consumer that upgrades must not start rejecting
 * history ({@code validate.on.read} stays off), a producer validates and scores by default, and
 * a failed registry lookup stands for thirty seconds before a retry. These pin the
 * {@link ProtoMoltSerdeConfig#CONFIG_DEF} values those behaviors fall out of, plus the
 * validation the config definition itself performs.
 */
class ProtoMoltSerdeConfigTest {

    @Test
    void documentsTheDefaults() {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(Map.of());

        assertThat(config.getString(ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE)).isNull();
        assertThat(config.getPassword(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64)).isNull();
        assertThat(config.getString(ProtoMoltSerdeConfig.MESSAGE_TYPE)).isNull();
        assertThat(config.getString(ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY))
                .isEqualTo(Subjects.TOPIC);
        assertThat(config.getBoolean(ProtoMoltSerdeConfig.GENERATED_CLASSES)).isTrue();
        assertThat(config.getInt(ProtoMoltSerdeConfig.USE_SCHEMA_ID)).isZero();
        assertThat(config.getString(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL)).isNull();
        assertThat(config.getString(ProtoMoltSerdeConfig.SUBJECT)).isNull();
        assertThat(config.getLong(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS))
                .isEqualTo(30_000L);
        assertThat(config.getBoolean(ProtoMoltSerdeConfig.LATEST_COMPATIBILITY_STRICT)).isTrue();
        assertThat(config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_WRITE)).isTrue();
        assertThat(config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_READ)).isFalse();
        assertThat(config.getBoolean(ProtoMoltSerdeConfig.QUALITY_ON_WRITE)).isTrue();
        assertThat(config.getBoolean(ProtoMoltSerdeConfig.QUALITY_ON_READ)).isFalse();
        assertThat(config.getDouble(ProtoMoltSerdeConfig.QUALITY_MIN)).isNull();
        assertThat(config.getList(ProtoMoltSerdeConfig.MAP_ON_WRITE)).isEmpty();
        assertThat(config.getList(ProtoMoltSerdeConfig.MAP_ON_READ)).isEmpty();
    }

    @Test
    void rejectsAnUnknownSubjectNameStrategy() {
        assertThatThrownBy(() -> new ProtoMoltSerdeConfig(
                Map.of(ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY, "bogus")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY);
    }

    @Test
    void rejectsANegativeRegistryRetryBackoff() {
        assertThatThrownBy(() -> new ProtoMoltSerdeConfig(
                Map.of(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS, -1L)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS);
    }

    /** The inline descriptor set is a PASSWORD, so it does not land in logs. */
    @Test
    void treatsTheInlineDescriptorSetAsAPassword() {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(
                Map.of(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, "aGVsbG8="));
        assertThat(config.getPassword(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64).value())
                .isEqualTo("aGVsbG8=");
    }

    @Test
    void acceptsMappingRulesAsLists() {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(Map.of(
                ProtoMoltSerdeConfig.MAP_ON_WRITE, List.of("a = b", "-c"),
                ProtoMoltSerdeConfig.MAP_ON_READ, List.of("d = e")));
        assertThat(config.getList(ProtoMoltSerdeConfig.MAP_ON_WRITE))
                .containsExactly("a = b", "-c");
        assertThat(config.getList(ProtoMoltSerdeConfig.MAP_ON_READ)).containsExactly("d = e");
    }
}
