package ai.protomolt.proto.validate.spi;

import ai.protomolt.proto.validate.source.AiPipestreamRuleSource;
import ai.protomolt.proto.validate.source.GoogleTypeRuleSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The rule-source chain factories. */
class ValidationRuleSourcesTest {

    @Test
    void defaultsLeadWithTheBuiltInReader() {
        List<ValidationRuleSource> sources = ValidationRuleSources.defaults();

        // No other ValidationRuleSource module is on this classpath, so the ServiceLoader
        // contributes nothing beyond the two built-in readers: the Pipestream dialect
        // first, then the google.type well-known constraints.
        assertThat(sources).hasSize(2);
        assertThat(sources.get(0)).isInstanceOf(AiPipestreamRuleSource.class);
        assertThat(sources.get(1)).isInstanceOf(GoogleTypeRuleSource.class);
    }

    @Test
    void defaultsReturnsAnImmutableFreshList() {
        List<ValidationRuleSource> first = ValidationRuleSources.defaults();
        List<ValidationRuleSource> second = ValidationRuleSources.defaults();

        assertThatThrownBy(() -> first.add(new AiPipestreamRuleSource()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(first).isNotSameAs(second);
        assertThat(first).hasSameSizeAs(second);
    }

    @Test
    void pipestreamOnlyIsExactlyTheBuiltInReader() {
        List<ValidationRuleSource> sources = ValidationRuleSources.pipestreamOnly();

        assertThat(sources).hasSize(1);
        assertThat(sources.get(0)).isInstanceOf(AiPipestreamRuleSource.class);
    }
}
