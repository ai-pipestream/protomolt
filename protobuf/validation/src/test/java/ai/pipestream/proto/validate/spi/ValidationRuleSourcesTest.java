package ai.pipestream.proto.validate.spi;

import ai.pipestream.proto.validate.source.AiPipestreamRuleSource;
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
        // contributes nothing and the built-in Pipestream reader stands alone.
        assertThat(sources).hasSize(1);
        assertThat(sources.get(0)).isInstanceOf(AiPipestreamRuleSource.class);
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
