package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.search.embedding.VectorizationPolicy;
import ai.protomolt.proto.search.service.RepoDocumentMapping;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Which content this node may send to an embedding provider. The default is the
 * platform's own decision, stated rather than assumed: the repo document's body is
 * classified {@code screened}, indexing it is what the product does, and every other
 * class still has to be named.
 */
class PlatformVectorizationTest {

    private static VectorizationPolicy of(String value) {
        return DocumentPlatform.vectorizationFromEnvironment(value == null
                ? Map.of()
                : Map.of(DocumentPlatformConfig.ENV_SEARCH_VECTORIZE_SENSITIVITY, value));
    }

    @Test
    void theDefaultPermitsTheBodyClassAndNothingElse() {
        VectorizationPolicy policy = of(null);
        assertThat(policy.permits(RepoDocumentMapping.BODY_SENSITIVITY)).isTrue();
        assertThat(policy.permits("")).isTrue();
        assertThat(policy.permits("pii")).isFalse();
        assertThat(policy.isUnrestricted()).isFalse();
    }

    @Test
    void anExplicitListReplacesTheDefaultRatherThanAddingToIt() {
        // The operator taking control takes all of it: naming 'pii' alone means this
        // node no longer vectorizes screened bodies, which is a real decision and must
        // not be silently widened back.
        VectorizationPolicy policy = of("pii");
        assertThat(policy.permits("pii")).isTrue();
        assertThat(policy.permits(RepoDocumentMapping.BODY_SENSITIVITY)).isFalse();
    }

    @Test
    void severalClassesParseAndWhitespaceIsIgnored() {
        VectorizationPolicy policy = of(" screened , internal ");
        assertThat(policy.permits("screened")).isTrue();
        assertThat(policy.permits("internal")).isTrue();
        assertThat(policy.permits("pii")).isFalse();
    }

    @Test
    void theWildcardPermitsEveryClass() {
        VectorizationPolicy policy = of("*");
        assertThat(policy.isUnrestricted()).isTrue();
        assertThat(policy.permits("anything-at-all")).isTrue();
    }

    @Test
    void aValueNamingNoClassRefusesRatherThanMeaningNothing() {
        assertThatThrownBy(() -> of(" , "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        DocumentPlatformConfig.ENV_SEARCH_VECTORIZE_SENSITIVITY);
    }
}
