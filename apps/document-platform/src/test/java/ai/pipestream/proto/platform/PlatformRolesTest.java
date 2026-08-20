package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The read of PROTOMOLT_ROLES: a comma list of role ids, trimmed and
 * lowercased; absent means the full one-container preset. Canonical ids
 * are singular and share their module tree's stem, and the accepted
 * aliases normalize to them, so naming one role under both spellings is a
 * contradiction refused by both.
 */
class PlatformRolesTest {

    @Test
    void absentMeansTheFullPreset() {
        assertThat(DocumentPlatformConfig.rolesFromEnvironment(null))
                .isEqualTo(DocumentPlatformConfig.DEFAULT_ROLES);
        assertThat(DocumentPlatformConfig.rolesFromEnvironment("  "))
                .isEqualTo(DocumentPlatformConfig.DEFAULT_ROLES);
    }

    @Test
    void canonicalIdsParseTrimmedAndLowercasedInOrder() {
        assertThat(DocumentPlatformConfig.rolesFromEnvironment(
                " Search , metric,, parse-text "))
                .containsExactly("search", "metric", "parse-text");
    }

    @Test
    void aliasesNormalizeToTheirCanonicalId() {
        assertThat(DocumentPlatformConfig.rolesFromEnvironment("search,metrics,parser-text"))
                .containsExactly("search", "metric", "parse-text");
    }

    @Test
    void anAliasBesideItsCanonicalIdRefusesNamingBothSpellings() {
        assertThatThrownBy(() -> DocumentPlatformConfig.rolesFromEnvironment(
                "search,metric,metrics"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DocumentPlatformConfig.ENV_ROLES)
                .hasMessageContaining("as 'metric' and 'metrics'");
        assertThatThrownBy(() -> DocumentPlatformConfig.rolesFromEnvironment(
                "parser-text,parse-text"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("as 'parser-text' and 'parse-text'");
    }

    @Test
    void everyAliasNamesAKnownRole() {
        assertThat(DocumentPlatformConfig.ALIASES.values())
                .isSubsetOf(DocumentPlatformConfig.KNOWN_ROLES);
    }
}
