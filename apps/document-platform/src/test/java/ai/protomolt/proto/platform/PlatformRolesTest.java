package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.acquire.jdbc.JdbcPullModule;
import ai.protomolt.proto.acquire.s3.S3PullModule;
import ai.protomolt.proto.intake.service.IntakeModule;
import ai.protomolt.proto.jobs.service.JobsModule;
import ai.protomolt.proto.metric.lucene.MetricServiceModule;
import ai.protomolt.proto.parse.playground.PlaygroundModule;
import ai.protomolt.proto.parse.service.ParseModule;
import ai.protomolt.proto.parse.text.TextParserModule;
import ai.protomolt.proto.registry.service.RegistryModule;
import ai.protomolt.proto.repo.service.RepoServiceModule;
import ai.protomolt.proto.search.console.SearchConsoleModule;
import ai.protomolt.proto.search.service.SearchServiceModule;
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

    /**
     * Both role sets are written as the mounting modules' own constants, so
     * these hold by construction and say so: an alias or a preset entry
     * naming something no module mounts cannot compile, and a role renamed
     * on its module renames both sets with it.
     */
    @Test
    void everyAliasNamesAKnownRole() {
        assertThat(DocumentPlatformConfig.ALIASES.values())
                .isSubsetOf(DocumentPlatformConfig.KNOWN_ROLES);
    }

    @Test
    void thePresetNamesOnlyKnownRoles() {
        assertThat(DocumentPlatformConfig.DEFAULT_ROLES)
                .isSubsetOf(DocumentPlatformConfig.KNOWN_ROLES);
    }

    @Test
    void everyModuleTheBinaryWiresIsNamedInTheKnownSet() {
        assertThat(DocumentPlatformConfig.KNOWN_ROLES).containsExactlyInAnyOrder(
                RepoServiceModule.ROLE, TextParserModule.ROLE, RegistryModule.ROLE,
                ParseModule.ROLE, JobsModule.ROLE, IntakeModule.ROLE,
                PlaygroundModule.ROLE, SearchServiceModule.ROLE,
                MetricServiceModule.ROLE, SearchConsoleModule.ROLE,
                S3PullModule.ROLE, JdbcPullModule.ROLE);
    }
}
