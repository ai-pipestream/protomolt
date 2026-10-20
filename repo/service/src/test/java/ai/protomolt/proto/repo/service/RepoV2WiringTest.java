package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the non-regex leading-slash strip that replaced
 * {@code replaceFirst("^/+", "")} in the drive-prefix normalization.
 */
class RepoV2WiringTest {

    @Test
    void stripsLeadingSlashesExactlyLikeTheRegexItReplaced() {
        assertThat(RepoV2Wiring.stripLeadingSlashes("")).isEqualTo("");
        assertThat(RepoV2Wiring.stripLeadingSlashes("/")).isEqualTo("");
        assertThat(RepoV2Wiring.stripLeadingSlashes("///")).isEqualTo("");
        assertThat(RepoV2Wiring.stripLeadingSlashes("/a")).isEqualTo("a");
        assertThat(RepoV2Wiring.stripLeadingSlashes("//a/b/")).isEqualTo("a/b/");
        assertThat(RepoV2Wiring.stripLeadingSlashes("a/b")).isEqualTo("a/b");
        assertThat(RepoV2Wiring.stripLeadingSlashes("a//")).isEqualTo("a//");
        assertThat(RepoV2Wiring.stripLeadingSlashes(" /a")).isEqualTo(" /a");
    }

    @Test
    void matchesTheReplacedRegexOnRepresentativeInputs() {
        // The oracle uses the regex the production code may no longer use.
        for (String input : new String[] {"", "/", "///", "/a", "//a/b/", "a/b", "a//", " /a", "/ /"}) {
            assertThat(RepoV2Wiring.stripLeadingSlashes(input))
                    .as("input '%s'", input)
                    .isEqualTo(input.replaceFirst("^/+", ""));
        }
    }
}
