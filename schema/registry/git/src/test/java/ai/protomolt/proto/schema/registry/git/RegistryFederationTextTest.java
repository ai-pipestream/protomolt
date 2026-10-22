package ai.protomolt.proto.schema.registry.git;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regex-free text helpers behind federation parsing, checked against the regexes they
 * replaced (the former {@code [a-z][a-z0-9-]{0,63}}, {@code v(\d+)\.proto} /
 * {@code v(\d+)\.json}, and {@code String.split("/")}).
 */
class RegistryFederationTextTest {

    private static final Pattern REMOTE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");
    private static final Pattern VERSION_PROTO = Pattern.compile("v(\\d+)\\.proto");
    private static final Pattern VERSION_META = Pattern.compile("v(\\d+)\\.json");

    @Test
    void remoteNamesMatchTheFormerPattern() {
        for (String name : new String[]{
                "a", "mesh-b", "a1", "r-2-x", "z".repeat(64),
                "", "A", "1a", "a_b", "a.b", "-a", "a-", "z".repeat(65),
                "a b", "mesh:b", "mésh", "a/", "v1"}) {
            assertThat(RegistryFederation.isRemoteName(name))
                    .as("remote name '%s'", name)
                    .isEqualTo(REMOTE_NAME.matcher(name).matches());
        }
    }

    @Test
    void versionDigitsMatchTheFormerPatterns() {
        for (String file : new String[]{
                "v1.proto", "v0.proto", "v42.proto", "v007.proto", "v2147483648.proto",
                "v.proto", "vx.proto", "v1x.proto", "x1.proto", "v1.protox", "v1proto",
                "v1.json", "v12.json", "v.json", "v1a.json", "1.json", "v１.proto",
                ".proto", ".json", "", "v", "v1.", "config.json"}) {
            Matcher proto = VERSION_PROTO.matcher(file);
            assertThat(RegistryFederation.versionDigits(file, ".proto"))
                    .as("proto version of '%s'", file)
                    .isEqualTo(proto.matches() ? proto.group(1) : null);
            Matcher meta = VERSION_META.matcher(file);
            assertThat(RegistryFederation.versionDigits(file, ".json"))
                    .as("meta version of '%s'", file)
                    .isEqualTo(meta.matches() ? meta.group(1) : null);
        }
    }

    @Test
    void slashSplittingMatchesStringSplit() {
        for (String path : new String[]{
                "subjects/orders/v1.proto", "a/b", "a", "", "/", "//", "a/", "a//",
                "/a", "//a", "a//b", "a/b/", "/a/b/c/", "///"}) {
            assertThat(RegistryFederation.splitOnSlash(path))
                    .as("split of '%s'", path)
                    .containsExactly(path.split("/"));
        }
    }

    @Test
    void subjectLayoutPathsSplitIntoThreeParts() {
        List<String> parts = RegistryFederation.splitOnSlash("subjects/orders%2Fvalue/v3.proto");
        assertThat(parts).containsExactly("subjects", "orders%2Fvalue", "v3.proto");
        assertThat(RegistryFederation.versionDigits("v3.proto", ".proto")).isEqualTo("3");
    }
}
