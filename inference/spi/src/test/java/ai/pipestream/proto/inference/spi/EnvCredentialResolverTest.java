package ai.pipestream.proto.inference.spi;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvCredentialResolverTest {

    private static final String REF = "env:OPENAI_TOKEN";
    private static final String SECRET = "sk-test-9f8e7d6c5b";

    private final EnvCredentialResolver resolver =
            new EnvCredentialResolver(Map.of("OPENAI_TOKEN", SECRET)::get);

    @Test
    void resolvesTheEnvScheme() {
        assertThat(resolver.resolve(REF)).isEqualTo(SECRET);
    }

    @Test
    void malformedReferenceFailsFastWithoutEchoingIt() {
        assertThatThrownBy(() -> resolver.resolve("OPENAI TOKEN"))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("malformed")
                .hasMessageNotContaining("OPENAI TOKEN");
        assertThatThrownBy(() -> resolver.resolve("env:"))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("malformed");
        assertThatThrownBy(() -> resolver.resolve(null))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("malformed");
    }

    @Test
    void unsupportedSchemeFailsFastWithoutEchoingTheReference() {
        assertThatThrownBy(() -> resolver.resolve("vault:secret/data/openai"))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("unsupported")
                .hasMessageNotContaining("vault:secret/data/openai");
    }

    @Test
    void unsetVariableFailsFastWithoutEchoingTheReference() {
        assertThatThrownBy(() -> resolver.resolve("env:DEFINITELY_UNSET_VAR"))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("unset or empty")
                .hasMessageNotContaining("DEFINITELY_UNSET_VAR");
    }

    @Test
    void emptyVariableFailsFast() {
        EnvCredentialResolver empty = new EnvCredentialResolver(name -> "");
        assertThatThrownBy(() -> empty.resolve(REF))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("unset or empty");

        EnvCredentialResolver blank = new EnvCredentialResolver(name -> "   ");
        assertThatThrownBy(() -> blank.resolve(REF))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("unset or empty");
    }

    @Test
    void noFailureMessageEverCarriesTheSecret() {
        EnvCredentialResolver failing = new EnvCredentialResolver(name -> null);
        assertThatThrownBy(() -> failing.resolve(REF))
                .hasMessageNotContaining(SECRET);
    }

    @Test
    void checkFormatAcceptsWellFormedReferences() {
        CredentialResolver.checkFormat("env:OPENAI_TOKEN");
        CredentialResolver.checkFormat("env:x.y/z-w_1");
    }

    @Test
    void checkFormatRejectsMalformedReferencesWithoutEchoingThem() {
        assertThatThrownBy(() -> CredentialResolver.checkFormat("Env:OPENAI_TOKEN"))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageNotContaining("Env:OPENAI_TOKEN");
        assertThatThrownBy(() -> CredentialResolver.checkFormat(""))
                .isInstanceOf(CredentialResolutionException.class);
        assertThatThrownBy(() -> CredentialResolver.checkFormat(
                "env:" + "X".repeat(CredentialResolver.MAX_REFERENCE_LENGTH)))
                .isInstanceOf(CredentialResolutionException.class);
    }

    @Test
    void transportGuardRejectsBadCustomResolverResultsWithoutEchoingThem() {
        assertThatThrownBy(() -> CredentialResolver.resolveBearer(ref -> null, REF))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("no credential material");
        assertThatThrownBy(() -> CredentialResolver.resolveBearer(ref -> "bad token", REF))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("not valid bearer-token");
        assertThatThrownBy(() -> CredentialResolver.resolveBearer(ref ->
                        "x".repeat(CredentialResolver.MAX_CREDENTIAL_LENGTH + 1), REF))
                .isInstanceOf(CredentialResolutionException.class)
                .hasMessageContaining("size limit");

        String secret = "secret-from-resolver-exception";
        CredentialResolutionException failure = org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> CredentialResolver.resolveBearer(ref -> {
                    throw new CredentialResolutionException(secret);
                }, REF), CredentialResolutionException.class);
        assertThat(failure.getMessage().contains(secret)).isFalse();
    }
}
