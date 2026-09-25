package ai.protomolt.proto.inference.v1;

import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModelEntryContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static ModelEntry.Builder entry() {
        return ModelEntry.newBuilder().setId("model").setProvider("provider")
                .setEndpoint("https://models.example.test");
    }

    @Test
    void anUncredentialedEndpointMayOmitCredentialReference() {
        assertThat(VALIDATOR.validate(entry().build()).valid()).isTrue();
    }

    @Test
    void anEnvironmentCredentialReferenceMayBeProvided() {
        assertThat(VALIDATOR.validate(entry().setCredentialRef("env:MODEL_API_KEY").build()).valid())
                .isTrue();
    }

    @Test
    void credentialReferenceMustUseTheOpaqueSchemeAndNameForm() {
        for (String invalid : new String[] {
                "MODEL_API_KEY", "env:", "ENV:MODEL_API_KEY", "env:MODEL API KEY"
        }) {
            assertThat(VALIDATOR.validate(entry().setCredentialRef(invalid).build()).valid())
                    .as("credential reference %s", invalid).isFalse();
        }
    }

    @Test
    void credentialReferenceChangeDoesNotRelaxRequiredCatalogIdentity() {
        assertThat(VALIDATOR.validate(entry().setCredentialRef("env:MODEL_API_KEY")
                .clearId().build()).valid()).isFalse();
        assertThat(VALIDATOR.validate(entry().setCredentialRef("env:MODEL_API_KEY")
                .clearProvider().build()).valid()).isFalse();
        assertThat(VALIDATOR.validate(entry().setCredentialRef("env:MODEL_API_KEY")
                .clearEndpoint().build()).valid()).isFalse();
    }
}
