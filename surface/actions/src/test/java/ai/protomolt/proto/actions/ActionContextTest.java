package ai.protomolt.proto.actions;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ActionContextTest {

    @Test
    void createProvidesARegistryWithWellKnownTypes() {
        ActionContext context = ActionContext.create();
        assertThat(context.registry()).isNotNull();
        assertThat(context.registry().findDescriptorByFullName("google.protobuf.Struct"))
                .isNotNull();
        assertThat(context.objectMapper()).isNotNull();
        assertThat(context.transcoder()).isNotNull();
    }

    @Test
    void builderAcceptsACustomRegistryAndMapper() {
        DescriptorRegistry registry = DescriptorRegistry.create();
        ObjectMapper mapper = new ObjectMapper();
        ActionContext context = ActionContext.builder()
                .registry(registry)
                .objectMapper(mapper)
                .build();
        assertThat(context.registry()).isSameAs(registry);
        assertThat(context.objectMapper()).isSameAs(mapper);
    }

    @Test
    void builderDefaultsMatchCreate() {
        ActionContext context = ActionContext.builder().build();
        assertThat(context.registry().findDescriptorByFullName("google.protobuf.Timestamp"))
                .isNotNull();
        assertThat(context.objectMapper()).isNotNull();
    }

    @Test
    void nullBuilderArgumentsAreRejected() {
        assertThatNullPointerException()
                .isThrownBy(() -> ActionContext.builder().registry(null));
        assertThatNullPointerException()
                .isThrownBy(() -> ActionContext.builder().objectMapper(null));
    }

    @Test
    void transcoderIsBoundToTheContextRegistry() throws Exception {
        ActionContext context = TestFixtures.personContext();
        Descriptor person = context.registry().findDescriptorByFullName("actions.test.Person");
        assertThat(person).isNotNull();
        DynamicMessage message = DynamicMessage.newBuilder(person)
                .setField(person.findFieldByName("name"), "Joseph")
                .setField(person.findFieldByName("age"), 42)
                .build();
        String json = context.transcoder().toJson(message);
        assertThat(context.objectMapper().readTree(json).get("name").asText())
                .isEqualTo("Joseph");

        DynamicMessage roundTrip = context.transcoder()
                .fromJsonDynamic("{\"name\": \"Jo\", \"age\": 7}", person);
        assertThat(roundTrip.getField(person.findFieldByName("name"))).isEqualTo("Jo");
        assertThat(roundTrip.getField(person.findFieldByName("age"))).isEqualTo(7);
    }
}
