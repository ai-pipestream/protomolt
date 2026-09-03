package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MappingHelperJsonSupportTest {

    private final MappingHelper helper = new MappingHelper();
    private final MappingHelperJsonSupport json = new MappingHelperJsonSupport();

    @Test
    void fieldsToJsonContainsPaths() {
        Descriptor descriptor = Timestamp.getDescriptor();
        String out = json.fieldsToJson(helper.getFieldInfos(descriptor, 2));
        assertThat(out).contains("seconds").contains("nanos").contains("\"path\"");
    }

    @Test
    void schemaToJsonContainsChildren() {
        String out = json.schemaToJson(helper.exportSchema(Timestamp.getDescriptor(), 2));
        assertThat(out).contains("Timestamp").contains("children");
    }

    @Test
    void suggestionsAndValidationRoundTrip() {
        Descriptor d = Timestamp.getDescriptor();
        String suggestions = json.suggestionsToJson(helper.suggestTargetFields("seconds", d, d));
        assertThat(suggestions).contains("fieldPath").contains("score");

        String validation = json.validationToJson(helper.validateRule("seconds = seconds", d, d));
        assertThat(validation).contains("\"valid\"").contains("SUCCESS");
    }

    @Test
    void validationErrorSerializes() {
        String validation = json.validationToJson(
                MappingHelper.ValidationResult.error("boom"));
        assertThat(validation).contains("ERROR").contains("boom").contains("\"valid\" : false");
    }
}
