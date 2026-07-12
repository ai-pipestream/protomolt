package ai.pipestream.proto.jsonschema;

import ai.pipestream.proto.jsonschema.testdata.Account;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ProtoJsonSchemaGeneratorTest {

    private static final String ACCOUNT = "ai.pipestream.proto.jsonschema.testdata.v1.Account";

    private final Map<String, Object> schema =
            ProtoJsonSchemaGenerator.create().generate(Account.getDescriptor());

    @SuppressWarnings("unchecked")
    private Map<String, Object> accountDef() {
        Map<String, Object> defs = (Map<String, Object>) schema.get("$defs");
        return (Map<String, Object>) defs.get(ACCOUNT);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> property(String name) {
        Map<String, Object> properties = (Map<String, Object>) accountDef().get("properties");
        return (Map<String, Object>) properties.get(name);
    }

    @Test
    void rootRefsIntoDefs() {
        assertThat(schema.get("$schema"))
                .isEqualTo("https://json-schema.org/draft/2020-12/schema");
        assertThat(schema.get("$ref")).isEqualTo("#/$defs/" + ACCOUNT);
        assertThat(accountDef().get("type")).isEqualTo("object");
        assertThat(accountDef().get("title")).isEqualTo("Account");
    }

    @Test
    void requiredConstraintBecomesRequiredArray() {
        assertThat((List<Object>) accountDef().get("required")).containsExactly("username");
    }

    @Test
    void stringRulesMapToStringKeywords() {
        Map<String, Object> username = property("username");
        assertThat(username.get("type")).isEqualTo("string");
        assertThat(username.get("minLength")).isEqualTo(3L);
        assertThat(username.get("maxLength")).isEqualTo(20L);
        assertThat(username.get("pattern")).isEqualTo("^[a-z0-9_]+$");
    }

    @Test
    void emailBecomesFormat() {
        assertThat(property("email").get("format")).isEqualTo("email");
    }

    @Test
    void integerBoundsMapToMinimumAndExclusiveMaximum() {
        Map<String, Object> age = property("age");
        assertThat(age.get("type")).isEqualTo("integer");
        assertThat(age.get("minimum")).isEqualTo(13L);
        assertThat(age.get("exclusiveMaximum")).isEqualTo(200L);
    }

    @Test
    void uint64AcceptsIntegerOrStringWithUnsignedBounds() {
        Map<String, Object> balance = property("balance");
        assertThat((List<Object>) balance.get("type")).containsExactly("integer", "string");
        assertThat(balance.get("pattern")).isEqualTo("^[0-9]+$");
        assertThat(balance.get("minimum")).isEqualTo(0L);
        assertThat(balance.get("maximum")).isEqualTo(1000000L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void repeatedRulesMapToArrayKeywords() {
        Map<String, Object> roles = property("roles");
        assertThat(roles.get("type")).isEqualTo("array");
        assertThat(roles.get("minItems")).isEqualTo(1L);
        assertThat(roles.get("uniqueItems")).isEqualTo(true);
        Map<String, Object> items = (Map<String, Object>) roles.get("items");
        assertThat((List<Object>) items.get("enum")).containsExactly("admin", "user");
    }

    @Test
    @SuppressWarnings("unchecked")
    void mapRulesMapToObjectKeywords() {
        Map<String, Object> quotas = property("quotas");
        assertThat(quotas.get("type")).isEqualTo("object");
        Map<String, Object> values = (Map<String, Object>) quotas.get("additionalProperties");
        assertThat(values.get("type")).isEqualTo("integer");
        assertThat(values.get("minimum")).isEqualTo(0L);
        Map<String, Object> keys = (Map<String, Object>) quotas.get("propertyNames");
        assertThat(keys.get("minLength")).isEqualTo(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void enumAcceptsNamesOrNumbers() {
        Map<String, Object> tier = property("tier");
        List<Map<String, Object>> anyOf = (List<Map<String, Object>>) tier.get("anyOf");
        assertThat((List<Object>) anyOf.get(0).get("enum"))
                .containsExactly("TIER_UNSPECIFIED", "TIER_BASIC", "TIER_PRO");
        assertThat(anyOf.get(1).get("type")).isEqualTo("integer");
        // defined_only restricts the integer branch via a merged conjunction.
        List<Map<String, Object>> allOf = (List<Map<String, Object>>) tier.get("allOf");
        List<Map<String, Object>> definedOnly =
                (List<Map<String, Object>>) allOf.get(0).get("anyOf");
        assertThat((List<Object>) definedOnly.get(1).get("enum")).containsExactly(0L, 1L, 2L);
    }

    @Test
    void bytesCarryBase64ContentEncoding() {
        Map<String, Object> avatar = property("avatar");
        assertThat(avatar.get("type")).isEqualTo("string");
        assertThat(avatar.get("contentEncoding")).isEqualTo("base64");
    }

    @Test
    void boolConstMapsToConst() {
        assertThat(property("active").get("const")).isEqualTo(true);
    }

    @Test
    void wellKnownTimeTypesMapToStringForms() {
        assertThat(property("created").get("format")).isEqualTo("date-time");
        assertThat((String) property("ttl").get("pattern")).endsWith("s$");
    }

    @Test
    void selfReferenceUsesRefAndTerminates() {
        assertThat(property("parent").get("$ref")).isEqualTo("#/$defs/" + ACCOUNT);
    }

    @Test
    @SuppressWarnings("unchecked")
    void celRulesSurfaceAsVendorKeyword() {
        List<Map<String, Object>> fieldCel =
                (List<Map<String, Object>>) property("note").get("x-pipestream-cel");
        assertThat(fieldCel).hasSize(1);
        assertThat(fieldCel.get(0).get("id")).isEqualTo("note.short");
        assertThat(fieldCel.get(0).get("expression")).isEqualTo("this.size() < 100");

        List<Map<String, Object>> messageCel =
                (List<Map<String, Object>>) accountDef().get("x-pipestream-cel");
        assertThat(messageCel).hasSize(1);
        assertThat(messageCel.get(0).get("id")).isEqualTo("account.pro_needs_email");
    }

    @Test
    void generatedJsonIsValidJson() {
        String json = ProtoJsonSchemaGenerator.create().generateJson(Account.getDescriptor());
        assertThatCode(() -> JsonFormat.parser().merge(json, Struct.newBuilder()))
                .doesNotThrowAnyException();
        assertThat(json).contains("\"$schema\"");
    }
}
