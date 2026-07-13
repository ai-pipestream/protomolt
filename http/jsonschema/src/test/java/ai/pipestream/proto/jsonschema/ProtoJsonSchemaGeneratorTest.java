package ai.pipestream.proto.jsonschema;

import ai.pipestream.proto.jsonschema.testdata.Account;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
    @SuppressWarnings("unchecked")
    void uint64AcceptsIntegerOrStringWithUnsignedBounds() {
        Map<String, Object> balance = property("balance");
        assertThat((List<Object>) balance.get("type")).containsExactly("integer", "string");
        assertThat(balance.get("pattern")).isEqualTo("^[0-9]+$");
        assertThat(balance.get("minimum")).isEqualTo(0L);
        // range bounds cover both accepted spellings: numeric keywords and a string pattern
        List<Map<String, Object>> anyOf = (List<Map<String, Object>>) balance.get("anyOf");
        assertThat(anyOf).hasSize(2);
        assertThat(anyOf.get(0))
                .containsEntry("type", "integer")
                .containsEntry("maximum", 1000000L);
        assertThat(anyOf.get(1)).containsEntry("type", "string");
        Pattern stringForm = Pattern.compile((String) anyOf.get(1).get("pattern"));
        assertThat(stringForm.matcher("1000000").matches()).isTrue();
        assertThat(stringForm.matcher("0").matches()).isTrue();
        assertThat(stringForm.matcher("1000001").matches()).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void int64ConstAndEnumConstraintsAcceptBothSpellings() {
        // JsonFormat prints int64 as a JSON string; const/in/not_in must match both forms.
        assertThat((List<Object>) property("exactVersion").get("enum"))
                .containsExactly(5L, "5");
        assertThat((List<Object>) property("level").get("enum"))
                .containsExactly(1L, "1", 2L, "2", 3L, "3");
        Map<String, Object> not = (Map<String, Object>) property("shard").get("not");
        assertThat((List<Object>) not.get("enum")).containsExactly(4L, "4");
    }

    @Test
    @SuppressWarnings("unchecked")
    void int64RangeConstraintsApplyToTheStringSpelling() {
        Map<String, Object> offset = property("offsetMs");
        List<Map<String, Object>> anyOf = (List<Map<String, Object>>) offset.get("anyOf");
        assertThat(anyOf.get(0))
                .containsEntry("type", "integer")
                .containsEntry("minimum", -500L)
                .containsEntry("maximum", 500L);
        Pattern stringForm = Pattern.compile((String) anyOf.get(1).get("pattern"));
        assertThat(stringForm.matcher("500").matches()).isTrue();
        assertThat(stringForm.matcher("-500").matches()).isTrue();
        assertThat(stringForm.matcher("0").matches()).isTrue();
        assertThat(stringForm.matcher("42").matches()).isTrue();
        assertThat(stringForm.matcher("501").matches()).isFalse();
        assertThat(stringForm.matcher("-501").matches()).isFalse();
        assertThat(stringForm.matcher("5000").matches()).isFalse();
    }

    @Test
    void jsonFormatPrintedDocumentValidates() throws Exception {
        Account account = Account.newBuilder()
                .setUsername("user_1")
                .setAge(20)
                .setBalance(999_999L)
                .addRoles("admin")
                .setExactVersion(5L)
                .setLevel(2L)
                .setOffsetMs(-250L)
                .build();
        assertThat(validate(account)).isEmpty();
    }

    @Test
    void canonicalStringSpellingOutOfConstraintFailsValidation() throws Exception {
        Account overBalance = Account.newBuilder()
                .setUsername("user_1")
                .setBalance(1_000_001L)
                .build();
        assertThat(validate(overBalance)).isNotEmpty();

        Account wrongConst = Account.newBuilder()
                .setUsername("user_1")
                .setExactVersion(6L)
                .build();
        assertThat(validate(wrongConst)).isNotEmpty();

        Account outOfRange = Account.newBuilder()
                .setUsername("user_1")
                .setOffsetMs(501L)
                .build();
        assertThat(validate(outOfRange)).isNotEmpty();

        Account forbidden = Account.newBuilder()
                .setUsername("user_1")
                .setShard(4L)
                .build();
        assertThat(validate(forbidden)).isNotEmpty();
    }

    /** Validates the canonical JsonFormat printing of {@code account} against the schema. */
    private static Set<ValidationMessage> validate(Account account) throws Exception {
        ObjectMapper json = new ObjectMapper();
        String schemaJson = ProtoJsonSchemaGenerator.create().generateJson(Account.getDescriptor());
        JsonSchema jsonSchema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
                .getSchema(json.readTree(schemaJson));
        String document = JsonFormat.printer().print(account);
        return jsonSchema.validate(json.readTree(document));
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
