package ai.protomolt.proto.http.jsonschema;

import ai.protomolt.proto.http.jsonschema.testdata.Account;
import ai.protomolt.proto.http.jsonschema.testdata.Tier;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ProtoJsonSchemaGeneratorTest {

    private static final String ACCOUNT = "ai.protomolt.proto.http.jsonschema.testdata.v1.Account";

    private final Map<String, Object> schema =
            ProtoJsonSchemaGenerator.create().generate(Account.getDescriptor());

    private final Map<String, Object> typeUniformSchema =
            ProtoJsonSchemaGenerator.createTypeUniform().generate(Account.getDescriptor());

    @SuppressWarnings("unchecked")
    private Map<String, Object> accountDef() {
        return accountDefOf(schema);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> accountDefOf(Map<String, Object> from) {
        Map<String, Object> defs = (Map<String, Object>) from.get("$defs");
        return (Map<String, Object>) defs.get(ACCOUNT);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> property(String name) {
        return propertyOf(schema, name);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> propertyOf(Map<String, Object> from, String name) {
        Map<String, Object> properties =
                (Map<String, Object>) accountDefOf(from).get("properties");
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
    void typeUniformInt64ConstraintsSplitBySpelling() {
        // Opt-in for strict tool-schema validators: both spellings stay accepted,
        // but every enum array is single-type.
        List<Map<String, Object>> constAnyOf =
                (List<Map<String, Object>>) propertyOf(typeUniformSchema, "exactVersion")
                        .get("anyOf");
        assertThat((List<Object>) constAnyOf.get(0).get("enum")).containsExactly(5L);
        assertThat((List<Object>) constAnyOf.get(1).get("enum")).containsExactly("5");

        List<Map<String, Object>> inAnyOf =
                (List<Map<String, Object>>) propertyOf(typeUniformSchema, "level").get("anyOf");
        assertThat((List<Object>) inAnyOf.get(0).get("enum")).containsExactly(1L, 2L, 3L);
        assertThat((List<Object>) inAnyOf.get(1).get("enum")).containsExactly("1", "2", "3");

        List<Map<String, Object>> allOf =
                (List<Map<String, Object>>) propertyOf(typeUniformSchema, "shard").get("allOf");
        Map<String, Object> notNumeric = (Map<String, Object>) allOf.get(0).get("not");
        Map<String, Object> notString = (Map<String, Object>) allOf.get(1).get("not");
        assertThat((List<Object>) notNumeric.get("enum")).containsExactly(4L);
        assertThat((List<Object>) notString.get("enum")).containsExactly("4");
    }

    @Test
    @SuppressWarnings("unchecked")
    void enumNotInRendersTypeUniformBranches() {
        // Opt-in mode: defined_only plus not_in zero renders the forbidden value once as a
        // name and once as a number, never mixed into one enum array. The branches nest
        // under allOf conjuncts, so search recursively.
        Map<String, Object> verifiedTier = propertyOf(typeUniformSchema, "verifiedTier");
        List<List<Object>> forbidden = new ArrayList<>();
        collectNotEnums(verifiedTier, forbidden);
        assertThat(forbidden).hasSize(2);
        assertThat(forbidden).anySatisfy(values ->
                assertThat(values).containsExactly("TIER_UNSPECIFIED"));
        assertThat(forbidden).anySatisfy(values ->
                assertThat(values).containsExactly(0L));
    }

    /** Collects the enum array of every {@code not} conjunct under {@code node}. */
    @SuppressWarnings("unchecked")
    private static void collectNotEnums(Object node, List<List<Object>> out) {
        if (node instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) node;
            Object not = map.get("not");
            if (not instanceof Map) {
                Object values = ((Map<String, Object>) not).get("enum");
                if (values instanceof List) {
                    out.add((List<Object>) values);
                }
            }
            map.values().forEach(v -> collectNotEnums(v, out));
        } else if (node instanceof List) {
            ((List<Object>) node).forEach(v -> collectNotEnums(v, out));
        }
    }

    @Test
    void noEnumArrayMixesJsonTypesInTypeUniformMode() {
        List<String> mixed = new ArrayList<>();
        findMixedEnums(typeUniformSchema, "$", mixed);
        assertThat(mixed).isEmpty();
    }

    /** Records the path of every enum array whose entries are not all one JSON type. */
    @SuppressWarnings("unchecked")
    private static void findMixedEnums(Object node, String path, List<String> mixed) {
        if (node instanceof Map) {
            ((Map<String, Object>) node).forEach((key, value) -> {
                if ("enum".equals(key) && value instanceof List) {
                    List<Object> values = (List<Object>) value;
                    long kinds = values.stream()
                            .map(v -> v instanceof String ? "string" : "number")
                            .distinct()
                            .count();
                    if (kinds > 1) {
                        mixed.add(path + ".enum");
                    }
                } else {
                    findMixedEnums(value, path + "." + key, mixed);
                }
            });
        } else if (node instanceof List) {
            int i = 0;
            for (Object value : (List<Object>) node) {
                findMixedEnums(value, path + "[" + i++ + "]", mixed);
            }
        }
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
    private static List<Error> validate(Account account) throws Exception {
        return validate(JsonFormat.printer().print(account),
                ProtoJsonSchemaGenerator.create());
    }

    /** Validates a raw JSON document against the schema from {@code generator}. */
    private static List<Error> validate(String json, ProtoJsonSchemaGenerator generator)
            throws Exception {
        String schemaJson = generator.generateJson(Account.getDescriptor());
        Schema schema = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(schemaJson);
        return schema.validate(json, InputFormat.JSON);
    }

    @Test
    void typeUniformSchemaAcceptsAndRejectsTheSameValues() throws Exception {
        ProtoJsonSchemaGenerator uniform = ProtoJsonSchemaGenerator.createTypeUniform();
        Account good = Account.newBuilder()
                .setUsername("user_1")
                .setAge(20)
                .setBalance(999_999L)
                .addRoles("admin")
                .setExactVersion(5L)
                .setLevel(2L)
                .setOffsetMs(-250L)
                .setVerifiedTier(Tier.TIER_PRO)
                .build();
        assertThat(validate(JsonFormat.printer().print(good), uniform)).isEmpty();

        // The forbidden enum value is rejected under both spellings.
        assertThat(validate("{\"username\":\"user_1\",\"verifiedTier\":\"TIER_UNSPECIFIED\"}",
                uniform)).isNotEmpty();
        assertThat(validate("{\"username\":\"user_1\",\"verifiedTier\":0}",
                uniform)).isNotEmpty();
        // The forbidden int64 value is rejected under both spellings.
        assertThat(validate("{\"username\":\"user_1\",\"shard\":4}",
                uniform)).isNotEmpty();
        assertThat(validate("{\"username\":\"user_1\",\"shard\":\"4\"}",
                uniform)).isNotEmpty();
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
                (List<Map<String, Object>>) property("note").get("x-protomolt-cel");
        assertThat(fieldCel).hasSize(1);
        assertThat(fieldCel.get(0).get("id")).isEqualTo("note.short");
        assertThat(fieldCel.get(0).get("expression")).isEqualTo("this.size() < 100");

        List<Map<String, Object>> messageCel =
                (List<Map<String, Object>>) accountDef().get("x-protomolt-cel");
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
