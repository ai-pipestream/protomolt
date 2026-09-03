package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.BodyType;
import ai.protomolt.proto.acquire.confluence.v1.ChangeOperation;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.protomolt.proto.acquire.confluence.v1.ContentProperty;
import ai.protomolt.proto.acquire.confluence.v1.Page;
import ai.protomolt.proto.acquire.confluence.v1.PropertyKey;
import ai.protomolt.proto.acquire.confluence.v1.Redaction;
import ai.protomolt.proto.acquire.confluence.v1.User;
import ai.protomolt.proto.acquire.confluence.v1.Version;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the validate.v1 annotations on the Confluence domain model are live:
 * one assertion per rule family, both the violating and the passing shape.
 * These are acquisition-side invariants — identity fields that make an entity
 * addressable, spec-backed numeric floors, and the cross-field facts the
 * OpenAPI spec cannot express (redaction ranges, custom property keys,
 * upsert payloads).
 */
class ConfluenceValidationTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected no violations")
                .isEmpty();
    }

    @Test
    void pageIdentityIsRequired() {
        assertViolation(Page.getDefaultInstance(), "id", "required");
        assertViolation(Page.getDefaultInstance(), "space_id", "required");
        assertValid(Page.newBuilder().setId("123").setSpaceId("456").build());
    }

    @Test
    void versionNumbersAreNeverNegative() {
        // Plain int32 renders absence as 0, so 0 must pass; a negative number
        // is a lie, not an absence.
        assertValid(Version.getDefaultInstance());
        assertViolation(Version.newBuilder().setNumber(-1).build(), "number", "int32.gte");
        assertValid(Version.newBuilder().setNumber(1).build());
    }

    @Test
    void userEmailMustBeAnEmailWhenPresent() {
        User valid = User.newBuilder().setAccountId("abc").build();
        assertValid(valid);
        assertValid(valid.toBuilder().setEmail("kim@example.com").build());
        assertViolation(valid.toBuilder().setEmail("not-an-email").build(),
                "email", "user.email_format");
        assertViolation(User.getDefaultInstance(), "account_id", "required");
    }

    @Test
    void redactionRangeAndPointerRules() {
        Redaction valid = Redaction.newBuilder()
                .setPointer("/body/storage")
                .setFrom(3).setTo(9)
                .setRedactionId("6cd7e1d0-3f7b-4a86-9b0f-6f34e0a2b8d1")
                .build();
        assertValid(valid);
        assertViolation(Redaction.getDefaultInstance(), "pointer", "required");
        assertViolation(valid.toBuilder().setFrom(10).setTo(4).build(),
                "", "redaction.range");
        assertViolation(valid.toBuilder().setRedactionId("not-a-uuid").build(),
                "redaction_id", "redaction.id_uuid");
        // Request shapes carry no redaction_id yet; absence is not a violation.
        assertValid(valid.toBuilder().clearRedactionId().build());
    }

    @Test
    void customPropertyKeyIsExplicitBothWays() {
        ContentProperty custom = ContentProperty.newBuilder()
                .setId("p1")
                .setKey(PropertyKey.PROPERTY_KEY_CUSTOM)
                .setCustomKey("team.owner")
                .build();
        assertValid(custom);
        // CUSTOM without a custom_key names nothing.
        assertViolation(custom.toBuilder().clearCustomKey().build(),
                "", "property.custom_key");
        // A well-known key with a custom_key is two contradictory claims.
        assertViolation(ContentProperty.newBuilder()
                        .setId("p2")
                        .setKey(PropertyKey.PROPERTY_KEY_EDITOR)
                        .setCustomKey("stray")
                        .build(),
                "", "property.custom_key");
    }

    @Test
    void populatedBodyMustDeclareItsFormat() {
        assertValid(BodyType.getDefaultInstance());
        assertViolation(BodyType.newBuilder().setValue("<p>hi</p>").build(),
                "", "body_type.format_declared");
        assertValid(BodyType.newBuilder()
                .setFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                .setValue("<p>hi</p>")
                .build());
    }

    @Test
    void upsertChangesMustCarryTheirEntity() {
        ConfluenceChange delete = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .build();
        assertValid(delete);
        assertViolation(delete.toBuilder()
                        .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                        .build(),
                "", "change.upsert_has_entity");
        assertValid(ConfluenceChange.newBuilder()
                .setChangeId("c2")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("page:123")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1_753_000_000)))
                .build());
    }
}
