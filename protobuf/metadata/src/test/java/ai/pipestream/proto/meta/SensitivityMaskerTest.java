package ai.pipestream.proto.meta;

import ai.pipestream.proto.meta.testdata.MaskerDoc;
import ai.pipestream.proto.meta.testdata.SecretNote;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The masker's traversal must reach sensitive data wherever the schema can put it — in
 * particular inside protobuf maps, both when the map field itself is classed and when a
 * message-valued map entry carries classed fields — and encrypted values must be bound to
 * the field they were sealed under.
 */
class SensitivityMaskerTest {

    private static final byte[] KEY =
            "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);

    private static MaskerDoc doc() {
        return MaskerDoc.newBuilder()
                .setName("visible")
                .setEmail("pat@example.com")
                .setToken(ByteString.copyFromUtf8("s3cr3t"))
                .putPlainAttrs("color", "blue")
                .putSecretAttrs("ssn", "123-45-6789")
                .putSecretAttrs("phone", "555-0100")
                .putNotes("a", SecretNote.newBuilder().setBody("hidden").setLabel("keep").build())
                .putChildren("kid", MaskerDoc.newBuilder()
                        .setEmail("child@example.com")
                        .putSecretAttrs("dob", "2001-01-01")
                        .build())
                .addEntries(SecretNote.newBuilder().setBody("listed").setLabel("open").build())
                .putCounts("logins", 42)
                .build();
    }

    @Test
    void redactReachesEveryMapPosition() {
        var result = SensitivityMasker.mask(doc(), Set.of("pii"),
                SensitivityMasker.Strategy.REDACT);
        MaskerDoc masked = (MaskerDoc) result.message();

        // Untouched: unclassed scalar, unclassed map, other-class bytes.
        assertThat(masked.getName()).isEqualTo("visible");
        assertThat(masked.getPlainAttrsMap()).containsEntry("color", "blue");
        assertThat(masked.getToken().toStringUtf8()).isEqualTo("s3cr3t");

        // Classed scalar.
        assertThat(masked.getEmail()).isEqualTo("***");
        // Classed map field: every entry's value redacts, keys stay.
        assertThat(masked.getSecretAttrsMap())
                .containsEntry("ssn", "***")
                .containsEntry("phone", "***");
        // Message-valued map entry: the classed nested field redacts, the rest stays.
        assertThat(masked.getNotesMap().get("a").getBody()).isEqualTo("***");
        assertThat(masked.getNotesMap().get("a").getLabel()).isEqualTo("keep");
        // A map of messages that themselves contain maps: full recursion.
        assertThat(masked.getChildrenMap().get("kid").getEmail()).isEqualTo("***");
        assertThat(masked.getChildrenMap().get("kid").getSecretAttrsMap())
                .containsEntry("dob", "***");
        // Repeated message.
        assertThat(masked.getEntries(0).getBody()).isEqualTo("***");
        assertThat(masked.getEntries(0).getLabel()).isEqualTo("open");
        // Classed map with non-string values: cleared, a redacted number would still lie.
        assertThat(masked.getCountsMap()).isEmpty();

        assertThat(result.maskedPaths()).contains(
                "email", "secret_attrs", "counts",
                "notes[a].body", "children[kid].email", "children[kid].secret_attrs",
                "entries.body");
    }

    @Test
    void removeClearsClassedFieldsIncludingMaps() {
        MaskerDoc masked = (MaskerDoc) SensitivityMasker.mask(doc(), Set.of("pii"),
                SensitivityMasker.Strategy.REMOVE).message();
        assertThat(masked.getEmail()).isEmpty();
        assertThat(masked.getSecretAttrsMap()).isEmpty();
        assertThat(masked.getCountsMap()).isEmpty();
        assertThat(masked.getNotesMap().get("a").getBody()).isEmpty();
        assertThat(masked.getNotesMap().get("a").getLabel()).isEqualTo("keep");
        assertThat(masked.getChildrenMap().get("kid").getSecretAttrsMap()).isEmpty();
    }

    @Test
    void encryptRoundTripsEverywhereIncludingMapValues() {
        MaskerDoc sealed = (MaskerDoc) SensitivityMasker.mask(doc(), Set.of("pii", "secret"),
                SensitivityMasker.Strategy.ENCRYPT, KEY).message();
        assertThat(sealed.getEmail()).isNotEqualTo("pat@example.com");
        assertThat(sealed.getSecretAttrsMap().get("ssn")).isNotEqualTo("123-45-6789");
        assertThat(sealed.getNotesMap().get("a").getBody()).isNotEqualTo("hidden");
        assertThat(sealed.getToken().toStringUtf8()).isNotEqualTo("s3cr3t");

        // The classed int64 map cannot hold ciphertext, so ENCRYPT clears it for good.
        assertThat(sealed.getCountsMap()).isEmpty();

        MaskerDoc opened = (MaskerDoc) SensitivityMasker.mask(sealed, Set.of("pii", "secret"),
                SensitivityMasker.Strategy.DECRYPT, KEY).message();
        assertThat(opened).isEqualTo(doc().toBuilder().clearCounts().build());
    }

    @Test
    void ciphertextIsBoundToItsField() {
        MaskerDoc sealed = (MaskerDoc) SensitivityMasker.mask(doc(), Set.of("pii"),
                SensitivityMasker.Strategy.ENCRYPT, KEY).message();
        // Move email's ciphertext into a note body (same class, same string type).
        MaskerDoc swapped = sealed.toBuilder()
                .putNotes("a", sealed.getNotesOrThrow("a").toBuilder()
                        .setBody(sealed.getEmail())
                        .build())
                .build();
        assertThatThrownBy(() -> SensitivityMasker.mask(swapped, Set.of("pii"),
                SensitivityMasker.Strategy.DECRYPT, KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wrong key, wrong field, or tampered");
    }

    @Test
    void wrongKeyAndMalformedEnvelopeFailLoudly() {
        MaskerDoc sealed = (MaskerDoc) SensitivityMasker.mask(doc(), Set.of("pii"),
                SensitivityMasker.Strategy.ENCRYPT, KEY).message();
        byte[] otherKey = "fedcba9876543210fedcba9876543210".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> SensitivityMasker.mask(sealed, Set.of("pii"),
                SensitivityMasker.Strategy.DECRYPT, otherKey))
                .isInstanceOf(IllegalArgumentException.class);

        // A value that was never encrypted (no envelope) is refused, not garbled.
        MaskerDoc unencrypted = doc().toBuilder()
                .setEmail(Base64.getEncoder().encodeToString("just text".getBytes(
                        StandardCharsets.UTF_8)))
                .build();
        assertThatThrownBy(() -> SensitivityMasker.mask(unencrypted, Set.of("pii"),
                SensitivityMasker.Strategy.DECRYPT, KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("envelope");
    }

    @Test
    void versionByteLeadsTheEnvelope() {
        MaskerDoc sealed = (MaskerDoc) SensitivityMasker.mask(doc(), Set.of("secret"),
                SensitivityMasker.Strategy.ENCRYPT, KEY).message();
        assertThat(sealed.getToken().byteAt(0)).isEqualTo((byte) 1);
    }
}
