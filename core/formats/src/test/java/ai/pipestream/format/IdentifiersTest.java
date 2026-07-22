package ai.pipestream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Identifiers}: RFC 4122 textual UUID (purely syntactic — no version/variant checks),
 * trimmed UUID, Crockford base-32 ULID (first char 0-7 so the value fits 128 bits), and
 * protobuf fully-qualified names in both relative and absolute (leading-dot) forms.
 */
class IdentifiersTest {

    // ---------------------------------------------------------------- UUID

    @ParameterizedTest
    @ValueSource(strings = {
            "123e4567-e89b-12d3-a456-426614174000",
            "00000000-0000-0000-0000-000000000000",   // nil UUID: no version/variant check
            "ffffffff-ffff-ffff-ffff-ffffffffffff",   // max UUID
            "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",   // uppercase hex
            "123E4567-e89b-12D3-a456-426614174000",   // mixed case
            "a987fbc9-4bed-3078-cf07-9141ba07c9f3",
            "01890a5d-ac96-774b-bcce-b302099a8057",   // v7-shaped
    })
    void uuidAccepts(String value) {
        assertThat(Identifiers.isUuid(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "123e4567-e89b-12d3-a456-42661417400",    // 35 chars
            "123e4567-e89b-12d3-a456-4266141740000",  // 37 chars
            "123e4567e89b12d3a456426614174000",       // dashes required
            "123e4567-e89b-12d3-a456_426614174000",   // wrong separator
            "123e4567+e89b+12d3+a456+426614174000",
            "123e-4567e89b-12d3-a456-426614174000",   // dash in the wrong position
            "123e45677e89b-12d3-a456-42661417400-",   // dash misplaced to the end
            "g23e4567-e89b-12d3-a456-426614174000",   // non-hex char
            "123e4567-e89b-12d3-a456-42661417400z",
            "{123e4567-e89b-12d3-a456-426614174000}", // Microsoft brace form
            "urn:uuid:123e4567-e89b-12d3-a456-426614174000",
            "123e4567-e89b-12d3-a456-426614174000 ",
            " 123e4567-e89b-12d3-a456-426614174000",
            "123e4567-e89b-12d3-a456-42661417 000",
            "------------------------------------",
    })
    void uuidRejects(String value) {
        assertThat(Identifiers.isUuid(value)).isFalse();
    }

    // ---------------------------------------------------------------- trimmed UUID

    @ParameterizedTest
    @ValueSource(strings = {
            "123e4567e89b12d3a456426614174000",
            "00000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffff",
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
            "AbCdEf0123456789aBcDeF0123456789",
    })
    void tuuidAccepts(String value) {
        assertThat(Identifiers.isTuuid(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "123e4567e89b12d3a45642661417400",        // 31 chars
            "123e4567e89b12d3a4564266141740000",      // 33 chars
            "123e4567-e89b-12d3-a456-426614174000",   // dashed form is not trimmed
            "g23e4567e89b12d3a456426614174000",
            "123e4567e89b12d3a45642661417400z",
            "123e4567e89b12d3a45642661417400 ",
    })
    void tuuidRejects(String value) {
        assertThat(Identifiers.isTuuid(value)).isFalse();
    }

    // ---------------------------------------------------------------- ULID

    @ParameterizedTest
    @ValueSource(strings = {
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "00000000000000000000000000",             // minimum
            "7ZZZZZZZZZZZZZZZZZZZZZZZZZ",             // maximum 128-bit value
            "01arz3ndektsv4rrffq69g5fav",             // Crockford decode is case-insensitive
            "01aRz3NdEkTsV4rRfFq69G5fAv",
            "01BX5ZZKBKACTAV9WEVGEMMVRZ",
            "7zzzzzzzzzzzzzzzzzzzzzzzzz",
    })
    void ulidAccepts(String value) {
        assertThat(Identifiers.isUlid(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "01ARZ3NDEKTSV4RRFFQ69G5FA",              // 25 chars
            "01ARZ3NDEKTSV4RRFFQ69G5FAVX",            // 27 chars
            "8ZZZZZZZZZZZZZZZZZZZZZZZZZ",             // first char 8 overflows 128 bits
            "9ZZZZZZZZZZZZZZZZZZZZZZZZZ",
            "AZZZZZZZZZZZZZZZZZZZZZZZZZ",             // first char must be a digit 0-7
            "aZZZZZZZZZZZZZZZZZZZZZZZZZ",
            "01ARZ3NDEKTSV4RRFFQ69G5FAI",              // 'I' excluded from Crockford base-32
            "01ARZ3NDEKTSV4RRFFQ69G5FAL",              // 'L' excluded
            "01ARZ3NDEKTSV4RRFFQ69G5FAO",              // 'O' excluded
            "01ARZ3NDEKTSV4RRFFQ69G5FAU",              // 'U' excluded
            "01arz3ndektsv4rrffq69g5fai",              // exclusions are case-insensitive too
            "01arz3ndektsv4rrffq69g5fal",
            "01arz3ndektsv4rrffq69g5fao",
            "01arz3ndektsv4rrffq69g5fau",
            "01ARZ3NDEKTSV4RRFFQ69G5FA-",
            "01ARZ3NDEKTSV4RRFFQ69G5FA ",
            "01ARZ3-NDEKTSV4RRFFQ69G5FA",
    })
    void ulidRejects(String value) {
        assertThat(Identifiers.isUlid(value)).isFalse();
    }

    // ---------------------------------------------------------------- protobuf FQN

    @ParameterizedTest
    @ValueSource(strings = {
            "foo",
            "foo.bar",
            "foo.bar.Baz",
            "google.protobuf.Any",
            "google.protobuf.FileDescriptorProto",
            "_",
            "__",
            "_leading._underscore",
            "a1.b2.c3",
            "F_o0.bar_1.Msg2",
            "UPPER.CASE.NAME",
            "a",
            "a.b.c.d.e.f.g.h.i.j",
    })
    void fqnAccepts(String value) {
        assertThat(Identifiers.isProtobufFqn(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            ".",
            "..",
            ".foo",                // absolute form is the other method's job
            "foo.",                // trailing dot
            "foo..bar",            // empty segment
            "1foo",                // segment may not start with a digit
            "foo.1bar",
            "foo.bar.9",
            "foo-bar",             // hyphens are not identifier chars
            "foo.b-r",
            "foo bar",
            "foo. bar",
            "foo.bar ",
            "foo/bar",
            "foo$bar",             // '$' (JVM inner-class syntax) is not protobuf syntax
            "fo€o",
            "foo.bär",
            "foo\tbar",
    })
    void fqnRejects(String value) {
        assertThat(Identifiers.isProtobufFqn(value)).isFalse();
    }

    // ---------------------------------------------------------------- absolute (leading-dot) FQN

    @ParameterizedTest
    @ValueSource(strings = {
            ".foo",
            ".foo.bar",
            ".foo.bar.Baz",
            ".google.protobuf.Any",
            "._",
            "._x._y",
            ".a1.b2",
    })
    void dotFqnAccepts(String value) {
        assertThat(Identifiers.isProtobufDotFqn(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            ".",
            "..",
            "foo",                 // must begin with a dot
            "foo.bar.Baz",
            "..foo",               // empty first segment
            ".foo.",               // trailing dot
            ".foo..bar",
            ".1foo",
            ".foo.1bar",
            ". foo",
            ".foo-bar",
    })
    void dotFqnRejects(String value) {
        assertThat(Identifiers.isProtobufDotFqn(value)).isFalse();
    }

    @Test
    void uuidDashPositionsAreExact() {
        // Dashes must sit at offsets 8, 13, 18 and 23 — shifting any one of them breaks it.
        String canonical = "123e4567-e89b-12d3-a456-426614174000";
        assertThat(Identifiers.isUuid(canonical)).isTrue();
        for (int shifted : new int[] {7, 9, 12, 14, 17, 19, 22, 24}) {
            StringBuilder sb = new StringBuilder(canonical.replace("-", "0"));
            sb.setCharAt(shifted, '-');
            assertThat(Identifiers.isUuid(sb.toString()))
                    .as("dash only at offset %d", shifted)
                    .isFalse();
        }
    }
}
