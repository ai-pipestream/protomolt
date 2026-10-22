package ai.protomolt.proto.shapes;

import java.util.List;
import java.util.Locale;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ShapeTextTest {
    @Test void pathsAndOperatorsKeepEmptySegmentsAndLimitSemantics() {
        for (String text : List.of("", ".", "..", ".a", "a..b.", "a.b", "a=b=c", "=", "+=", "x+=y=z")) {
            assertThat(ShapeText.splitPath(text)).containsExactly(text.split("\\."));
            assertThat(ShapeText.sides(text, false)).containsExactly(text.split("=", 2));
            assertThat(ShapeText.sides(text, true)).containsExactly(text.split("\\+=", 2));
        }
    }

    @Test void camelCaseConversionPreservesAcronymsDigitsAndExistingUnderscores() {
        for (String name : List.of("MyURLValue", "ABC", "aBCdE", "a1B2C", "a_B", "_Foo", "xYz", "x😀Y")) {
            assertThat(ShapeText.snakeCase(name))
                    .isEqualTo(name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT));
        }
    }

    @Test void synthesizedFileNameUsesTheSameCamelCaseConversion() {
        for (String name : List.of("MyURLValue", "ABC", "aBCdE", "a1B2C", "a_B", "_Foo")) {
            var shape = new ShapeSynthesizer().envelope("test." + name,
                    List.of(new ShapeSynthesizer.NamedType("source", Struct.getDescriptor())));
            assertThat(shape.type().getFile().getName()).isEqualTo("test/"
                    + name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT) + ".proto");
        }
    }

    @Test void inferredFieldNamesFoldSupplementaryCharactersOnce() {
        for (String key : List.of("x😀y", "x\ud83dy", "x\ude00y", "9😀", "")) {
            var input = Struct.newBuilder().putFields(key, Value.newBuilder().setStringValue("v").build()).build();
            var type = new SchemaInferrer().infer("test.Sample", List.of(input)).type();
            String expected = key.replaceAll("[^A-Za-z0-9_]", "_");
            if (expected.isEmpty() || Character.isDigit(expected.charAt(0))) expected = "_" + expected;
            assertThat(type.getFields().getFirst().getName()).isEqualTo(expected.toLowerCase(Locale.ROOT));
            assertThat(type.getFields().getFirst().getJsonName()).isEqualTo(key);
        }
    }

    @Test void numericLiteralsKeepAsciiDecimalGrammar() {
        for (String s : List.of("", "-", "0", "-12", "12.3", "1.", ".1", "+1", "1e3", "١", "12\n", "--1"))
            assertThat(Literals.isLiteral(s)).as(s).isEqualTo(s.matches("-?\\d+(\\.\\d+)?"));
    }
}
