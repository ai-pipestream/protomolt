package ai.pipestream.proto.asset.characterize;

import ai.pipestream.proto.asset.v1.FormatFact;
import ai.pipestream.proto.validate.FieldRules;
import ai.pipestream.proto.validate.ValidateProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The grammar parity pin: {@link FormatGrammars}' compiled patterns are
 * byte-for-byte the expressions the contract declares on each format
 * message's {@code filename} field. Read from the ACTUAL descriptor
 * annotations, so the identifier and the validator cannot drift apart —
 * a grammar edit in one place without the other fails here.
 */
class FormatGrammarParityTest {

    private static final Map<FormatFact.FormatCase, Pattern> GRAMMARS = Map.ofEntries(
            Map.entry(FormatFact.FormatCase.TAR, FormatGrammars.TAR),
            Map.entry(FormatFact.FormatCase.ZIP, FormatGrammars.ZIP),
            Map.entry(FormatFact.FormatCase.GZIP, FormatGrammars.GZIP),
            Map.entry(FormatFact.FormatCase.PARQUET, FormatGrammars.PARQUET),
            Map.entry(FormatFact.FormatCase.AVRO, FormatGrammars.AVRO),
            Map.entry(FormatFact.FormatCase.DELIMITED, FormatGrammars.DELIMITED),
            Map.entry(FormatFact.FormatCase.NDJSON, FormatGrammars.NDJSON),
            Map.entry(FormatFact.FormatCase.JSON, FormatGrammars.JSON),
            Map.entry(FormatFact.FormatCase.XML, FormatGrammars.XML),
            Map.entry(FormatFact.FormatCase.YAML, FormatGrammars.YAML),
            Map.entry(FormatFact.FormatCase.PDF, FormatGrammars.PDF),
            Map.entry(FormatFact.FormatCase.WORD, FormatGrammars.WORD),
            Map.entry(FormatFact.FormatCase.SPREADSHEET, FormatGrammars.SPREADSHEET),
            Map.entry(FormatFact.FormatCase.PRESENTATION, FormatGrammars.PRESENTATION),
            Map.entry(FormatFact.FormatCase.MARKDOWN, FormatGrammars.MARKDOWN),
            Map.entry(FormatFact.FormatCase.HTML, FormatGrammars.HTML),
            Map.entry(FormatFact.FormatCase.TEXT, FormatGrammars.TEXT),
            Map.entry(FormatFact.FormatCase.IMAGE, FormatGrammars.IMAGE));

    @Test
    void everyFormatArmHasAGrammarMatchingItsContractPattern() {
        Descriptor union = FormatFact.getDescriptor();
        for (FieldDescriptor arm : union.getOneofs().get(0).getFields()) {
            FormatFact.FormatCase formatCase =
                    FormatFact.FormatCase.forNumber(arm.getNumber());
            Pattern grammar = GRAMMARS.get(formatCase);
            assertThat(grammar).as("no compiled grammar for arm " + arm.getName()).isNotNull();

            FieldDescriptor filename = arm.getMessageType().findFieldByName("filename");
            assertThat(filename).as(arm.getName() + " has no filename field").isNotNull();
            FieldRules rules = filename.getOptions().getExtension(ValidateProto.field);
            assertThat(rules.getString().getPattern())
                    .as(arm.getName() + "'s contract pattern vs the compiled grammar")
                    .isEqualTo(grammar.pattern());
        }
        assertThat(union.getOneofs().get(0).getFields())
                .as("every arm accounted for")
                .hasSize(GRAMMARS.size());
    }
}
