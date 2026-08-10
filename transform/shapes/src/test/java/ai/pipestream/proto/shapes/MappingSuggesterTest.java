package ai.pipestream.proto.shapes;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Structural mapping suggestions: name-matched candidates identify their source and target
 * fields, respect type and shape agreement, rank exact spellings first, and every emitted
 * rule passes the same {@link RuleChecker} chains are verified with.
 */
class MappingSuggesterTest {

    private static final String PROTO = """
            syntax = "proto3";
            package suggest.test;
            message Order {
              string id = 1;
              int64 qty = 2;
              repeated string tags = 3;
              Address ship_to = 4;
              map<string, string> attrs = 5;
              int32 priority = 6;
            }
            message Address {
              string city = 1;
              string zip = 2;
            }
            message Customer {
              string id = 1;
              string display_name = 2;
              Address home = 3;
            }
            message Ticket {
              string displayName = 2;
              repeated string tags = 3;
              int64 qty = 4;
              string city = 5;
              Address ship_to = 6;
              string attrs = 7;
              int64 priority = 8;
              bool priority_flag = 9;
            }
            """;

    private static Descriptor order;
    private static Descriptor customer;
    private static Descriptor ticket;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("suggest/test/suggest.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("suggest/test/suggest.proto").orElseThrow();
        order = file.findMessageTypeByName("Order");
        customer = file.findMessageTypeByName("Customer");
        ticket = file.findMessageTypeByName("Ticket");
    }

    private static Map<String, Descriptor> sources() {
        return Map.of("order", order, "customer", customer);
    }

    private static List<MappingSuggester.Candidate> forField(
            MappingSuggester.Suggestions suggestions, String field) {
        return suggestions.candidates().stream()
                .filter(candidate -> candidate.targetPath().equals(field))
                .toList();
    }

    @Test
    void normalizedNamesMeetAcrossSpellings() {
        MappingSuggester.Suggestions suggestions =
                MappingSuggester.suggest(sources(), ticket);

        // snake_case on the source, camelCase on the target: one normalized candidate.
        assertThat(forField(suggestions, "displayName"))
                .extracting(MappingSuggester.Candidate::rule)
                .containsExactly("displayName=customer.display_name");
        assertThat(forField(suggestions, "displayName").get(0).basis())
                .isEqualTo("normalized name");
    }

    @Test
    void nestedAndMessageTypedCandidatesAreFound() {
        MappingSuggester.Suggestions suggestions =
                MappingSuggester.suggest(sources(), ticket);

        // One level down: Address.city through both holders, alphabetically by source path.
        assertThat(forField(suggestions, "city"))
                .extracting(MappingSuggester.Candidate::sourcePath)
                .containsExactly("customer.home.city", "order.ship_to.city");
        // A message field maps only to the same message type.
        assertThat(forField(suggestions, "ship_to"))
                .extracting(MappingSuggester.Candidate::rule)
                .containsExactly("ship_to=order.ship_to");
    }

    @Test
    void typeAndShapeMismatchesAreNeverSuggested() {
        MappingSuggester.Suggestions suggestions =
                MappingSuggester.suggest(sources(), ticket);

        // attrs is a map on the source and a string on the target: no candidate.
        assertThat(forField(suggestions, "attrs")).isEmpty();
        // priority is int32 on the source, int64 on the target: no coercion guesses.
        assertThat(forField(suggestions, "priority")).isEmpty();
        // No bool anywhere near that name: still empty.
        assertThat(forField(suggestions, "priority_flag")).isEmpty();
        // tags is repeated on both sides: suggested.
        assertThat(forField(suggestions, "tags"))
                .extracting(MappingSuggester.Candidate::rule)
                .containsExactly("tags=order.tags");
        // qty matches exactly (int64 to int64).
        assertThat(forField(suggestions, "qty"))
                .extracting(MappingSuggester.Candidate::rule)
                .containsExactly("qty=order.qty");
    }

    @Test
    void ambiguousFieldsOfferEverySource() {
        MappingSuggester.Suggestions suggestions =
                MappingSuggester.suggest(sources(), ticket);

        // id lives on both sources; both are offered, neither is picked for the caller.
        MappingSuggester.Suggestions forId = MappingSuggester.suggest(sources(), order);
        assertThat(forField(forId, "id"))
                .extracting(MappingSuggester.Candidate::sourcePath)
                .containsExactlyInAnyOrder("order.id", "customer.id");
    }

    @Test
    void exactSpellingRanksBeforeNormalizedAndNested() {
        MappingSuggester.Suggestions suggestions =
                MappingSuggester.suggest(sources(), order);

        List<MappingSuggester.Candidate> id = forField(suggestions, "id");
        assertThat(id).isNotEmpty();
        assertThat(id.get(0).basis()).isEqualTo("exact name");
    }

    @Test
    void everySuggestedRulePassesTheChainTypeChecker() {
        MappingSuggester.Suggestions suggestions =
                MappingSuggester.suggest(sources(), ticket);

        assertThat(suggestions.rules()).isNotEmpty();
        assertThat(new RuleChecker().checkScoped(sources(), ticket,
                suggestions.rules(), List.of(), List.of())).isEmpty();
    }
}
