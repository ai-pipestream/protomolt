package ai.protomolt.proto.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ai.protomolt.proto.config.ConceptSchemeMounts.Expansion;
import ai.protomolt.proto.config.ConceptSchemeMounts.UnresolvedMapping;
import ai.protomolt.proto.types.Concept;
import ai.protomolt.proto.types.ConceptExpansion;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.types.ConceptsProto;
import ai.protomolt.proto.types.LangLabel;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidateProto;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.spi.ConceptSchemeCatalog;
import ai.protomolt.proto.validate.spi.PostalCodeCatalog;
import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The concept-scheme mount contract: the subject is the identity and the source's version
 * the version; a document mounts only after its declared rules and the whole-scheme graph
 * checks pass, and a refused document leaves the previous version serving; a validator
 * built over the mounts changes verdicts on the next validation after a swap; and query
 * expansion over the mounted schemes is deterministic, bounded, and reports the mappings
 * it could not follow.
 */
class ConceptSchemeMountsTest {

    /** A source over a map, as the consumer proof uses. */
    static final class FakeSource implements ConfigSource {
        final Map<String, Fetched> documents = new HashMap<>();

        @Override
        public Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }

        void put(String schemeId, String version, ConceptScheme scheme) {
            documents.put(ConceptSchemeMounts.SUBJECT_PREFIX + schemeId,
                    new Fetched(version, scheme.toByteArray()));
        }
    }

    private static final ConceptExpansion DEFAULT = ConceptExpansion.getDefaultInstance();
    private static final ConceptExpansion NARROWER =
            ConceptExpansion.newBuilder().setNarrower(true).build();
    private static final ConceptExpansion CLOSE =
            ConceptExpansion.newBuilder().setCloseMatch(true).build();

    private static ConceptRef ref(String schemeId, String conceptId) {
        return ConceptRef.newBuilder().setSchemeId(schemeId).setConceptId(conceptId).build();
    }

    private static Concept.Builder concept(String id) {
        return Concept.newBuilder().setId(id)
                .addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText(id));
    }

    private static ConceptScheme scheme(Concept.Builder... concepts) {
        ConceptScheme.Builder builder = ConceptScheme.newBuilder();
        for (Concept.Builder concept : concepts) {
            builder.addConcepts(concept);
        }
        return builder.build();
    }

    private static List<String> rendered(Expansion expansion) {
        return expansion.concepts().stream()
                .map(ref -> ref.getSchemeId() + ":" + ref.getConceptId())
                .toList();
    }

    /** subject v1: a polyhierarchy under science with a retired chain into chemistry. */
    private static ConceptScheme subjects() {
        return scheme(
                concept("science"),
                concept("physics").addBroader("science")
                        .addExactMatch(ref("lcsh", "sh-physics"))
                        .addCloseMatch(ref("wiki", "physics"))
                        .addCloseMatch(ref("wiki", "gone"))
                        .addExactMatch(ref("absent", "x")),
                concept("chemistry").addBroader("science").addRelated("physics"),
                concept("biophysics").addBroader("physics").addBroader("chemistry"),
                concept("quantum").addBroader("physics"),
                concept("natural-philosophy").setDeprecated(true).addReplacedBy("old-physics"),
                concept("old-physics").setDeprecated(true).addReplacedBy("physics"),
                concept("alchemy").setDeprecated(true).addReplacedBy("chemistry"));
    }

    /** lcsh: maps back to subject on its own side, and onward to a third scheme. */
    private static ConceptScheme lcsh() {
        return scheme(
                concept("sh-physics").addExactMatch(ref("dewey", "530")),
                concept("sh-optics").addBroader("sh-physics"));
    }

    /** dewey: writes its exact match toward subject, the only side of the pair stored. */
    private static ConceptScheme dewey() {
        return scheme(
                concept("500"),
                concept("530").addBroader("500"),
                concept("535").addBroader("530"));
    }

    /** wiki: close_match is not transitive, so its own close match must not leak in. */
    private static ConceptScheme wiki() {
        return scheme(
                concept("physics").addCloseMatch(ref("dewey", "500")),
                concept("mechanics").addCloseMatch(ref("subject", "physics")));
    }

    private static ConceptSchemeMounts mountAll(FakeSource source, DistributedConfig config) {
        source.put("subject", "s1", subjects());
        source.put("lcsh", "l1", lcsh());
        source.put("dewey", "d1", dewey());
        source.put("wiki", "w1", wiki());
        ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(
                config, List.of("subject", "lcsh", "dewey", "wiki"));
        config.refresh();
        return mounts;
    }

    @Test
    void schemesFollowTheirSubjectsAndSwap() {
        FakeSource source = new FakeSource();
        source.put("subject", "s1", subjects());
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject"));
            assertThat(mounts.scheme("subject")).as("unmounted until a document applies").isEmpty();

            assertThat(config.refresh().applied()).containsExactly("concept-scheme:subject");
            ConceptSchemeCatalog.Mounted mounted = mounts.scheme("subject").orElseThrow();
            assertThat(mounted.version()).isEqualTo("s1");
            assertThat(mounted.conceptIds()).contains("physics", "alchemy").hasSize(8);

            source.put("subject", "s2", scheme(concept("physics")));
            config.refresh();
            assertThat(mounts.scheme("subject").orElseThrow().conceptIds())
                    .containsExactly("physics");
            assertThat(mounts.scheme("subject").orElseThrow().version()).isEqualTo("s2");
        }
    }

    @Test
    void emptyMountsRefuseEverySchemeLikeAnUnappliedFollow() {
        ConceptSchemeMounts mounts = ConceptSchemeMounts.empty();
        assertThat(mounts.scheme("subject")).isEmpty();
        assertThat(mounts.decidesMembership()).isTrue();
        assertThatThrownBy(() -> mounts.expand(ref("subject", "physics"), DEFAULT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("subject");
    }

    @Test
    void aSchemeFailingTheGraphChecksIsRefusedAndThePreviousVersionKeepsServing() {
        FakeSource source = new FakeSource();
        source.put("subject", "s1", subjects());
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject"));
            config.refresh();

            source.put("subject", "s2", scheme(
                    concept("a").addBroader("b"), concept("b").addBroader("a")));
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.applied()).isEmpty();
            assertThat(outcome.refused()).singleElement().satisfies(refusal -> {
                assertThat(refusal.subject()).isEqualTo("concept-scheme:subject");
                assertThat(refusal.version()).isEqualTo("s2");
                assertThat(refusal.reason()).contains("broader cycle: a -> b -> a");
            });
            assertThat(mounts.scheme("subject").orElseThrow().version()).isEqualTo("s1");
            assertThat(mounts.expand(ref("subject", "physics"), DEFAULT).mountedVersions())
                    .containsEntry("subject", "s1");

            // The refused version is not remembered as applied: the next refresh refuses it
            // again rather than reporting it unchanged.
            assertThat(config.refresh().refused()).hasSize(1);
        }
    }

    @Test
    void aSchemeFailingItsDeclaredRulesNeverMounts() {
        FakeSource source = new FakeSource();
        source.put("subject", "s1", scheme(concept("has space")));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject"));
            assertThat(config.refresh().refused()).singleElement()
                    .satisfies(refusal -> assertThat(refusal.reason()).contains("concepts[0].id"));
            assertThat(mounts.scheme("subject")).isEmpty();
        }
    }

    @Test
    void aMappingIntoTheOwnSchemeIsRefusedUsingTheSubjectAsIdentity() {
        // The same document mounts under one id and is refused under the id it maps into.
        ConceptScheme document = scheme(concept("a").addExactMatch(ref("lcsh", "b")));
        FakeSource source = new FakeSource();
        source.put("subject", "s1", document);
        source.put("lcsh", "l1", document);
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject", "lcsh"));
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.applied()).containsExactly("concept-scheme:subject");
            assertThat(outcome.refused()).extracting(DistributedConfig.Refusal::subject)
                    .containsExactly("concept-scheme:lcsh");
            assertThat(mounts.scheme("lcsh")).isEmpty();
        }
    }

    @Test
    void aValidatorOverTheMountsSeesEachSwap() {
        FakeSource source = new FakeSource();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject"));
            ProtoValidator validator = ProtoValidator.create(ValidationRuleSources.defaults(),
                    TaxonomyCatalog.empty(), PostalCodeCatalog.empty(), mounts);
            Message tagged = tagged(ref("subject", "alchemy"));

            assertThat(ruleIds(validator.validate(tagged))).containsExactly("concept.unmounted");

            source.put("subject", "s1", subjects());
            config.refresh();
            assertThat(validator.validate(tagged).valid())
                    .as("a deprecated concept is still a member").isTrue();

            source.put("subject", "s2", scheme(concept("physics")));
            config.refresh();
            ValidationResult result = validator.validate(tagged);
            assertThat(result.violations()).singleElement().satisfies(violation -> {
                assertThat(violation.ruleId()).isEqualTo("concept.member");
                assertThat(violation.message()).contains("alchemy").contains("version s2");
            });
        }
    }

    /** A document type with one ConceptRef field bound to the "subject" scheme. */
    private static Message tagged(ConceptRef value) {
        DescriptorProtos.FieldOptions bound = DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(ValidateProto.field,
                        FieldRules.newBuilder().addConceptSchemes("subject").build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("concept_scheme_mounts_test.proto")
                .setSyntax("proto3")
                .setPackage("ai.protomolt.proto.config.test")
                .addDependency(ConceptsProto.getDescriptor().getName())
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Tagged")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("subject").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.protomolt.proto.types.v1.ConceptRef")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(bound)))
                .build();
        Descriptors.Descriptor type;
        try {
            type = Descriptors.FileDescriptor.buildFrom(file,
                    new Descriptors.FileDescriptor[] {ConceptsProto.getDescriptor()})
                    .findMessageTypeByName("Tagged");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
        return DynamicMessage.newBuilder(type).setField(type.findFieldByName("subject"), value).build();
    }

    private static List<String> ruleIds(ValidationResult result) {
        return result.violations().stream().map(ValidationResult.Violation::ruleId).toList();
    }

    @Test
    void theDefaultExpansionIsTheExactClosureAndTheRetiredPredecessors() {
        FakeSource source = new FakeSource();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = mountAll(source, config);
            Expansion expansion = mounts.expand(ref("subject", "physics"), DEFAULT);
            assertThat(rendered(expansion)).containsExactly(
                    // dewey:530 is reached transitively through lcsh; neither link is
                    // written from dewey's side.
                    "dewey:530",
                    "lcsh:sh-physics",
                    // Two hops back along replaced_by.
                    "subject:natural-philosophy",
                    "subject:old-physics",
                    "subject:physics");
            // wiki contributes nothing to this expansion, but its version is still a
            // dependency: the inbound exact_match lookup read it.
            assertThat(expansion.mountedVersions())
                    .containsExactly(Map.entry("dewey", "d1"), Map.entry("lcsh", "l1"),
                            Map.entry("subject", "s1"), Map.entry("wiki", "w1"));
            assertThat(expansion.unresolvedMappings())
                    .extracting(UnresolvedMapping::relation, m -> m.target().getSchemeId(),
                            UnresolvedMapping::reason)
                    .containsExactly(tuple("exact_match", "absent", "scheme \"absent\" is not mounted"));
        }
    }

    @Test
    void evidenceNamesEverySchemeTheReverseLookupReadEvenWhenItMatchedNothing() {
        FakeSource source = new FakeSource();
        source.put("subject", "s1", scheme(concept("physics")));
        source.put("other", "o1", scheme(concept("unrelated")));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(
                    config, List.of("subject", "other"));
            config.refresh();
            Expansion before = mounts.expand(ref("subject", "physics"), DEFAULT);
            assertThat(rendered(before)).containsExactly("subject:physics");
            assertThat(before.mountedVersions())
                    .containsExactly(Map.entry("other", "o1"), Map.entry("subject", "s1"));

            // subject is unchanged; a new version of other adds an inbound exact_match.
            source.put("other", "o2", scheme(concept("unrelated"),
                    concept("physik").addExactMatch(ref("subject", "physics"))));
            config.refresh();
            Expansion after = mounts.expand(ref("subject", "physics"), DEFAULT);
            assertThat(rendered(after)).containsExactly("other:physik", "subject:physics");
            assertThat(after.mountedVersions())
                    .containsExactly(Map.entry("other", "o2"), Map.entry("subject", "s1"));
        }
    }

    @Test
    void theExactClosureIsSymmetricFromEveryMember() {
        FakeSource source = new FakeSource();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = mountAll(source, config);
            assertThat(rendered(mounts.expand(ref("dewey", "530"), DEFAULT))).containsExactly(
                    "dewey:530", "lcsh:sh-physics", "subject:natural-philosophy",
                    "subject:old-physics", "subject:physics");
        }
    }

    @Test
    void narrowerAddsEachDescendantOnceAcrossThePolyhierarchy() {
        FakeSource source = new FakeSource();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = mountAll(source, config);
            assertThat(rendered(mounts.expand(ref("subject", "science"), NARROWER))).containsExactly(
                    "subject:biophysics", "subject:chemistry", "subject:physics",
                    "subject:quantum", "subject:science");
            // Descendants come from every scheme of the exact closure, in that scheme.
            assertThat(rendered(mounts.expand(ref("subject", "physics"), NARROWER))).containsExactly(
                    "dewey:530", "dewey:535", "lcsh:sh-optics", "lcsh:sh-physics",
                    "subject:biophysics", "subject:natural-philosophy",
                    "subject:old-physics", "subject:physics", "subject:quantum");
        }
    }

    @Test
    void closeMatchIsOneHopBothWaysAndReportsWhatItCannotResolve() {
        FakeSource source = new FakeSource();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = mountAll(source, config);
            Expansion expansion = mounts.expand(ref("subject", "physics"), CLOSE);
            assertThat(rendered(expansion))
                    .contains("wiki:physics", "wiki:mechanics")
                    .as("wiki:physics's own close match is a second hop")
                    .doesNotContain("dewey:500");
            assertThat(expansion.unresolvedMappings())
                    .extracting(UnresolvedMapping::relation, UnresolvedMapping::reason)
                    .containsExactly(
                            tuple("close_match",
                                    "\"gone\" is not a concept of scheme \"wiki\" at version w1"),
                            tuple("exact_match", "scheme \"absent\" is not mounted"));
            assertThat(expansion.mountedVersions()).containsKey("wiki");
        }
    }

    @Test
    void navigationRelationsNeverExpandAQuery() {
        FakeSource source = new FakeSource();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = mountAll(source, config);
            // chemistry: related to physics, broader science, a retired predecessor.
            assertThat(rendered(mounts.expand(ref("subject", "chemistry"),
                    ConceptExpansion.newBuilder().setNarrower(true).setCloseMatch(true).build())))
                    .containsExactly("subject:alchemy", "subject:biophysics", "subject:chemistry");
        }
    }

    @Test
    void anExpansionOverTheBoundIsRefusedNotTruncated() {
        FakeSource source = new FakeSource();
        source.put("subject", "s1", subjects());
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject"), 4);
            config.refresh();
            assertThat(rendered(mounts.expand(ref("subject", "physics"), DEFAULT))).hasSize(3);
            assertThatThrownBy(() -> mounts.expand(ref("subject", "science"), NARROWER))
                    .as("science and its four descendants are five concepts")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("subject:science")
                    .hasMessageContaining("more than 4 concepts");
        }
    }

    @Test
    void aQueryOnAnUnmountedSchemeOrUnknownConceptIsRefused() {
        FakeSource source = new FakeSource();
        source.put("subject", "s1", subjects());
        try (DistributedConfig config = DistributedConfig.over(source)) {
            ConceptSchemeMounts mounts = ConceptSchemeMounts.follow(config, List.of("subject", "region"));
            config.refresh();
            assertThatThrownBy(() -> mounts.expand(ref("region", "emea"), DEFAULT))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("\"region\" is not mounted");
            assertThatThrownBy(() -> mounts.expand(ref("subject", "biology"), DEFAULT))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("biology").hasMessageContaining("version s1");
        }
    }

    @Test
    void schemeIdsAndTheBoundAreCheckedWhenFollowing() {
        try (DistributedConfig config = DistributedConfig.over(new FakeSource())) {
            assertThatThrownBy(() -> ConceptSchemeMounts.follow(config, List.of("Not A Slug")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Not A Slug");
            assertThatThrownBy(() -> ConceptSchemeMounts.follow(config, List.of("subject"), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
