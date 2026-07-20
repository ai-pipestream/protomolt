package ai.pipestream.proto.quality;

import ai.pipestream.proto.quality.testdata.BrokenRules;
import ai.pipestream.proto.quality.testdata.DecayingDoc;
import ai.pipestream.proto.quality.testdata.DuplicateDimensionIds;
import ai.pipestream.proto.quality.testdata.MissingCelExpression;
import ai.pipestream.proto.quality.testdata.MissingDimensionId;
import ai.pipestream.proto.quality.testdata.NegativeWeight;
import ai.pipestream.proto.quality.testdata.ScoredDoc;
import ai.pipestream.proto.quality.testdata.Unscored;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * The schema declares what "good" means; the scorer just measures. Weights shift the composite,
 * bools coerce, out-of-range formulas clamp, a per-message evaluation failure is a measurement
 * gap rather than a zero or an exception, and CEL that cannot compile against the type is a
 * schema error that throws deterministically.
 */
class QualityScorerTest {

    private final QualityScorer scorer = QualityScorer.create();

    private static ScoredDoc.Builder decent() {
        return ScoredDoc.newBuilder()
                .setTitle("t")
                .setAuthor("a")
                .setBody("b".repeat(50))
                .setDivisor(2);
    }

    @Test
    void scoresDeclaredDimensionsAndWeighsTheComposite() {
        QualityReport report = scorer.score(decent().build());

        assertThat(report.scored()).isTrue();
        assertThat(report.dimensions().get("completeness")).isEqualTo(1.0);
        assertThat(report.dimensions().get("body")).isEqualTo(0.5);
        assertThat(report.dimensions().get("has_title")).isEqualTo(1.0);
        assertThat(report.dimensions().get("fragile")).isEqualTo(0.25);
        // (1*1 + 0.5*2 + 1*1 + 0.25*1) / (1+2+1+1)
        assertThat(report.composite()).isCloseTo(0.65, offset(1e-9));
        assertThat(report.failed()).isEmpty();
    }

    @Test
    void boolsScoreOneOrZeroAndFormulasClamp() {
        QualityReport report = scorer.score(decent()
                .setTitle("")
                .setAuthor("")
                .setBody("b".repeat(1000))
                .build());

        assertThat(report.dimensions().get("has_title")).isEqualTo(0.0);
        // 1000/100 = 10, clamped by the expression itself and by the scorer.
        assertThat(report.dimensions().get("body")).isEqualTo(1.0);
    }

    /** Division by a zero field: this message could not be measured on that dimension. */
    @Test
    void anEvaluationFailureIsAGapNotAZero() {
        QualityReport report = scorer.score(decent().setDivisor(0).build());

        assertThat(report.failed()).containsExactly("fragile");
        assertThat(report.dimensions()).doesNotContainKey("fragile");
        // The composite is the weighted average of what WAS measured.
        assertThat(report.composite()).isCloseTo((1.0 + 0.5 * 2 + 1.0) / 4.0, offset(1e-9));
    }

    @Test
    void aTypeWithNoDimensionsIsNotScored() {
        QualityReport report = scorer.score(Unscored.newBuilder().setAnything("x").build());
        assertThat(report.scored()).isFalse();
        assertThat(report).isSameAs(QualityReport.none());
    }

    /** CEL naming a field the message does not have is a schema error, not a bad score. */
    @Test
    void unCompilableDimensionsThrowDeterministically() {
        assertThatThrownBy(() -> scorer.score(BrokenRules.newBuilder().setPresent("x").build()))
                .isInstanceOf(QualitySchemaException.class)
                .hasMessageContaining("bad")
                .hasMessageContaining("does not compile");
    }

    /**
     * Two dimensions sharing an id cannot both be reported: the report keys on id, so one score
     * would be reported while the composite weighed both. The schema is refused at compile time.
     */
    @Test
    void duplicateDimensionIdsAreASchemaError() {
        assertThatThrownBy(() -> scorer.score(
                DuplicateDimensionIds.newBuilder().setPresent("x").build()))
                .isInstanceOf(QualitySchemaException.class)
                .hasMessageContaining("completeness")
                .hasMessageContaining("declared more than once");
    }

    /** The report keys on id, so a dimension without one could never be reported. */
    @Test
    void aDimensionWithNoIdIsASchemaError() {
        assertThatThrownBy(() -> scorer.score(
                MissingDimensionId.newBuilder().setPresent("x").build()))
                .isInstanceOf(QualitySchemaException.class)
                .hasMessage("A quality dimension on "
                        + "ai.pipestream.proto.quality.testdata.v1.MissingDimensionId has no id");
    }

    @Test
    void aDimensionWithNoCelExpressionIsASchemaError() {
        assertThatThrownBy(() -> scorer.score(
                MissingCelExpression.newBuilder().setPresent("x").build()))
                .isInstanceOf(QualitySchemaException.class)
                .hasMessage("Quality dimension 'completeness' on "
                        + "ai.pipestream.proto.quality.testdata.v1.MissingCelExpression"
                        + " has no CEL expression");
    }

    /** A negative weight would pull the composite the wrong way, or below zero outright. */
    @Test
    void aNegativeWeightIsASchemaError() {
        assertThatThrownBy(() -> scorer.score(
                NegativeWeight.newBuilder().setPresent("x").build()))
                .isInstanceOf(QualitySchemaException.class)
                .hasMessage("Quality dimension 'completeness' on "
                        + "ai.pipestream.proto.quality.testdata.v1.NegativeWeight"
                        + " has a negative weight");
    }

    /** exp() is the decay curve helper; it is not in the CEL standard library. */
    @Test
    void expIsAvailableToDimensionExpressions() {
        assertThat(scorer.score(DecayingDoc.newBuilder().setAgeDays(0).build())
                .dimensions().get("recency")).isEqualTo(1.0);
        assertThat(scorer.score(DecayingDoc.newBuilder().setAgeDays(5).build())
                .dimensions().get("recency")).isCloseTo(Math.exp(-0.5), offset(1e-12));
        assertThat(scorer.score(DecayingDoc.newBuilder().setAgeDays(30).build())
                .dimensions().get("recency")).isCloseTo(Math.exp(-3.0), offset(1e-12));
    }

    /**
     * The per-type cache is clear-on-threshold, not an LRU: crossing the bound drops everything
     * rather than growing without limit for a process that meets many types. This pins the
     * discipline and that an evicted type recompiles to the same scores.
     */
    @Test
    void thePerTypeCacheClearsOnceItReachesItsBound() throws Exception {
        QualityScorer local = QualityScorer.create();
        Map<?, ?> cache = cacheOf(local);
        int bound = maxCachedTypes();

        double composite = local.score(decent().build()).composite();
        assertThat(cache).hasSize(1);

        // Fill exactly to the bound with distinct types.
        for (int i = 0; i < bound - 1; i++) {
            local.score(DynamicMessage.getDefaultInstance(syntheticType(i)));
        }
        assertThat(cache).hasSize(bound);

        // The next unseen type finds the cache full: it is cleared, then this one type is cached.
        local.score(DynamicMessage.getDefaultInstance(syntheticType(bound)));
        assertThat(cache).hasSize(1);

        assertThat(local.score(decent().build()).composite()).isEqualTo(composite);
        assertThat(cache).hasSize(2);
    }

    private static Map<?, ?> cacheOf(QualityScorer scorer) throws Exception {
        Field field = QualityScorer.class.getDeclaredField("byType");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(scorer);
    }

    private static int maxCachedTypes() throws Exception {
        Field field = QualityScorer.class.getDeclaredField("MAX_CACHED_TYPES");
        field.setAccessible(true);
        return (int) field.getInt(null);
    }

    /** A distinct, unannotated message type — enough to occupy one cache slot. */
    private static Descriptor syntheticType(int index) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("quality/cache/synthetic" + index + ".proto")
                .setSyntax("proto3")
                .setPackage("quality.cache")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Synthetic" + index)
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("value")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Synthetic" + index);
    }

    /** The annotation must survive a descriptor linked without the quality extension. */
    @Test
    void readsAnnotationsCarriedOnlyAsUnknownFields() throws Exception {
        Descriptor blind = relinkWithoutExtensions(ScoredDoc.getDescriptor().getFile())
                .findMessageTypeByName("ScoredDoc");
        assertThat(blind.getOptions().getUnknownFields().asMap()).isNotEmpty();

        DynamicMessage message = DynamicMessage.newBuilder(blind)
                .setField(blind.findFieldByName("title"), "t")
                .setField(blind.findFieldByName("author"), "a")
                .setField(blind.findFieldByName("divisor"), 2)
                .build();

        QualityReport report = QualityScorer.create().score(message);
        assertThat(report.scored()).isTrue();
        assertThat(report.dimensions().get("completeness")).isEqualTo(1.0);
    }

    private static FileDescriptor relinkWithoutExtensions(FileDescriptor file) throws Exception {
        List<FileDescriptor> dependencies = new ArrayList<>();
        for (FileDescriptor dependency : file.getDependencies()) {
            dependencies.add(relinkWithoutExtensions(dependency));
        }
        FileDescriptorProto blind = FileDescriptorProto.parseFrom(file.toProto().toByteArray());
        return FileDescriptor.buildFrom(blind, dependencies.toArray(new FileDescriptor[0]));
    }
}
