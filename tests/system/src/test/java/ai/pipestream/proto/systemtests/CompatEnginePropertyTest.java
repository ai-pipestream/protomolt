package ai.pipestream.proto.systemtests;

import ai.pipestream.proto.compat.ChangeRules;
import ai.pipestream.proto.compat.CompatibilityChecker;
import ai.pipestream.proto.compat.CompatibilityMode;
import ai.pipestream.proto.compat.CompatibilityResult;
import ai.pipestream.proto.compat.SchemaChange;
import ai.pipestream.proto.compat.SchemaDiff;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Randomized properties over the compat engine ({@link SchemaDiff} +
 * {@link CompatibilityChecker} with the default wire-only rules), driven by a fixed-seed
 * {@link Random} so every run explores the identical case set. Schemas are small proto3
 * messages (2-6 fields over a scalar pool plus two nested enums) built directly as descriptor
 * protos; mutations are add-field, remove-field, change-type, and rename, each with an exact
 * inverse.
 *
 * <p><b>Dropped as unsound</b> — the originally proposed property "a mutation sequence applied
 * then reversed yields a schema where FULL-mode violations exist iff the sequence contained a
 * lossy step" cannot hold as stated, for three reasons:</p>
 * <ol>
 *   <li>A lossy step followed later in the same sequence by its own reversal (e.g. change
 *       {@code int32 -> string} then {@code string -> int32}) nets out to no change, so no
 *       violation survives even though the sequence "contained a lossy step".</li>
 *   <li>A lossy type change on a field that a later step removes leaves no violation either:
 *       under the default wire rules a field removal is clean
 *       ({@code FIELD_REMOVED} carries only JSON/source impacts).</li>
 *   <li>Conversely, remove-then-re-add of the same number with a different type is lossy on
 *       the wire without any single step being a "type change".</li>
 * </ol>
 * <p>It is restated here as two sound properties: (a) a sequence followed by its exact inverse
 * restores the schema byte-for-byte, hence a clean diff
 * ({@link #mutationSequenceFollowedByItsExactInverseRestoresTheSchema()}), and (b) FULL-mode
 * violations exist iff the <em>net</em> per-field-number type transition between start and end
 * schema is wire-lossy, judged by an independent oracle over protobuf's wire-format groups
 * ({@link #fullModeViolationsExistIffTheNetFieldTypeTransitionIsWireLossy()}).</p>
 *
 * <p><b>Restricted, not dropped</b> — the backward/forward mirror property holds only for
 * type-change rules whose impact classification is direction-symmetric. The engine computes
 * one diff (old to new) and tags each change with directional impacts; cross-wire-group scalar
 * changes and enum-identity changes carry both wire directions, so a BACKWARD violation on
 * {@code diff(x, y)} mirrors as a FORWARD violation on {@code diff(y, x)}. The one deliberate
 * exception is {@code bytes <-> string}: {@code bytes -> string} is backward-only lossy (old
 * payloads may hold non-UTF-8 bytes) while {@code string -> bytes} is wire-clean, so those
 * transitions are excluded from the mirror and pinned separately by
 * {@link #bytesToStringAsymmetryJustifiesItsExclusionFromTheMirrorProperty()}.</p>
 */
class CompatEnginePropertyTest {

    private static final int ITERATIONS = 200;
    private static final String PACKAGE = "gen";
    private static final String MESSAGE = "Doc";

    private final CompatibilityChecker checker = CompatibilityChecker.create();

    // ------------------------------------------------------------------ the model

    /** The generator's type pool: scalars over every wire group, plus two nested enums. */
    private enum FieldType {
        INT32(Type.TYPE_INT32), INT64(Type.TYPE_INT64), UINT32(Type.TYPE_UINT32),
        UINT64(Type.TYPE_UINT64), BOOL(Type.TYPE_BOOL),
        SINT32(Type.TYPE_SINT32), SINT64(Type.TYPE_SINT64),
        FIXED32(Type.TYPE_FIXED32), SFIXED32(Type.TYPE_SFIXED32),
        FIXED64(Type.TYPE_FIXED64), SFIXED64(Type.TYPE_SFIXED64),
        FLOAT(Type.TYPE_FLOAT), DOUBLE(Type.TYPE_DOUBLE),
        STRING(Type.TYPE_STRING), BYTES(Type.TYPE_BYTES),
        ENUM_COLOR(Type.TYPE_ENUM), ENUM_SHADE(Type.TYPE_ENUM);

        final Type protoType;

        FieldType(Type protoType) {
            this.protoType = protoType;
        }

        boolean isEnum() {
            return protoType == Type.TYPE_ENUM;
        }

        /** Protobuf wire-format encoding group; proto3 enums are open, so enum sits in varint. */
        String wireGroup() {
            return switch (this) {
                case INT32, INT64, UINT32, UINT64, BOOL, ENUM_COLOR, ENUM_SHADE -> "varint";
                case SINT32, SINT64 -> "zigzag";
                case FIXED32, SFIXED32 -> "fixed32";
                case FIXED64, SFIXED64 -> "fixed64";
                case FLOAT -> "float";
                case DOUBLE -> "double";
                case STRING -> "string";
                case BYTES -> "bytes";
            };
        }
    }

    private record FieldModel(int number, String name, FieldType type) {
    }

    /** A mutable schema-under-generation: {@code Doc} plus its field map and name/number wells. */
    private static final class SchemaModel {
        final Map<Integer, FieldModel> fields = new LinkedHashMap<>();
        int nextNumber = 1;
        int nameSeq = 0;

        SchemaModel copy() {
            SchemaModel copy = new SchemaModel();
            copy.fields.putAll(fields);
            copy.nextNumber = nextNumber;
            copy.nameSeq = nameSeq;
            return copy;
        }

        String freshName() {
            return "field_" + (++nameSeq);
        }

        FileDescriptorSet toDescriptorSet() {
            DescriptorProto.Builder doc = DescriptorProto.newBuilder()
                    .setName(MESSAGE)
                    .addEnumType(enumType("Color", "COLOR"))
                    .addEnumType(enumType("Shade", "SHADE"));
            fields.values().stream()
                    .sorted(Comparator.comparingInt(FieldModel::number))
                    .forEach(field -> {
                        FieldDescriptorProto.Builder proto = FieldDescriptorProto.newBuilder()
                                .setName(field.name())
                                .setNumber(field.number())
                                .setLabel(Label.LABEL_OPTIONAL)
                                .setType(field.type().protoType);
                        if (field.type() == FieldType.ENUM_COLOR) {
                            proto.setTypeName("." + PACKAGE + "." + MESSAGE + ".Color");
                        } else if (field.type() == FieldType.ENUM_SHADE) {
                            proto.setTypeName("." + PACKAGE + "." + MESSAGE + ".Shade");
                        }
                        doc.addField(proto);
                    });
            return FileDescriptorSet.newBuilder()
                    .addFile(FileDescriptorProto.newBuilder()
                            .setName("gen/doc.proto")
                            .setPackage(PACKAGE)
                            .setSyntax("proto3")
                            .addMessageType(doc))
                    .build();
        }

        private static EnumDescriptorProto enumType(String name, String prefix) {
            return EnumDescriptorProto.newBuilder()
                    .setName(name)
                    .addValue(EnumValueDescriptorProto.newBuilder()
                            .setName(prefix + "_UNSPECIFIED").setNumber(0))
                    .addValue(EnumValueDescriptorProto.newBuilder()
                            .setName(prefix + "_ONE").setNumber(1))
                    .build();
        }
    }

    /**
     * One mutation as a (before, after) pair keyed by field number: add is (null, f), remove is
     * (f, null), type change and rename replace f in place. {@link #inverse()} swaps the pair,
     * so an applied sequence reversed through its inverses is an exact undo.
     */
    private record Mutation(FieldModel before, FieldModel after) {
        void apply(SchemaModel model) {
            if (after == null) {
                model.fields.remove(before.number());
            } else {
                model.fields.put(after.number(), after);
            }
        }

        Mutation inverse() {
            return new Mutation(after, before);
        }
    }

    // ------------------------------------------------------------------ generation

    private static SchemaModel randomSchema(Random random) {
        SchemaModel model = new SchemaModel();
        int fieldCount = 2 + random.nextInt(5);
        for (int i = 0; i < fieldCount; i++) {
            int number = model.nextNumber++;
            model.fields.put(number,
                    new FieldModel(number, model.freshName(), randomType(random)));
        }
        return model;
    }

    private static FieldType randomType(Random random) {
        FieldType[] pool = FieldType.values();
        return pool[random.nextInt(pool.length)];
    }

    /** Generates one applicable mutation against the model's current state. */
    private static Mutation randomMutation(Random random, SchemaModel model) {
        List<FieldModel> current = new ArrayList<>(model.fields.values());
        int kind = random.nextInt(4);
        if (kind == 0 || current.isEmpty()) { // add
            int number = model.nextNumber++;
            return new Mutation(null, new FieldModel(number, model.freshName(),
                    randomType(random)));
        }
        FieldModel target = current.get(random.nextInt(current.size()));
        return switch (kind) {
            case 1 -> current.size() > 1
                    ? new Mutation(target, null) // remove, but never empty the message
                    : new Mutation(target, new FieldModel(target.number(), target.name(),
                            otherType(random, target.type())));
            case 2 -> new Mutation(target, new FieldModel(target.number(), target.name(),
                    otherType(random, target.type()))); // change type
            default -> new Mutation(target, new FieldModel(target.number(), model.freshName(),
                    target.type())); // rename
        };
    }

    private static FieldType otherType(Random random, FieldType current) {
        FieldType candidate;
        do {
            candidate = randomType(random);
        } while (candidate == current);
        return candidate;
    }

    /** Applies {@code count} freshly generated mutations to a copy of {@code start}. */
    private static List<Mutation> mutate(Random random, SchemaModel evolving, int count) {
        List<Mutation> sequence = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Mutation mutation = randomMutation(random, evolving);
            mutation.apply(evolving);
            sequence.add(mutation);
        }
        return sequence;
    }

    // ------------------------------------------------------------------ properties

    @Test
    void diffOfAnyGeneratedSchemaAgainstItselfIsEmpty() {
        Random random = new Random(42);
        for (int i = 0; i < ITERATIONS; i++) {
            FileDescriptorSet set = randomSchema(random).toDescriptorSet();
            assertThat(SchemaDiff.diff(set, set)).as("iteration %d", i).isEmpty();
        }
    }

    @Test
    void diffAndEveryModeCheckNeverThrowAcrossRandomSchemaPairs() {
        Random random = new Random(42);
        for (int i = 0; i < ITERATIONS; i++) {
            SchemaModel x = randomSchema(random);
            SchemaModel y = random.nextBoolean() ? randomSchema(random) : x.copy();
            mutate(random, y, random.nextInt(5));
            FileDescriptorSet oldSet = x.toDescriptorSet();
            FileDescriptorSet newSet = y.toDescriptorSet();
            int iteration = i;
            assertThatCode(() -> {
                SchemaDiff.diff(oldSet, newSet);
                SchemaDiff.diff(newSet, oldSet);
                for (CompatibilityMode mode : CompatibilityMode.values()) {
                    checker.check(oldSet, newSet, mode);
                }
            }).as("iteration %d", iteration).doesNotThrowAnyException();
        }
    }

    @Test
    void backwardViolationsMirrorAsForwardViolationsOnTheReversedDiffForSymmetricTypeChangeRules() {
        Random random = new Random(42);
        int nonEmptyCases = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            SchemaModel x = randomSchema(random);
            SchemaModel y = x.copy();
            mutate(random, y, 1 + random.nextInt(4));

            Set<String> backward = typeChangeViolationKeys(
                    checker.check(x.toDescriptorSet(), y.toDescriptorSet(),
                            CompatibilityMode.BACKWARD),
                    y, x);
            Set<String> forward = typeChangeViolationKeys(
                    checker.check(y.toDescriptorSet(), x.toDescriptorSet(),
                            CompatibilityMode.FORWARD),
                    x, y);
            assertThat(forward).as("iteration %d", i).isEqualTo(backward);
            if (!backward.isEmpty()) {
                nonEmptyCases++;
            }
        }
        // Non-vacuity: the fixed seed must actually exercise mirrored violations.
        assertThat(nonEmptyCases).isPositive();
    }

    /**
     * The engine's documented one-way rule, pinned here because it is exactly what the mirror
     * property above must exclude: {@code bytes -> string} breaks BACKWARD (old payloads may be
     * non-UTF-8) while its reverse is wire-clean, so no FORWARD violation mirrors it.
     */
    @Test
    void bytesToStringAsymmetryJustifiesItsExclusionFromTheMirrorProperty() {
        SchemaModel bytes = new SchemaModel();
        bytes.fields.put(1, new FieldModel(1, "payload", FieldType.BYTES));
        SchemaModel string = new SchemaModel();
        string.fields.put(1, new FieldModel(1, "payload", FieldType.STRING));

        CompatibilityResult toString = checker.check(bytes.toDescriptorSet(),
                string.toDescriptorSet(), CompatibilityMode.BACKWARD);
        assertThat(toString.isCompatible()).isFalse();
        assertThat(toString.violations())
                .anyMatch(v -> v.ruleId().equals(ChangeRules.FIELD_TYPE_CHANGED));

        CompatibilityResult reversedForward = checker.check(string.toDescriptorSet(),
                bytes.toDescriptorSet(), CompatibilityMode.FORWARD);
        assertThat(reversedForward.isCompatible()).isTrue();
    }

    @Test
    void mutationSequenceFollowedByItsExactInverseRestoresTheSchema() {
        Random random = new Random(42);
        for (int i = 0; i < ITERATIONS; i++) {
            SchemaModel original = randomSchema(random);
            SchemaModel roundTripped = original.copy();
            List<Mutation> sequence = mutate(random, roundTripped, 1 + random.nextInt(5));
            for (int step = sequence.size() - 1; step >= 0; step--) {
                sequence.get(step).inverse().apply(roundTripped);
            }

            assertThat(roundTripped.fields).as("iteration %d", i).isEqualTo(original.fields);
            assertThat(roundTripped.toDescriptorSet()).isEqualTo(original.toDescriptorSet());
            assertThat(SchemaDiff.diff(original.toDescriptorSet(),
                    roundTripped.toDescriptorSet())).isEmpty();
        }
    }

    @Test
    void fullModeViolationsExistIffTheNetFieldTypeTransitionIsWireLossy() {
        Random random = new Random(42);
        int lossyCases = 0;
        int cleanCases = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            SchemaModel x = randomSchema(random);
            SchemaModel y = x.copy();
            mutate(random, y, 1 + random.nextInt(5));

            CompatibilityResult result = checker.check(x.toDescriptorSet(), y.toDescriptorSet(),
                    CompatibilityMode.FULL);
            boolean oracle = anyNetTransitionIsLossy(x, y);

            assertThat(result.isCompatible()).as("iteration %d: %s", i, result.violations())
                    .isEqualTo(!oracle);
            // The oracle's completeness rests on type transitions being the only wire-impacting
            // changes this generator can produce; pin that so a generator change fails loudly.
            assertThat(result.violations())
                    .extracting(SchemaChange::ruleId)
                    .allMatch(rule -> rule.equals(ChangeRules.FIELD_TYPE_CHANGED)
                            || rule.equals(ChangeRules.FIELD_ENUM_TYPE_CHANGED));
            if (oracle) {
                lossyCases++;
            } else {
                cleanCases++;
            }
        }
        // Non-vacuity: the fixed seed must exercise both sides of the iff.
        assertThat(lossyCases).isPositive();
        assertThat(cleanCases).isPositive();
    }

    @Test
    void purelyAdditiveMutationsNeverViolateAnyMode() {
        Random random = new Random(42);
        for (int i = 0; i < ITERATIONS; i++) {
            SchemaModel x = randomSchema(random);
            SchemaModel y = x.copy();
            int additions = 1 + random.nextInt(3);
            for (int a = 0; a < additions; a++) {
                int number = y.nextNumber++;
                new Mutation(null, new FieldModel(number, y.freshName(), randomType(random)))
                        .apply(y);
            }
            for (CompatibilityMode mode : CompatibilityMode.values()) {
                assertThat(checker.check(x.toDescriptorSet(), y.toDescriptorSet(), mode)
                        .isCompatible()).as("iteration %d mode %s", i, mode).isTrue();
            }
        }
    }

    // ------------------------------------------------------------------ oracle + mirror keys

    /**
     * Violations of the direction-symmetric type-change rules, keyed by rule id and field
     * number. Numbers (the wire identity) rather than paths, because a field renamed and
     * retyped in one step reports different paths in the two diff directions. Transitions
     * between bytes and string are excluded — the one documented asymmetric pair.
     */
    private static Set<String> typeChangeViolationKeys(CompatibilityResult result,
                                                       SchemaModel newSide,
                                                       SchemaModel oldSide) {
        Set<Integer> excluded = bytesStringTransitions(oldSide, newSide);
        Map<String, Integer> numberByName = new LinkedHashMap<>();
        newSide.fields.values().forEach(f -> numberByName.put(f.name(), f.number()));

        Set<String> keys = new TreeSet<>();
        for (SchemaChange violation : result.violations()) {
            if (!violation.ruleId().equals(ChangeRules.FIELD_TYPE_CHANGED)
                    && !violation.ruleId().equals(ChangeRules.FIELD_ENUM_TYPE_CHANGED)) {
                continue;
            }
            String fieldName = violation.path().substring(violation.path().lastIndexOf('.') + 1);
            Integer number = numberByName.get(fieldName);
            assertThat(number).as("path %s must resolve in the new-side model", violation.path())
                    .isNotNull();
            if (!excluded.contains(number)) {
                keys.add(violation.ruleId() + "#" + number);
            }
        }
        return keys;
    }

    private static Set<Integer> bytesStringTransitions(SchemaModel a, SchemaModel b) {
        Set<Integer> numbers = new HashSet<>();
        for (FieldModel fieldA : a.fields.values()) {
            FieldModel fieldB = b.fields.get(fieldA.number());
            if (fieldB == null) {
                continue;
            }
            boolean pair = (fieldA.type() == FieldType.BYTES && fieldB.type() == FieldType.STRING)
                    || (fieldA.type() == FieldType.STRING && fieldB.type() == FieldType.BYTES);
            if (pair) {
                numbers.add(fieldA.number());
            }
        }
        return numbers;
    }

    /** The independent lossiness oracle over net per-field-number transitions. */
    private static boolean anyNetTransitionIsLossy(SchemaModel oldSide, SchemaModel newSide) {
        for (FieldModel oldField : oldSide.fields.values()) {
            FieldModel newField = newSide.fields.get(oldField.number());
            if (newField != null && transitionIsWireLossyUnderFull(oldField.type(),
                    newField.type())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code old -> new} breaks binary wire compatibility in either direction:
     * enum-identity changes always do; {@code string -> bytes} never does;
     * {@code bytes -> string} does (backward); otherwise a transition is lossy exactly when it
     * crosses wire-format encoding groups.
     */
    private static boolean transitionIsWireLossyUnderFull(FieldType oldType, FieldType newType) {
        if (oldType == newType) {
            return false;
        }
        if (oldType.isEnum() && newType.isEnum()) {
            return true; // different enum identity
        }
        if (oldType == FieldType.STRING && newType == FieldType.BYTES) {
            return false;
        }
        if (oldType == FieldType.BYTES && newType == FieldType.STRING) {
            return true;
        }
        return !oldType.wireGroup().equals(newType.wireGroup());
    }
}
