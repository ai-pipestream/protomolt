package ai.protomolt.proto.prompt;

import ai.protomolt.proto.llm.DescriptorLlm;
import ai.protomolt.proto.llm.FieldLlm;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.meta.MessageMeta;
import ai.protomolt.proto.quality.QualityProto;
import ai.protomolt.proto.quality.QualityRules;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Renders the instruction prose of a {@link PromptPacket}. Reads every annotation
 * family a descriptor can carry — meta.v1 descriptions, validate.v1 constraints and CEL
 * through the neutral rule-source chain, quality.v1 scoring dimensions, llm.v1
 * instructions and safeguards — and lays them out in reading order for the model.
 */
final class InstructionRenderer {

    private InstructionRenderer() {
    }

    static String render(Descriptor descriptor, RenderPromptRequest request,
            List<ValidationRuleSource> sources, TypeRegistry typeRegistry) {
        StringBuilder out = new StringBuilder();

        out.append("You are filling the form \"").append(descriptor.getFullName()).append("\". ");
        DescriptorMetadata.message(descriptor).map(MessageMeta::getDescription)
                .filter(d -> !d.isEmpty()).ifPresent(d -> out.append(d).append(' '));
        DescriptorLlm.message(descriptor).ifPresent(m -> {
            if (!m.getInstruction().isEmpty()) {
                out.append(m.getInstruction()).append(' ');
            }
        });
        out.append('\n');

        if (request.hasPersona()) {
            Persona persona = request.getPersona();
            out.append("\nPersona: ").append(persona.getId())
                    .append(" (version ").append(persona.getVersion()).append(")\n");
            out.append(persona.getInstructions()).append('\n');
            appendBullets(out, "Persona safeguards:", persona.getSafeguardsList());
        }

        List<String> messageRules = new ArrayList<>();
        List<MessageConstraints> collected = new ArrayList<>(sources.size());
        for (ValidationRuleSource source : sources) {
            source.messageConstraints(descriptor).ifPresent(collected::add);
        }
        for (MessageConstraints constraints : collected) {
            for (CelConstraint cel : constraints.cel()) {
                messageRules.add(celProse(cel));
            }
            for (String oneof : constraints.requiredOneofs()) {
                messageRules.add("exactly one of the \"" + oneof + "\" alternatives must be set");
            }
        }
        appendBullets(out, "Form-wide requirements:", messageRules);
        DescriptorLlm.message(descriptor)
                .ifPresent(m -> appendBullets(out, "Form-wide safeguards:", m.getSafeguardsList()));

        List<FieldDescriptor> fields = descriptor.getFields();
        out.append("\nFields (fill every required field; fill optional fields when the information is available):\n");
        for (int i = 0; i < fields.size(); i++) {
            appendField(out, i + 1, fields.get(i), sources);
        }

        appendQuality(out, descriptor);

        out.append("\nRespond with exactly one JSON object that validates against the provided "
                + "JSON Schema. Produce no text before or after it.\n");

        if (request.hasOverrides()) {
            out.append("\nDocument-specific context (takes precedence where it conflicts):\n");
            out.append(renderOverrides(request, typeRegistry)).append('\n');
        }
        return out.toString();
    }

    private static void appendField(StringBuilder out, int index, FieldDescriptor field,
            List<ValidationRuleSource> sources) {
        List<FieldConstraints> collected = new ArrayList<>(sources.size());
        for (ValidationRuleSource source : sources) {
            source.fieldConstraints(field).ifPresent(collected::add);
        }
        boolean required = collected.stream().anyMatch(FieldConstraints::required);

        out.append(index).append(". \"").append(field.getJsonName()).append("\" (")
                .append(typeText(field)).append(required ? ") - required\n" : ") - optional\n");

        DescriptorMetadata.field(field).ifPresent(meta -> {
            if (!meta.getDescription().isEmpty()) {
                out.append("   ").append(meta.getDescription()).append('\n');
            }
        });
        Optional<FieldLlm> llm = DescriptorLlm.field(field);
        llm.map(FieldLlm::getInstruction).filter(d -> !d.isEmpty())
                .ifPresent(d -> out.append("   Instruction: ").append(d).append('\n'));

        List<String> requirements = new ArrayList<>();
        for (FieldConstraints constraints : collected) {
            requirements.addAll(constraintProse(field, constraints));
        }
        if (field.getType() == FieldDescriptor.Type.ENUM) {
            // The model cannot fill what it cannot name: enum vocabularies live in the
            // schema, not in any prose it has seen, so spell them out. (Enum value
            // comments ride SourceCodeInfo, which descriptor sets do not carry.)
            requirements.add("defined values: " + enumVocabulary(field));
        }
        if (requirements.isEmpty() && !required) {
            out.append("   Requirements: none beyond the type.\n");
        } else {
            if (required) {
                requirements.addFirst("must be present and non-empty");
            }
            appendBullets(out, "   Requirements:", requirements, "   ");
        }
        llm.ifPresent(l -> appendBullets(out, "   Safeguards:", l.getSafeguardsList(), "   "));
        llm.filter(FieldLlm::getVolatile).ifPresent(l -> out.append(
                "   Note: this value is time-relative; later re-verification may legitimately"
                        + " change it.\n"));
    }

    private static List<String> constraintProse(FieldDescriptor field, FieldConstraints constraints) {
        List<String> prose = new ArrayList<>();
        constraints.string().ifPresent(s -> stringProse(prose, s));
        constraints.integral().ifPresent(n -> numericProse(prose,
                n.constant().isPresent() ? Optional.of(n.constant().getAsLong()) : Optional.empty(),
                n.gt().isPresent() ? Optional.of(n.gt().getAsLong()) : Optional.empty(),
                n.gte().isPresent() ? Optional.of(n.gte().getAsLong()) : Optional.empty(),
                n.lt().isPresent() ? Optional.of(n.lt().getAsLong()) : Optional.empty(),
                n.lte().isPresent() ? Optional.of(n.lte().getAsLong()) : Optional.empty(),
                n.in(), n.notIn(), n.ruleIdPrefix(), field, null));
        constraints.floating().ifPresent(n -> numericProse(prose,
                n.constant().isPresent() ? Optional.of(n.constant().getAsDouble()) : Optional.empty(),
                n.gt().isPresent() ? Optional.of(n.gt().getAsDouble()) : Optional.empty(),
                n.gte().isPresent() ? Optional.of(n.gte().getAsDouble()) : Optional.empty(),
                n.lt().isPresent() ? Optional.of(n.lt().getAsDouble()) : Optional.empty(),
                n.lte().isPresent() ? Optional.of(n.lte().getAsDouble()) : Optional.empty(),
                n.in(), n.notIn(), n.ruleIdPrefix(), field, n.finite() ? Boolean.TRUE : null));
        constraints.bool().flatMap(b -> b.constant())
                .ifPresent(c -> prose.add("must be " + c));
        constraints.enumeration().ifPresent(e -> {
            if (e.definedOnly()) {
                prose.add("must be a defined " + field.getEnumType().getFullName() + " value");
            }
            e.constant().ifPresent(c -> prose.add("must equal " + enumName(field, c)));
            if (!e.in().isEmpty()) {
                prose.add("must be one of: " + joinEnums(field, e.in()));
            }
            if (!e.notIn().isEmpty()) {
                prose.add("must not be any of: " + joinEnums(field, e.notIn()));
            }
        });
        constraints.bytes().ifPresent(b -> {
            b.len().ifPresent(n -> prose.add("must be exactly " + n + " bytes"));
            b.minLen().ifPresent(n -> prose.add("must be at least " + n + " bytes"));
            b.maxLen().ifPresent(n -> prose.add("must be at most " + n + " bytes"));
            b.pattern().ifPresent(p -> prose.add("must match /" + p + "/ (base64-decoded form)"));
        });
        constraints.repeated().ifPresent(r -> {
            r.minItems().ifPresent(n -> prose.add("must have at least " + n + " item(s)"));
            r.maxItems().ifPresent(n -> prose.add("must have at most " + n + " item(s)"));
            if (r.unique()) {
                prose.add("items must be unique");
            }
            r.items().ifPresent(items -> {
                for (String item : constraintProse(field, items)) {
                    prose.add("each item: " + item);
                }
            });
        });
        constraints.map().ifPresent(m -> {
            m.minPairs().ifPresent(n -> prose.add("must have at least " + n + " entries"));
            m.maxPairs().ifPresent(n -> prose.add("must have at most " + n + " entries"));
        });
        constraints.timestamp().ifPresent(t -> {
            t.constant().ifPresent(c -> prose.add("must equal " + c));
            t.gt().ifPresent(c -> prose.add("must be after " + c));
            t.gte().ifPresent(c -> prose.add("must be at or after " + c));
            t.lt().ifPresent(c -> prose.add("must be before " + c));
            t.lte().ifPresent(c -> prose.add("must be at or before " + c));
            if (t.ltNow()) {
                prose.add("must be in the past");
            }
            if (t.gtNow()) {
                prose.add("must be in the future");
            }
            t.within().ifPresent(d -> prose.add("must be within " + d + " of the current time"));
        });
        constraints.duration().ifPresent(d -> {
            d.constant().ifPresent(c -> prose.add("must equal " + c));
            d.gt().ifPresent(c -> prose.add("must be longer than " + c));
            d.gte().ifPresent(c -> prose.add("must be at least " + c));
            d.lt().ifPresent(c -> prose.add("must be shorter than " + c));
            d.lte().ifPresent(c -> prose.add("must be at most " + c));
        });
        constraints.any().ifPresent(a -> {
            if (!a.in().isEmpty()) {
                prose.add("must pack one of these types: " + String.join(", ", a.in()));
            }
            if (!a.notIn().isEmpty()) {
                prose.add("must not pack any of these types: " + String.join(", ", a.notIn()));
            }
        });
        constraints.fieldMask().ifPresent(m -> {
            if (!m.in().isEmpty()) {
                prose.add("paths must be among: " + String.join(", ", m.in()));
            }
            if (!m.notIn().isEmpty()) {
                prose.add("paths must not include: " + String.join(", ", m.notIn()));
            }
        });
        for (CelConstraint cel : constraints.cel()) {
            prose.add(celProse(cel));
        }
        return prose;
    }

    private static void stringProse(List<String> prose, StringConstraints s) {
        s.constant().ifPresent(c -> prose.add("must equal \"" + c + "\""));
        s.len().ifPresent(n -> prose.add("must be exactly " + n + " characters"));
        s.minLen().ifPresent(n -> prose.add("must be at least " + n + " characters"));
        s.maxLen().ifPresent(n -> prose.add("must be at most " + n + " characters"));
        s.pattern().ifPresent(p -> prose.add("must match /" + p + "/"));
        s.prefix().ifPresent(p -> prose.add("must start with \"" + p + "\""));
        s.suffix().ifPresent(p -> prose.add("must end with \"" + p + "\""));
        s.contains().ifPresent(c -> prose.add("must contain \"" + c + "\""));
        s.notContains().ifPresent(c -> prose.add("must not contain \"" + c + "\""));
        if (!s.in().isEmpty()) {
            prose.add("must be one of: " + joinQuoted(s.in()));
        }
        if (!s.notIn().isEmpty()) {
            prose.add("must not be any of: " + joinQuoted(s.notIn()));
        }
        for (var format : s.formats()) {
            prose.add("must be a valid "
                    + format.name().toLowerCase(Locale.ROOT).replace('_', ' '));
        }
    }

    private static <T extends Number> void numericProse(List<String> prose,
            Optional<T> constant, Optional<T> gt, Optional<T> gte, Optional<T> lt,
            Optional<T> lte, List<T> in, List<T> notIn, String ruleIdPrefix,
            FieldDescriptor field, Boolean finite) {
        constant.ifPresent(c -> prose.add("must equal " + c));
        gt.ifPresent(c -> prose.add("must be greater than " + c));
        gte.ifPresent(c -> prose.add("must be at least " + c));
        lt.ifPresent(c -> prose.add("must be less than " + c));
        lte.ifPresent(c -> prose.add("must be at most " + c));
        if (!in.isEmpty()) {
            prose.add("must be one of: " + joinNumbers(field, in));
        }
        if (!notIn.isEmpty()) {
            prose.add("must not be any of: " + joinNumbers(field, notIn));
        }
        if (Boolean.TRUE.equals(finite)) {
            prose.add("must be finite (not NaN or infinite)");
        }
    }

    private static <T extends Number> String joinNumbers(FieldDescriptor field, List<T> values) {
        if (field.getType() == FieldDescriptor.Type.ENUM) {
            List<String> names = new ArrayList<>(values.size());
            for (T value : values) {
                names.add(enumName(field, value.intValue()));
            }
            return String.join(", ", names);
        }
        List<String> parts = new ArrayList<>(values.size());
        for (T value : values) {
            parts.add(value.toString());
        }
        return String.join(", ", parts);
    }

    private static String enumName(FieldDescriptor field, int number) {
        EnumValueDescriptor value = field.getEnumType().findValueByNumber(number);
        return value != null ? value.getName() : "UNKNOWN(" + number + ")";
    }

    private static String enumVocabulary(FieldDescriptor field) {
        List<String> names = new ArrayList<>();
        for (EnumValueDescriptor value : field.getEnumType().getValues()) {
            if (value.getNumber() == 0 && value.getName().endsWith("_UNSPECIFIED")) {
                names.add(value.getName() + " (means unknown)");
            } else {
                names.add(value.getName());
            }
        }
        return String.join(", ", names);
    }

    private static String joinEnums(FieldDescriptor field, List<Integer> numbers) {
        List<String> names = new ArrayList<>(numbers.size());
        for (int number : numbers) {
            names.add(enumName(field, number));
        }
        return String.join(", ", names);
    }

    private static String joinQuoted(List<String> values) {
        List<String> quoted = new ArrayList<>(values.size());
        for (String value : values) {
            quoted.add("\"" + value + "\"");
        }
        return String.join(", ", quoted);
    }

    private static String celProse(CelConstraint cel) {
        if (!cel.message().isEmpty()) {
            return "rule " + cel.id() + ": " + cel.message();
        }
        return "rule " + cel.id() + ": must satisfy `" + cel.expression() + "`";
    }

    private static void appendQuality(StringBuilder out, Descriptor descriptor) {
        var options = descriptor.getOptions();
        if (!options.hasExtension(QualityProto.quality)) {
            return;
        }
        QualityRules rules = options.getExtension(QualityProto.quality);
        if (rules.getDimensionList().isEmpty()) {
            return;
        }
        List<String> dimensions = new ArrayList<>(rules.getDimensionCount());
        for (var dimension : rules.getDimensionList()) {
            double weight = dimension.getWeight() == 0 ? 1.0 : dimension.getWeight();
            dimensions.add(dimension.getId() + " (weight " + weight + "): `" + dimension.getCel() + "`");
        }
        appendBullets(out, "\nYour fill will be scored on:", dimensions);
    }

    private static String renderOverrides(RenderPromptRequest request, TypeRegistry typeRegistry) {
        String typeUrl = request.getOverrides().getTypeUrl();
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        Descriptor overrideType = typeRegistry.find(typeName);
        if (overrideType == null) {
            throw new PromptRenderException("cannot resolve overrides type '" + typeUrl
                    + "' in the supplied TypeRegistry");
        }
        try {
            DynamicMessage overrides = DynamicMessage.parseFrom(
                    overrideType, request.getOverrides().getValue());
            return JsonFormat.printer().usingTypeRegistry(typeRegistry).print(overrides);
        } catch (Exception e) {
            throw new PromptRenderException(
                    "cannot decode overrides of type '" + typeUrl + "': " + e.getMessage());
        }
    }

    private static String typeText(FieldDescriptor field) {
        if (field.isMapField()) {
            return "map<" + typeText(field.getMessageType().findFieldByName("key")) + ", "
                    + typeText(field.getMessageType().findFieldByName("value")) + ">";
        }
        String base = switch (field.getType()) {
            case MESSAGE -> field.getMessageType().getFullName();
            case ENUM -> field.getEnumType().getFullName();
            case GROUP -> "group";
            default -> field.getType().name().toLowerCase(Locale.ROOT);
        };
        return field.isRepeated() ? "repeated " + base : base;
    }

    private static void appendBullets(StringBuilder out, String heading, List<String> items) {
        appendBullets(out, heading, items, "");
    }

    private static void appendBullets(StringBuilder out, String heading, List<String> items,
            String indent) {
        if (items.isEmpty()) {
            return;
        }
        out.append(indent).append(heading).append('\n');
        for (String item : items) {
            out.append(indent).append("- ").append(item).append('\n');
        }
    }
}
