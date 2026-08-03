package ai.pipestream.proto.sources;

import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.GroupElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.RpcElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Renders a {@code .proto} source with every option statement removed, preserving everything
 * else (structure, defaults, json names, documentation).
 *
 * <p>This is the front half of the compiler's option strategy. Wire's {@code SchemaEncoder}
 * cannot be trusted with options at all: it keys an intermediate map by the option field's
 * <em>simple</em> name (colliding same-named extension families), and its option value coercion
 * has no map branch — any map-valued option (for example {@code labels} on the meta annotation
 * family) aborts the whole encode with {@code "not implemented: map<...>"}. The encoder is
 * therefore fed only option-free structure, and {@link LinkedOptionsRepair} re-encodes every
 * option payload from the original linked model.</p>
 *
 * <p>The rewrite is parser-driven, never textual: the source is parsed with Wire's
 * {@link ProtoParser}, option lists are dropped from the resulting element tree, and the tree is
 * rendered back with {@code toSchema()}. A parse failure is loud and names the source.</p>
 */
final class OptionStrippingRewriter {

    private OptionStrippingRewriter() {
    }

    /**
     * Returns {@code content} with every option statement removed.
     *
     * @param path source path, used for error messages and the parse location
     * @throws ProtoCompilationException if the source does not parse or the element tree holds
     *     a shape this rewriter does not know
     */
    static String strip(String path, String content) throws ProtoCompilationException {
        ProtoFileElement parsed;
        try {
            parsed = ProtoParser.Companion.parse(Location.Companion.get(path), content);
        } catch (RuntimeException e) {
            throw new ProtoCompilationException("Failed to parse " + path + " for option stripping"
                    + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
        }
        return stripFile(parsed).toSchema();
    }

    private static ProtoFileElement stripFile(ProtoFileElement file) throws ProtoCompilationException {
        List<TypeElement> types = new ArrayList<>();
        for (TypeElement type : file.getTypes()) {
            types.add(stripType(type));
        }
        return new ProtoFileElement(file.getLocation(), file.getPackageName(), file.getSyntax(),
                file.getImports(), file.getPublicImports(), file.getWeakImports(),
                types, stripServices(file.getServices()), stripExtends(file.getExtendDeclarations()),
                List.of());
    }

    private static TypeElement stripType(TypeElement type) throws ProtoCompilationException {
        if (type instanceof MessageElement message) {
            return stripMessage(message);
        }
        if (type instanceof EnumElement enumElement) {
            return stripEnum(enumElement);
        }
        throw new ProtoCompilationException("unknown type element " + type.getClass().getName()
                + " (" + type.getName() + ") — extend the option-stripping rewriter");
    }

    private static MessageElement stripMessage(MessageElement message) throws ProtoCompilationException {
        List<TypeElement> nested = new ArrayList<>();
        for (TypeElement type : message.getNestedTypes()) {
            nested.add(stripType(type));
        }
        return new MessageElement(message.getLocation(), message.getName(), message.getDocumentation(),
                nested, List.of(), message.getReserveds(), stripFields(message.getFields()),
                stripOneOfs(message.getOneOfs()), message.getExtensions(),
                stripGroups(message.getGroups()), stripExtends(message.getExtendDeclarations()));
    }

    private static EnumElement stripEnum(EnumElement enumElement) {
        List<EnumConstantElement> constants = new ArrayList<>();
        for (EnumConstantElement constant : enumElement.getConstants()) {
            constants.add(new EnumConstantElement(constant.getLocation(), constant.getName(),
                    constant.getTag(), constant.getDocumentation(), List.of()));
        }
        return new EnumElement(enumElement.getLocation(), enumElement.getName(),
                enumElement.getDocumentation(), structuralEnumOptions(enumElement),
                constants, enumElement.getReserveds());
    }

    /**
     * {@code allow_alias} is the one enum option that changes what links: without it the
     * stripped schema rejects duplicate constant tags. It stays; everything else is an option
     * payload and is restored from the linked model by the repair pass.
     */
    private static List<OptionElement> structuralEnumOptions(EnumElement enumElement) {
        List<OptionElement> kept = new ArrayList<>();
        for (OptionElement option : enumElement.getOptions()) {
            if (option.getName().equals("allow_alias")) {
                kept.add(option);
            }
        }
        return kept;
    }

    private static List<ServiceElement> stripServices(List<ServiceElement> services) {
        List<ServiceElement> stripped = new ArrayList<>();
        for (ServiceElement service : services) {
            List<RpcElement> rpcs = new ArrayList<>();
            for (RpcElement rpc : service.getRpcs()) {
                rpcs.add(new RpcElement(rpc.getLocation(), rpc.getName(), rpc.getDocumentation(),
                        rpc.getRequestType(), rpc.getResponseType(),
                        rpc.getRequestStreaming(), rpc.getResponseStreaming(), List.of()));
            }
            stripped.add(new ServiceElement(service.getLocation(), service.getName(),
                    service.getDocumentation(), rpcs, List.of()));
        }
        return stripped;
    }

    private static List<ExtendElement> stripExtends(List<ExtendElement> extendsList) {
        List<ExtendElement> stripped = new ArrayList<>();
        for (ExtendElement extend : extendsList) {
            stripped.add(new ExtendElement(extend.getLocation(), extend.getName(),
                    extend.getDocumentation(), stripFields(extend.getFields())));
        }
        return stripped;
    }

    private static List<OneOfElement> stripOneOfs(List<OneOfElement> oneOfs) {
        List<OneOfElement> stripped = new ArrayList<>();
        for (OneOfElement oneOf : oneOfs) {
            stripped.add(new OneOfElement(oneOf.getName(), oneOf.getDocumentation(),
                    stripFields(oneOf.getFields()), stripGroups(oneOf.getGroups()), List.of(),
                    oneOf.getLocation()));
        }
        return stripped;
    }

    private static List<GroupElement> stripGroups(List<GroupElement> groups) {
        List<GroupElement> stripped = new ArrayList<>();
        for (GroupElement group : groups) {
            stripped.add(new GroupElement(group.getLabel(), group.getLocation(), group.getName(),
                    group.getTag(), group.getDocumentation(), stripFields(group.getFields())));
        }
        return stripped;
    }

    private static List<FieldElement> stripFields(List<FieldElement> fields) {
        List<FieldElement> stripped = new ArrayList<>();
        for (FieldElement field : fields) {
            stripped.add(new FieldElement(field.getLocation(), field.getLabel(), field.getType(),
                    field.getName(), field.getDefaultValue(), field.getJsonName(), field.getTag(),
                    field.getDocumentation(), List.of()));
        }
        return stripped;
    }
}
