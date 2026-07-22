package ai.pipestream.proto.cel;

import com.google.protobuf.Descriptors.Descriptor;
import dev.cel.bundle.Cel;
import dev.cel.bundle.CelBuilder;
import dev.cel.bundle.CelFactory;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.types.CelType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructTypeReference;
import dev.cel.extensions.CelExtensions;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelFunctionBinding;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Builds CEL environments for protobuf descriptors and application bindings. */
public final class CelEnvironmentFactory {
    private final List<Descriptor> messageTypes = new ArrayList<>();
    private final Map<String, CelType> variables = new LinkedHashMap<>();
    private final List<CelFunctionDecl> functionDeclarations = new ArrayList<>();
    private final List<CelFunctionBinding> functionBindings = new ArrayList<>();

    public CelEnvironmentFactory() {
    }

    public static CelEnvironmentFactory builder() {
        return new CelEnvironmentFactory();
    }

    public CelEnvironmentFactory addMessageType(Descriptor descriptor) {
        messageTypes.add(Objects.requireNonNull(descriptor, "descriptor"));
        return this;
    }

    public CelEnvironmentFactory addVar(String name) {
        return addVar(name, SimpleType.DYN);
    }

    public CelEnvironmentFactory addVar(String name, CelType type) {
        variables.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(type, "type"));
        return this;
    }

    /**
     * Declares {@code name} with the concrete type of {@code descriptor} (and registers the type),
     * so field access like {@code this.foo} is type-checked at compile time. Overrides any prior
     * declaration of {@code name}.
     */
    public CelEnvironmentFactory addMessageVar(String name, Descriptor descriptor) {
        addMessageType(descriptor);
        return addVar(name, StructTypeReference.create(descriptor.getFullName()));
    }

    /**
     * Registers a custom function library: the declarations are added to the compiler and the
     * bindings to the runtime, so callers can extend the environment with domain-specific functions
     * (e.g. the validation format functions) without this module owning their semantics.
     */
    public CelEnvironmentFactory addFunctions(
            Iterable<CelFunctionDecl> declarations, Iterable<CelFunctionBinding> bindings) {
        declarations.forEach(functionDeclarations::add);
        bindings.forEach(functionBindings::add);
        return this;
    }

    /** Returns a builder for callers that need additional typed CEL declarations. */
    public CelBuilder advisoryBuilder() {
        CelBuilder builder = CelFactory.standardCelBuilder();
        builder.setStandardMacros(CelStandardMacro.STANDARD_MACROS);
        // The strings extension supplies format()/CharAt/indexOf etc. that protovalidate rules use.
        builder.addCompilerLibraries(CelExtensions.strings());
        builder.addRuntimeLibraries(CelExtensions.strings());
        builder.addMessageTypes(messageTypes);
        variables.forEach(builder::addVar);
        if (!functionDeclarations.isEmpty()) {
            builder.addFunctionDeclarations(functionDeclarations);
        }
        if (!functionBindings.isEmpty()) {
            builder.addFunctionBindings(functionBindings);
        }
        return builder;
    }

    public Cel build() {
        return advisoryBuilder().build();
    }
}
