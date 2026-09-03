package ai.protomolt.proto.quality;

import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.SimpleType;
import dev.cel.runtime.CelFunctionBinding;

import java.util.Arrays;
import java.util.List;

/**
 * The scoring helpers quality expressions get on top of the CEL standard library:
 * {@code exp(x)} for decay curves (recency, staleness) and {@code clamp(x, lo, hi)} for keeping
 * a hand-built formula inside the score range explicitly.
 */
final class QualityCelFunctions {

    private QualityCelFunctions() {
    }

    static List<CelFunctionDecl> declarations() {
        return List.of(
                CelFunctionDecl.newFunctionDeclaration("exp",
                        CelOverloadDecl.newGlobalOverload(
                                "exp_double", SimpleType.DOUBLE, SimpleType.DOUBLE)),
                CelFunctionDecl.newFunctionDeclaration("clamp",
                        CelOverloadDecl.newGlobalOverload(
                                "clamp_double", SimpleType.DOUBLE,
                                SimpleType.DOUBLE, SimpleType.DOUBLE, SimpleType.DOUBLE)));
    }

    static List<CelFunctionBinding> bindings() {
        return List.of(
                CelFunctionBinding.from("exp_double", Double.class, Math::exp),
                CelFunctionBinding.from("clamp_double",
                        Arrays.asList(Double.class, Double.class, Double.class),
                        args -> {
                            double value = (Double) args[0];
                            double lo = (Double) args[1];
                            double hi = (Double) args[2];
                            return Math.max(lo, Math.min(hi, value));
                        }));
    }
}
