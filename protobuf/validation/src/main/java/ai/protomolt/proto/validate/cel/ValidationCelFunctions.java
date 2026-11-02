package ai.protomolt.proto.validate.cel;

import ai.protomolt.proto.formats.Formats;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.SimpleType;
import dev.cel.runtime.CelFunctionBinding;

import java.util.List;

/**
 * The CEL standard-library format functions protovalidate expects on the receiver type — member
 * calls such as {@code this.isHostname()}, {@code this.isIp(4)}, {@code this.isUri()}. Declarations
 * feed the compiler; bindings feed the runtime. Both are registered into the validation CEL
 * environment via {@link ai.protomolt.proto.cel.CelEnvironmentFactory#addFunctions}.
 *
 * <p>All semantics come from the dependency-free {@link Formats} validators, so the same
 * RFC-accurate logic backs both these CEL functions and any direct use of the formats library.
 *
 * <p>{@code <string>.format(<list>)} is not declared here. CEL's {@code strings} extension (which
 * {@link ai.protomolt.proto.cel.CelEnvironmentFactory} registers) has supplied it since cel-java
 * 0.15.0, with the CEL-spec semantics protovalidate's message templates are written against.
 */
public final class ValidationCelFunctions {

    private ValidationCelFunctions() {
    }

    /** Function declarations (name + typed overloads) for the CEL compiler. */
    public static List<CelFunctionDecl> declarations() {
        return List.of(
                CelFunctionDecl.newFunctionDeclaration("isHostname",
                        CelOverloadDecl.newMemberOverload("is_hostname", SimpleType.BOOL, SimpleType.STRING)),
                CelFunctionDecl.newFunctionDeclaration("isEmail",
                        CelOverloadDecl.newMemberOverload("is_email", SimpleType.BOOL, SimpleType.STRING)),
                CelFunctionDecl.newFunctionDeclaration("isIp",
                        CelOverloadDecl.newMemberOverload("is_ip_unary", SimpleType.BOOL, SimpleType.STRING),
                        CelOverloadDecl.newMemberOverload("is_ip", SimpleType.BOOL, SimpleType.STRING, SimpleType.INT)),
                CelFunctionDecl.newFunctionDeclaration("isIpPrefix",
                        CelOverloadDecl.newMemberOverload("is_ip_prefix", SimpleType.BOOL, SimpleType.STRING),
                        CelOverloadDecl.newMemberOverload("is_ip_prefix_int", SimpleType.BOOL, SimpleType.STRING, SimpleType.INT),
                        CelOverloadDecl.newMemberOverload("is_ip_prefix_bool", SimpleType.BOOL, SimpleType.STRING, SimpleType.BOOL),
                        CelOverloadDecl.newMemberOverload("is_ip_prefix_int_bool", SimpleType.BOOL, SimpleType.STRING, SimpleType.INT, SimpleType.BOOL)),
                CelFunctionDecl.newFunctionDeclaration("isUri",
                        CelOverloadDecl.newMemberOverload("is_uri", SimpleType.BOOL, SimpleType.STRING)),
                CelFunctionDecl.newFunctionDeclaration("isUriRef",
                        CelOverloadDecl.newMemberOverload("is_uri_ref", SimpleType.BOOL, SimpleType.STRING)),
                CelFunctionDecl.newFunctionDeclaration("isHostAndPort",
                        CelOverloadDecl.newMemberOverload("is_host_and_port", SimpleType.BOOL, SimpleType.STRING, SimpleType.BOOL)),
                CelFunctionDecl.newFunctionDeclaration("isNan",
                        CelOverloadDecl.newMemberOverload("is_nan", SimpleType.BOOL, SimpleType.DOUBLE)),
                CelFunctionDecl.newFunctionDeclaration("isInf",
                        CelOverloadDecl.newMemberOverload("is_inf_unary", SimpleType.BOOL, SimpleType.DOUBLE),
                        CelOverloadDecl.newMemberOverload("is_inf_binary", SimpleType.BOOL, SimpleType.DOUBLE, SimpleType.INT)));
    }

    /** Runtime overload bindings, keyed by the overload ids used in {@link #declarations()}. */
    public static List<CelFunctionBinding> bindings() {
        return List.of(
                CelFunctionBinding.from("is_hostname", String.class, Formats::isHostname),
                CelFunctionBinding.from("is_email", String.class, Formats::isEmail),
                CelFunctionBinding.from("is_ip_unary", String.class, s -> Formats.isIp(s, 0)),
                CelFunctionBinding.from("is_ip", String.class, Long.class, Formats::isIp),
                CelFunctionBinding.from("is_ip_prefix", String.class, s -> Formats.isIpPrefix(s, 0, false)),
                CelFunctionBinding.from("is_ip_prefix_int", String.class, Long.class,
                        (s, version) -> Formats.isIpPrefix(s, version, false)),
                CelFunctionBinding.from("is_ip_prefix_bool", String.class, Boolean.class,
                        (s, strict) -> Formats.isIpPrefix(s, 0, strict)),
                CelFunctionBinding.from("is_ip_prefix_int_bool",
                        List.<Class<?>>of(String.class, Long.class, Boolean.class),
                        args -> Formats.isIpPrefix((String) args[0], (Long) args[1], (Boolean) args[2])),
                CelFunctionBinding.from("is_uri", String.class, Formats::isUri),
                CelFunctionBinding.from("is_uri_ref", String.class, Formats::isUriRef),
                CelFunctionBinding.from("is_host_and_port", String.class, Boolean.class, Formats::isHostAndPort),
                CelFunctionBinding.from("is_nan", Double.class, (Double d) -> Double.isNaN(d)),
                CelFunctionBinding.from("is_inf_unary", Double.class, (Double d) -> Double.isInfinite(d)),
                CelFunctionBinding.from("is_inf_binary", Double.class, Long.class,
                        ValidationCelFunctions::isInf));
    }

    /** {@code isInf(value, sign)}: sign &gt; 0 tests +∞, sign &lt; 0 tests −∞, sign == 0 tests either. */
    private static boolean isInf(double value, long sign) {
        if (sign > 0) {
            return value == Double.POSITIVE_INFINITY;
        }
        if (sign < 0) {
            return value == Double.NEGATIVE_INFINITY;
        }
        return Double.isInfinite(value);
    }
}
