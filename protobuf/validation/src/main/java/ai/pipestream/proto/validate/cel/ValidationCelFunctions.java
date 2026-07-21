package ai.pipestream.proto.validate.cel;

import ai.pipestream.format.Formats;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.ListType;
import dev.cel.common.types.SimpleType;
import dev.cel.runtime.CelFunctionBinding;

import java.util.List;

/**
 * The CEL standard-library format functions protovalidate expects on the receiver type — member
 * calls such as {@code this.isHostname()}, {@code this.isIp(4)}, {@code this.isUri()}. Declarations
 * feed the compiler; bindings feed the runtime. Both are registered into the validation CEL
 * environment via {@link ai.pipestream.proto.cel.CelEnvironmentFactory#addFunctions}.
 *
 * <p>All semantics come from the dependency-free {@link Formats} validators, so the same
 * RFC-accurate logic backs both these CEL functions and any direct use of the formats library.
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
                        CelOverloadDecl.newMemberOverload("is_inf_binary", SimpleType.BOOL, SimpleType.DOUBLE, SimpleType.INT)),
                CelFunctionDecl.newFunctionDeclaration("format",
                        CelOverloadDecl.newMemberOverload("format_list", SimpleType.STRING,
                                SimpleType.STRING, ListType.create(SimpleType.DYN))));
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
                        ValidationCelFunctions::isInf),
                CelFunctionBinding.from("format_list", String.class, List.class,
                        (format, args) -> formatString(format, args)));
    }

    /**
     * protovalidate's {@code <string>.format(<list>)} — a printf-style templating function that is
     * not part of the CEL standard library. Substitutes {@code %s}/{@code %d}/{@code %f}/{@code %x}/
     * {@code %X}/{@code %o}/{@code %b}/{@code %e}/{@code %%} directives with the argument list, in
     * order. Precision ({@code %.Nf}) is honored for floating directives.
     */
    static String formatString(String format, List<?> args) {
        StringBuilder out = new StringBuilder(format.length());
        int arg = 0;
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c != '%') {
                out.append(c);
                continue;
            }
            if (i + 1 < format.length() && format.charAt(i + 1) == '%') {
                out.append('%');
                i++;
                continue;
            }
            // Optional precision: %.Nf / %.Ne
            int precision = -1;
            int j = i + 1;
            if (j < format.length() && format.charAt(j) == '.') {
                int start = ++j;
                while (j < format.length() && Character.isDigit(format.charAt(j))) {
                    j++;
                }
                if (j > start) {
                    precision = Integer.parseInt(format.substring(start, j));
                }
            }
            if (j >= format.length()) {
                out.append('%');
                break;
            }
            char verb = format.charAt(j);
            String rendered = formatVerb(verb, arg < args.size() ? args.get(arg) : null, precision);
            if (rendered == null) {
                // An unrecognised verb is copied through as written and consumes no argument;
                // consuming one would substitute every later directive an argument off.
                out.append('%').append(verb);
            } else {
                arg++;
                out.append(rendered);
            }
            i = j;
        }
        return out.toString();
    }

    /** The rendering of one directive, or null when {@code verb} is not a known directive. */
    private static String formatVerb(char verb, Object value, int precision) {
        return switch (verb) {
            case 's' -> celString(value);
            // UnsignedLong renders through its own toString; Number.longValue() would go negative
            // above Long.MAX_VALUE. (%o/%b/%x are bit-pattern renderings and stay unsigned.)
            case 'd' -> value instanceof UnsignedLong u ? u.toString() : String.valueOf(toLong(value));
            case 'f' -> String.format("%." + (precision < 0 ? 6 : precision) + "f", toDouble(value));
            case 'e' -> String.format("%." + (precision < 0 ? 6 : precision) + "e", toDouble(value));
            case 'x' -> hex(value, false);
            case 'X' -> hex(value, true);
            case 'o' -> Long.toOctalString(toLong(value));
            case 'b' -> Long.toBinaryString(toLong(value));
            default -> null;
        };
    }

    /** The CEL string form of a value, as {@code %s} produces it. */
    private static String celString(Object value) {
        if (value instanceof ByteString bytes) {
            return bytes.toStringUtf8();
        }
        if (value instanceof Double d) {
            if (d == Math.floor(d) && !d.isInfinite()) {
                // Whole values print without a fraction; beyond long range (where longValue()
                // would saturate) they switch to CEL's exponent form instead.
                return Math.abs(d) < 0x1p63 ? String.valueOf(d.longValue()) : exponentForm(d);
            }
            return d.toString();
        }
        return String.valueOf(value);
    }

    /** Go/CEL-style exponent form for a large whole double, e.g. {@code 1e+100}. */
    private static String exponentForm(double d) {
        String s = Double.toString(d); // always E-notation at this magnitude, e.g. "1.0E100"
        int e = s.indexOf('E');
        String mantissa = s.substring(0, e);
        if (mantissa.endsWith(".0")) {
            mantissa = mantissa.substring(0, mantissa.length() - 2);
        }
        return mantissa + "e+" + s.substring(e + 1);
    }

    private static long toLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static double toDouble(Object value) {
        return value instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(value));
    }

    private static String hex(Object value, boolean upper) {
        String hex;
        if (value instanceof ByteString bytes) {
            StringBuilder sb = new StringBuilder(bytes.size() * 2);
            for (byte b : bytes.toByteArray()) {
                sb.append(String.format("%02x", b));
            }
            hex = sb.toString();
        } else if (value instanceof String s) {
            StringBuilder sb = new StringBuilder(s.length() * 2);
            for (byte b : s.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
                sb.append(String.format("%02x", b));
            }
            hex = sb.toString();
        } else {
            hex = Long.toHexString(toLong(value));
        }
        return upper ? hex.toUpperCase(java.util.Locale.ROOT) : hex;
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
