package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.ApiStabilityResolver;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic snapshot of exported Java API declarations and their resolved
 * {@link StabilityTier}s.
 *
 * @param classes sorted list of captured Java type declarations
 */
@ApiStability(StabilityTier.STABLE)
public record JavaApiSnapshot(List<ClassEntry> classes) {

    public JavaApiSnapshot {
        Objects.requireNonNull(classes, "classes");
        List<ClassEntry> copy = new ArrayList<>(classes);
        copy.sort(Comparator.comparing(ClassEntry::className));
        classes = List.copyOf(copy);
    }

    /**
     * Indexes the captured classes by their binary/canonical class name.
     *
     * @return immutable map of class name to {@link ClassEntry}
     */
    public Map<String, ClassEntry> classesByName() {
        Map<String, ClassEntry> map = new LinkedHashMap<>();
        for (ClassEntry entry : classes) {
            map.put(entry.className(), entry);
        }
        return Map.copyOf(map);
    }

    /**
     * Captures the exported API snapshot of the supplied classes using
     * {@link ApiStabilityResolver#DEFAULT_UNANNOTATED_TIER} as the fallback tier.
     *
     * @param types classes to inspect
     * @return deterministic Java API snapshot
     */
    public static JavaApiSnapshot capture(Class<?>... types) {
        return capture(Arrays.asList(types), ApiStabilityResolver.DEFAULT_UNANNOTATED_TIER);
    }

    /**
     * Captures the exported API snapshot of the supplied classes.
     *
     * @param types        classes to inspect
     * @param fallbackTier fallback stability tier when a declaration has no explicit annotation
     * @return deterministic Java API snapshot
     */
    public static JavaApiSnapshot capture(Collection<Class<?>> types, StabilityTier fallbackTier) {
        Objects.requireNonNull(types, "types");
        Objects.requireNonNull(fallbackTier, "fallbackTier");
        List<ClassEntry> entries = new ArrayList<>();
        for (Class<?> type : types) {
            entries.add(captureClass(type, fallbackTier));
        }
        return new JavaApiSnapshot(entries);
    }

    /**
     * Captures a single Java class declaration and its public/protected members.
     *
     * @param type         the class to inspect
     * @param fallbackTier fallback stability tier when unannotated
     * @return the captured class entry
     */
    public static ClassEntry captureClass(Class<?> type, StabilityTier fallbackTier) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(fallbackTier, "fallbackTier");

        int classMods = type.getModifiers();
        TypeKind kind = resolveKind(type);
        Visibility visibility = Visibility.fromModifiers(classMods);
        StabilityTier classStability = ApiStabilityResolver.resolve(type, fallbackTier);
        boolean finalType = Modifier.isFinal(classMods) && kind == TypeKind.CLASS;
        boolean abstractType = Modifier.isAbstract(classMods) && kind == TypeKind.CLASS;

        String superclass = "";
        if (type.getSuperclass() != null
                && type.getSuperclass() != Object.class
                && kind == TypeKind.CLASS) {
            superclass = typeName(type.getSuperclass());
        }

        List<String> interfaces = new ArrayList<>();
        for (Class<?> iface : type.getInterfaces()) {
            interfaces.add(typeName(iface));
        }
        interfaces.sort(Comparator.naturalOrder());

        List<ConstructorEntry> constructors = new ArrayList<>();
        for (Constructor<?> ctor : type.getDeclaredConstructors()) {
            if (ctor.isSynthetic()) {
                continue;
            }
            Visibility ctorVis = Visibility.fromModifiers(ctor.getModifiers());
            if (!ctorVis.isExported()) {
                continue;
            }
            List<String> params = new ArrayList<>();
            for (Class<?> param : ctor.getParameterTypes()) {
                params.add(typeName(param));
            }
            constructors.add(new ConstructorEntry(
                    params,
                    ctorVis,
                    ApiStabilityResolver.resolve(ctor, classStability)));
        }

        List<MethodEntry> methods = new ArrayList<>();
        for (Method method : type.getDeclaredMethods()) {
            if (method.isSynthetic() || method.isBridge()) {
                continue;
            }
            int mods = method.getModifiers();
            Visibility methodVis = Visibility.fromModifiers(mods);
            if (!methodVis.isExported()) {
                continue;
            }
            List<String> params = new ArrayList<>();
            for (Class<?> param : method.getParameterTypes()) {
                params.add(typeName(param));
            }
            methods.add(new MethodEntry(
                    method.getName(),
                    params,
                    typeName(method.getReturnType()),
                    methodVis,
                    Modifier.isStatic(mods),
                    Modifier.isFinal(mods),
                    Modifier.isAbstract(mods) && kind != TypeKind.INTERFACE && kind != TypeKind.ANNOTATION,
                    method.isDefault(),
                    ApiStabilityResolver.resolve(method, classStability)));
        }

        List<FieldEntry> fields = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (field.isSynthetic()) {
                continue;
            }
            int mods = field.getModifiers();
            Visibility fieldVis = Visibility.fromModifiers(mods);
            if (!fieldVis.isExported()) {
                continue;
            }
            fields.add(new FieldEntry(
                    field.getName(),
                    typeName(field.getType()),
                    fieldVis,
                    Modifier.isStatic(mods),
                    Modifier.isFinal(mods),
                    ApiStabilityResolver.resolve(field, classStability)));
        }

        return new ClassEntry(
                typeName(type),
                kind,
                visibility,
                finalType,
                abstractType,
                superclass,
                interfaces,
                classStability,
                constructors,
                methods,
                fields);
    }

    private static TypeKind resolveKind(Class<?> type) {
        if (type.isAnnotation()) {
            return TypeKind.ANNOTATION;
        }
        if (type.isEnum()) {
            return TypeKind.ENUM;
        }
        if (type.isRecord()) {
            return TypeKind.RECORD;
        }
        if (type.isInterface()) {
            return TypeKind.INTERFACE;
        }
        return TypeKind.CLASS;
    }

    private static String typeName(Class<?> type) {
        if (type.isArray()) {
            return typeName(type.getComponentType()) + "[]";
        }
        String canonical = type.getCanonicalName();
        return canonical != null ? canonical : type.getName();
    }

    /** Java type kind. */
    @ApiStability(StabilityTier.STABLE)
    public enum TypeKind {
        CLASS,
        INTERFACE,
        ENUM,
        RECORD,
        ANNOTATION
    }

    /** Java declaration visibility. */
    @ApiStability(StabilityTier.STABLE)
    public enum Visibility {
        PUBLIC(3, true),
        PROTECTED(2, true),
        PACKAGE_PRIVATE(1, false),
        PRIVATE(0, false);

        private final int rank;
        private final boolean exported;

        Visibility(int rank, boolean exported) {
            this.rank = rank;
            this.exported = exported;
        }

        /** Relative visibility rank (higher means broader access). */
        public int rank() {
            return rank;
        }

        /** Whether this visibility level is part of the exported API surface. */
        public boolean isExported() {
            return exported;
        }

        /** Resolves visibility from {@link Modifier} bits. */
        public static Visibility fromModifiers(int modifiers) {
            if (Modifier.isPublic(modifiers)) {
                return PUBLIC;
            }
            if (Modifier.isProtected(modifiers)) {
                return PROTECTED;
            }
            if (Modifier.isPrivate(modifiers)) {
                return PRIVATE;
            }
            return PACKAGE_PRIVATE;
        }
    }

    /**
     * Snapshot of a single Java class, interface, enum, record, or annotation type.
     */
    @ApiStability(StabilityTier.STABLE)
    public record ClassEntry(
            String className,
            TypeKind kind,
            Visibility visibility,
            boolean finalType,
            boolean abstractType,
            String superclass,
            List<String> interfaces,
            StabilityTier stability,
            List<ConstructorEntry> constructors,
            List<MethodEntry> methods,
            List<FieldEntry> fields) {

        public ClassEntry {
            Objects.requireNonNull(className, "className");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(visibility, "visibility");
            superclass = superclass == null ? "" : superclass;
            List<String> ifaceCopy = new ArrayList<>(interfaces == null ? List.of() : interfaces);
            ifaceCopy.sort(Comparator.naturalOrder());
            interfaces = List.copyOf(ifaceCopy);
            Objects.requireNonNull(stability, "stability");

            List<ConstructorEntry> ctorCopy = new ArrayList<>(constructors == null ? List.of() : constructors);
            ctorCopy.sort(Comparator.comparing(ConstructorEntry::signatureKey));
            constructors = List.copyOf(ctorCopy);

            List<MethodEntry> methodCopy = new ArrayList<>(methods == null ? List.of() : methods);
            methodCopy.sort(Comparator.comparing(MethodEntry::signatureKey)
                    .thenComparing(MethodEntry::returnType));
            methods = List.copyOf(methodCopy);

            List<FieldEntry> fieldCopy = new ArrayList<>(fields == null ? List.of() : fields);
            fieldCopy.sort(Comparator.comparing(FieldEntry::name));
            fields = List.copyOf(fieldCopy);
        }
    }

    /**
     * Snapshot of an exported constructor.
     */
    @ApiStability(StabilityTier.STABLE)
    public record ConstructorEntry(
            List<String> parameterTypes,
            Visibility visibility,
            StabilityTier stability) {

        public ConstructorEntry {
            parameterTypes = List.copyOf(parameterTypes == null ? List.of() : parameterTypes);
            Objects.requireNonNull(visibility, "visibility");
            Objects.requireNonNull(stability, "stability");
        }

        /** Canonical constructor signature key, e.g. {@code "<init>(java.lang.String,int)"}. */
        public String signatureKey() {
            return "<init>(" + String.join(",", parameterTypes) + ")";
        }
    }

    /**
     * Snapshot of an exported method.
     */
    @ApiStability(StabilityTier.STABLE)
    public record MethodEntry(
            String name,
            List<String> parameterTypes,
            String returnType,
            Visibility visibility,
            boolean staticMethod,
            boolean finalMethod,
            boolean abstractMethod,
            boolean defaultMethod,
            StabilityTier stability) {

        public MethodEntry {
            Objects.requireNonNull(name, "name");
            parameterTypes = List.copyOf(parameterTypes == null ? List.of() : parameterTypes);
            Objects.requireNonNull(returnType, "returnType");
            Objects.requireNonNull(visibility, "visibility");
            Objects.requireNonNull(stability, "stability");
        }

        /** Canonical method overload key, e.g. {@code "check(java.lang.String,int)"}. */
        public String signatureKey() {
            return name + "(" + String.join(",", parameterTypes) + ")";
        }
    }

    /**
     * Snapshot of an exported field.
     */
    @ApiStability(StabilityTier.STABLE)
    public record FieldEntry(
            String name,
            String fieldType,
            Visibility visibility,
            boolean staticField,
            boolean finalField,
            StabilityTier stability) {

        public FieldEntry {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(fieldType, "fieldType");
            Objects.requireNonNull(visibility, "visibility");
            Objects.requireNonNull(stability, "stability");
        }
    }
}
