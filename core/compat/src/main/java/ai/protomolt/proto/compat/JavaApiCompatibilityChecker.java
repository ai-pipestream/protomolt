package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.JavaApiSnapshot.ClassEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.ConstructorEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.FieldEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.MethodEntry;
import ai.protomolt.proto.compat.JavaApiSnapshot.TypeKind;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Evaluates source and binary compatibility between a baseline {@link JavaApiSnapshot} and
 * a current {@link JavaApiSnapshot}.
 */
@ApiStability(StabilityTier.STABLE)
public final class JavaApiCompatibilityChecker {

    public static final String JAVA_TYPE_ADDED = "JAVA_TYPE_ADDED";
    public static final String JAVA_TYPE_REMOVED = "JAVA_TYPE_REMOVED";
    public static final String JAVA_TYPE_KIND_CHANGED = "JAVA_TYPE_KIND_CHANGED";
    public static final String JAVA_TYPE_VISIBILITY_REDUCED = "JAVA_TYPE_VISIBILITY_REDUCED";
    public static final String JAVA_TYPE_FINAL_ADDED = "JAVA_TYPE_FINAL_ADDED";
    public static final String JAVA_TYPE_ABSTRACT_ADDED = "JAVA_TYPE_ABSTRACT_ADDED";
    public static final String JAVA_SUPERTYPE_REMOVED = "JAVA_SUPERTYPE_REMOVED";
    public static final String JAVA_STABILITY_DOWNGRADED = "JAVA_STABILITY_DOWNGRADED";

    public static final String JAVA_CONSTRUCTOR_ADDED = "JAVA_CONSTRUCTOR_ADDED";
    public static final String JAVA_CONSTRUCTOR_REMOVED = "JAVA_CONSTRUCTOR_REMOVED";
    public static final String JAVA_CONSTRUCTOR_VISIBILITY_REDUCED = "JAVA_CONSTRUCTOR_VISIBILITY_REDUCED";

    public static final String JAVA_METHOD_ADDED = "JAVA_METHOD_ADDED";
    public static final String JAVA_ABSTRACT_METHOD_ADDED = "JAVA_ABSTRACT_METHOD_ADDED";
    public static final String JAVA_METHOD_REMOVED = "JAVA_METHOD_REMOVED";
    public static final String JAVA_METHOD_RETURN_TYPE_CHANGED = "JAVA_METHOD_RETURN_TYPE_CHANGED";
    public static final String JAVA_METHOD_VISIBILITY_REDUCED = "JAVA_METHOD_VISIBILITY_REDUCED";
    public static final String JAVA_METHOD_FINAL_ADDED = "JAVA_METHOD_FINAL_ADDED";
    public static final String JAVA_METHOD_STATIC_CHANGED = "JAVA_METHOD_STATIC_CHANGED";
    public static final String JAVA_METHOD_ABSTRACT_ADDED = "JAVA_METHOD_ABSTRACT_ADDED";

    public static final String JAVA_FIELD_ADDED = "JAVA_FIELD_ADDED";
    public static final String JAVA_FIELD_REMOVED = "JAVA_FIELD_REMOVED";
    public static final String JAVA_FIELD_TYPE_CHANGED = "JAVA_FIELD_TYPE_CHANGED";
    public static final String JAVA_FIELD_VISIBILITY_REDUCED = "JAVA_FIELD_VISIBILITY_REDUCED";
    public static final String JAVA_FIELD_FINAL_ADDED = "JAVA_FIELD_FINAL_ADDED";
    public static final String JAVA_FIELD_STATIC_CHANGED = "JAVA_FIELD_STATIC_CHANGED";

    private JavaApiCompatibilityChecker() {
    }

    /**
     * Compares {@code current} against {@code baseline} and returns all detected
     * {@link SurfaceChange}s in deterministic order.
     *
     * @param baseline the baseline Java API snapshot
     * @param current  the current Java API snapshot
     * @return immutable list of detected changes
     */
    public static List<SurfaceChange> compare(JavaApiSnapshot baseline, JavaApiSnapshot current) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");

        Map<String, ClassEntry> oldClasses = baseline.classesByName();
        Map<String, ClassEntry> newClasses = current.classesByName();
        List<SurfaceChange> changes = new ArrayList<>();

        for (ClassEntry oldClass : baseline.classes()) {
            ClassEntry newClass = newClasses.get(oldClass.className());
            if (newClass == null || !newClass.visibility().isExported()) {
                if (newClass != null && newClass.visibility().rank() < oldClass.visibility().rank()) {
                    changes.add(change(
                            JAVA_TYPE_VISIBILITY_REDUCED,
                            oldClass.className(),
                            oldClass.stability(),
                            oldClass.visibility().name(),
                            newClass.visibility().name(),
                            "Type visibility reduced from " + oldClass.visibility() + " to " + newClass.visibility(),
                            true));
                } else {
                    changes.add(change(
                            JAVA_TYPE_REMOVED,
                            oldClass.className(),
                            oldClass.stability(),
                            oldClass.kind().name(),
                            "",
                            "Exported Java type removed: " + oldClass.className(),
                            true));
                }
                continue;
            }
            compareClass(oldClass, newClass, changes);
        }

        for (ClassEntry newClass : current.classes()) {
            if (!oldClasses.containsKey(newClass.className()) && newClass.visibility().isExported()) {
                changes.add(change(
                        JAVA_TYPE_ADDED,
                        newClass.className(),
                        newClass.stability(),
                        "",
                        newClass.kind().name(),
                        "Exported Java type added: " + newClass.className(),
                        false));
            }
        }

        return List.copyOf(changes);
    }

    private static void compareClass(
            ClassEntry oldClass,
            ClassEntry newClass,
            List<SurfaceChange> changes) {
        String className = oldClass.className();
        StabilityTier tier = oldClass.stability();

        if (oldClass.kind() != newClass.kind()) {
            changes.add(change(
                    JAVA_TYPE_KIND_CHANGED,
                    className,
                    tier,
                    oldClass.kind().name(),
                    newClass.kind().name(),
                    "Java type kind changed from " + oldClass.kind() + " to " + newClass.kind(),
                    true));
        }

        if (newClass.visibility().rank() < oldClass.visibility().rank()) {
            changes.add(change(
                    JAVA_TYPE_VISIBILITY_REDUCED,
                    className,
                    tier,
                    oldClass.visibility().name(),
                    newClass.visibility().name(),
                    "Type visibility reduced from " + oldClass.visibility() + " to " + newClass.visibility(),
                    true));
        }

        if (!oldClass.finalType() && newClass.finalType()) {
            changes.add(change(
                    JAVA_TYPE_FINAL_ADDED,
                    className,
                    tier,
                    "non-final",
                    "final",
                    "Non-final Java type became final: " + className,
                    true));
        }

        if (!oldClass.abstractType() && newClass.abstractType() && newClass.kind() == TypeKind.CLASS) {
            changes.add(change(
                    JAVA_TYPE_ABSTRACT_ADDED,
                    className,
                    tier,
                    "concrete",
                    "abstract",
                    "Concrete Java class became abstract: " + className,
                    true));
        }

        if (!oldClass.superclass().isEmpty() && !oldClass.superclass().equals(newClass.superclass())) {
            changes.add(change(
                    JAVA_SUPERTYPE_REMOVED,
                    className,
                    tier,
                    oldClass.superclass(),
                    newClass.superclass(),
                    "Superclass changed or removed on " + className + ": "
                            + oldClass.superclass() + " -> " + newClass.superclass(),
                    true));
        }

        Set<String> newInterfaces = new LinkedHashSet<>(newClass.interfaces());
        for (String oldIface : oldClass.interfaces()) {
            if (!newInterfaces.contains(oldIface)) {
                changes.add(change(
                        JAVA_SUPERTYPE_REMOVED,
                        className + ":" + oldIface,
                        tier,
                        oldIface,
                        "",
                        "Implemented interface removed from " + className + ": " + oldIface,
                        true));
            }
        }

        if (tier.isDowngradeTo(newClass.stability())) {
            changes.add(change(
                    JAVA_STABILITY_DOWNGRADED,
                    className,
                    tier,
                    tier.name(),
                    newClass.stability().name(),
                    "Java type stability downgraded from " + tier + " to " + newClass.stability(),
                    true));
        }

        compareConstructors(oldClass, newClass, changes);
        compareMethods(oldClass, newClass, changes);
        compareFields(oldClass, newClass, changes);
    }

    private static void compareConstructors(
            ClassEntry oldClass,
            ClassEntry newClass,
            List<SurfaceChange> changes) {
        Map<String, ConstructorEntry> oldCtors = new LinkedHashMap<>();
        for (ConstructorEntry ctor : oldClass.constructors()) {
            oldCtors.put(ctor.signatureKey(), ctor);
        }
        Map<String, ConstructorEntry> newCtors = new LinkedHashMap<>();
        for (ConstructorEntry ctor : newClass.constructors()) {
            newCtors.put(ctor.signatureKey(), ctor);
        }

        for (ConstructorEntry oldCtor : oldClass.constructors()) {
            String path = oldClass.className() + "#" + oldCtor.signatureKey();
            ConstructorEntry newCtor = newCtors.get(oldCtor.signatureKey());
            if (newCtor == null || !newCtor.visibility().isExported()) {
                if (newCtor != null && newCtor.visibility().rank() < oldCtor.visibility().rank()) {
                    changes.add(change(
                            JAVA_CONSTRUCTOR_VISIBILITY_REDUCED,
                            path,
                            oldCtor.stability(),
                            oldCtor.visibility().name(),
                            newCtor.visibility().name(),
                            "Constructor visibility reduced on " + path,
                            true));
                } else {
                    changes.add(change(
                            JAVA_CONSTRUCTOR_REMOVED,
                            path,
                            oldCtor.stability(),
                            oldCtor.signatureKey(),
                            "",
                            "Constructor removed: " + path,
                            true));
                }
                continue;
            }
            if (newCtor.visibility().rank() < oldCtor.visibility().rank()) {
                changes.add(change(
                        JAVA_CONSTRUCTOR_VISIBILITY_REDUCED,
                        path,
                        oldCtor.stability(),
                        oldCtor.visibility().name(),
                        newCtor.visibility().name(),
                        "Constructor visibility reduced from " + oldCtor.visibility() + " to " + newCtor.visibility(),
                        true));
            }
            if (oldCtor.stability().isDowngradeTo(newCtor.stability())) {
                changes.add(change(
                        JAVA_STABILITY_DOWNGRADED,
                        path,
                        oldCtor.stability(),
                        oldCtor.stability().name(),
                        newCtor.stability().name(),
                        "Constructor stability downgraded from " + oldCtor.stability() + " to " + newCtor.stability(),
                        true));
            }
        }

        for (ConstructorEntry newCtor : newClass.constructors()) {
            if (!oldCtors.containsKey(newCtor.signatureKey()) && newCtor.visibility().isExported()) {
                String path = newClass.className() + "#" + newCtor.signatureKey();
                changes.add(change(
                        JAVA_CONSTRUCTOR_ADDED,
                        path,
                        newCtor.stability(),
                        "",
                        newCtor.signatureKey(),
                        "Constructor added: " + path,
                        false));
            }
        }
    }

    private static void compareMethods(
            ClassEntry oldClass,
            ClassEntry newClass,
            List<SurfaceChange> changes) {
        Map<String, MethodEntry> oldMethods = new LinkedHashMap<>();
        for (MethodEntry method : oldClass.methods()) {
            oldMethods.put(method.signatureKey(), method);
        }
        Map<String, MethodEntry> newMethods = new LinkedHashMap<>();
        for (MethodEntry method : newClass.methods()) {
            newMethods.put(method.signatureKey(), method);
        }

        for (MethodEntry oldMethod : oldClass.methods()) {
            String path = oldClass.className() + "#" + oldMethod.signatureKey();
            MethodEntry newMethod = newMethods.get(oldMethod.signatureKey());
            if (newMethod == null || !newMethod.visibility().isExported()) {
                if (newMethod != null && newMethod.visibility().rank() < oldMethod.visibility().rank()) {
                    changes.add(change(
                            JAVA_METHOD_VISIBILITY_REDUCED,
                            path,
                            oldMethod.stability(),
                            oldMethod.visibility().name(),
                            newMethod.visibility().name(),
                            "Method visibility reduced from " + oldMethod.visibility() + " to " + newMethod.visibility(),
                            true));
                } else {
                    changes.add(change(
                            JAVA_METHOD_REMOVED,
                            path,
                            oldMethod.stability(),
                            oldMethod.returnType(),
                            "",
                            "Method removed: " + path,
                            true));
                }
                continue;
            }

            if (!oldMethod.returnType().equals(newMethod.returnType())) {
                changes.add(change(
                        JAVA_METHOD_RETURN_TYPE_CHANGED,
                        path,
                        oldMethod.stability(),
                        oldMethod.returnType(),
                        newMethod.returnType(),
                        "Method return type changed on " + path + ": "
                                + oldMethod.returnType() + " -> " + newMethod.returnType(),
                        true));
            }

            if (newMethod.visibility().rank() < oldMethod.visibility().rank()) {
                changes.add(change(
                        JAVA_METHOD_VISIBILITY_REDUCED,
                        path,
                        oldMethod.stability(),
                        oldMethod.visibility().name(),
                        newMethod.visibility().name(),
                        "Method visibility reduced from " + oldMethod.visibility() + " to " + newMethod.visibility(),
                        true));
            }

            if (oldMethod.staticMethod() != newMethod.staticMethod()) {
                changes.add(change(
                        JAVA_METHOD_STATIC_CHANGED,
                        path,
                        oldMethod.stability(),
                        oldMethod.staticMethod() ? "static" : "instance",
                        newMethod.staticMethod() ? "static" : "instance",
                        "Method static modifier changed on " + path,
                        true));
            }

            if (!oldMethod.finalMethod()
                    && newMethod.finalMethod()
                    && !oldClass.finalType()
                    && !oldMethod.staticMethod()) {
                changes.add(change(
                        JAVA_METHOD_FINAL_ADDED,
                        path,
                        oldMethod.stability(),
                        "non-final",
                        "final",
                        "Overridable method became final on " + path,
                        true));
            }

            if (!oldMethod.abstractMethod() && newMethod.abstractMethod()) {
                changes.add(change(
                        JAVA_METHOD_ABSTRACT_ADDED,
                        path,
                        oldMethod.stability(),
                        "concrete",
                        "abstract",
                        "Concrete or default method became abstract on " + path,
                        true));
            }

            if (oldMethod.stability().isDowngradeTo(newMethod.stability())) {
                changes.add(change(
                        JAVA_STABILITY_DOWNGRADED,
                        path,
                        oldMethod.stability(),
                        oldMethod.stability().name(),
                        newMethod.stability().name(),
                        "Method stability downgraded from " + oldMethod.stability() + " to " + newMethod.stability(),
                        true));
            }
        }

        for (MethodEntry newMethod : newClass.methods()) {
            if (!oldMethods.containsKey(newMethod.signatureKey()) && newMethod.visibility().isExported()) {
                String path = newClass.className() + "#" + newMethod.signatureKey();
                boolean abstractAdditionOnExtensibleType =
                        (newClass.kind() == TypeKind.INTERFACE
                                && !newMethod.defaultMethod()
                                && !newMethod.staticMethod())
                        || (newClass.kind() == TypeKind.CLASS
                                && newClass.abstractType()
                                && newMethod.abstractMethod());
                if (abstractAdditionOnExtensibleType) {
                    changes.add(change(
                            JAVA_ABSTRACT_METHOD_ADDED,
                            path,
                            oldClass.stability(),
                            "",
                            newMethod.returnType(),
                            "Abstract method added to extensible type without default implementation: " + path,
                            true));
                } else {
                    changes.add(change(
                            JAVA_METHOD_ADDED,
                            path,
                            newMethod.stability(),
                            "",
                            newMethod.returnType(),
                            "Method added: " + path,
                            false));
                }
            }
        }
    }

    private static void compareFields(
            ClassEntry oldClass,
            ClassEntry newClass,
            List<SurfaceChange> changes) {
        Map<String, FieldEntry> oldFields = new LinkedHashMap<>();
        for (FieldEntry field : oldClass.fields()) {
            oldFields.put(field.name(), field);
        }
        Map<String, FieldEntry> newFields = new LinkedHashMap<>();
        for (FieldEntry field : newClass.fields()) {
            newFields.put(field.name(), field);
        }

        for (FieldEntry oldField : oldClass.fields()) {
            String path = oldClass.className() + "#" + oldField.name();
            FieldEntry newField = newFields.get(oldField.name());
            if (newField == null || !newField.visibility().isExported()) {
                if (newField != null && newField.visibility().rank() < oldField.visibility().rank()) {
                    changes.add(change(
                            JAVA_FIELD_VISIBILITY_REDUCED,
                            path,
                            oldField.stability(),
                            oldField.visibility().name(),
                            newField.visibility().name(),
                            "Field visibility reduced from " + oldField.visibility() + " to " + newField.visibility(),
                            true));
                } else {
                    changes.add(change(
                            JAVA_FIELD_REMOVED,
                            path,
                            oldField.stability(),
                            oldField.fieldType(),
                            "",
                            "Field removed: " + path,
                            true));
                }
                continue;
            }

            if (!oldField.fieldType().equals(newField.fieldType())) {
                changes.add(change(
                        JAVA_FIELD_TYPE_CHANGED,
                        path,
                        oldField.stability(),
                        oldField.fieldType(),
                        newField.fieldType(),
                        "Field type changed on " + path + ": "
                                + oldField.fieldType() + " -> " + newField.fieldType(),
                        true));
            }

            if (newField.visibility().rank() < oldField.visibility().rank()) {
                changes.add(change(
                        JAVA_FIELD_VISIBILITY_REDUCED,
                        path,
                        oldField.stability(),
                        oldField.visibility().name(),
                        newField.visibility().name(),
                        "Field visibility reduced from " + oldField.visibility() + " to " + newField.visibility(),
                        true));
            }

            if (oldField.staticField() != newField.staticField()) {
                changes.add(change(
                        JAVA_FIELD_STATIC_CHANGED,
                        path,
                        oldField.stability(),
                        oldField.staticField() ? "static" : "instance",
                        newField.staticField() ? "static" : "instance",
                        "Field static modifier changed on " + path,
                        true));
            }

            if (!oldField.finalField() && newField.finalField()) {
                changes.add(change(
                        JAVA_FIELD_FINAL_ADDED,
                        path,
                        oldField.stability(),
                        "non-final",
                        "final",
                        "Mutable field became final on " + path,
                        true));
            }

            if (oldField.stability().isDowngradeTo(newField.stability())) {
                changes.add(change(
                        JAVA_STABILITY_DOWNGRADED,
                        path,
                        oldField.stability(),
                        oldField.stability().name(),
                        newField.stability().name(),
                        "Field stability downgraded from " + oldField.stability() + " to " + newField.stability(),
                        true));
            }
        }

        for (FieldEntry newField : newClass.fields()) {
            if (!oldFields.containsKey(newField.name()) && newField.visibility().isExported()) {
                String path = newClass.className() + "#" + newField.name();
                changes.add(change(
                        JAVA_FIELD_ADDED,
                        path,
                        newField.stability(),
                        "",
                        newField.fieldType(),
                        "Field added: " + path,
                        false));
            }
        }
    }

    private static SurfaceChange change(
            String ruleId,
            String path,
            StabilityTier stability,
            String before,
            String after,
            String message,
            boolean breaking) {
        return new SurfaceChange(
                CompatibilitySurface.JAVA_API,
                ruleId,
                path,
                stability,
                before,
                after,
                message,
                breaking);
    }
}
