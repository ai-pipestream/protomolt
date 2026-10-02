package ai.protomolt.proto.actions;

import java.util.List;

/**
 * Optional actions supplied through {@link java.util.ServiceLoader}.
 * Providers must return actions in a stable order. Duplicate names fail catalog construction;
 * a provider cannot replace another action or bypass catalog authorization and validation.
 * Provider code runs in the host process and must be trusted like any other dependency.
 */
public interface ActionProvider {
    List<? extends ProtoAction> actions();
}
