package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import java.util.Objects;
import java.util.Optional;

/** Authentication result. An absent binding cannot authorize key-specific capabilities. */
public record AuthenticatedCaller(Caller caller, Optional<CredentialBinding> binding) {
    public AuthenticatedCaller {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(binding, "binding");
        if (caller.unrestricted() && binding.isPresent())
            throw new IllegalArgumentException("Process authority does not carry a scoped credential binding");
    }

    public static AuthenticatedCaller unbound(Caller caller) {
        return new AuthenticatedCaller(caller, Optional.empty());
    }
}
