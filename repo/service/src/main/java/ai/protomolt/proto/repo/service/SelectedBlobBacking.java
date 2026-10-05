package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One authoritative provider selection, distinct from any cache placed in front of it. */
record SelectedBlobBacking(OpenedBlobStore handle, Optional<BackendIdentity> identity, ObjectReclaimer reclaimer) {
    SelectedBlobBacking {
        Objects.requireNonNull(handle);
        Objects.requireNonNull(identity);
        Objects.requireNonNull(reclaimer);
    }

    static SelectedBlobBacking open(BlobStores providers, String provider, Map<String, String> options,
            boolean managed, OwnedResources lifetime) {
        var selectedOptions = Map.copyOf(options);
        // Identity is obtained from the selected factory and the same options as
        // open. Unmanaged providers need not implement managed identity at all.
        var identity = managed ? Optional.of(providers.managedIdentity(provider, selectedOptions))
                : Optional.<BackendIdentity>empty();
        var opened = lifetime.add(providers.open(provider, selectedOptions));
        return new SelectedBlobBacking(opened, identity, opened.reclaimer());
    }

    SelectedBlobBacking withReclaimer(ObjectReclaimer decorated) {
        return new SelectedBlobBacking(handle, identity, decorated);
    }

    BackendIdentity requireManagedIdentity() {
        return identity.orElseThrow(() -> new IllegalStateException("Selected backing has no managed identity"));
    }
}
