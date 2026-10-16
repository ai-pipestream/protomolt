package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import io.grpc.Status;

/** Current read policy for a retained rejection, shared by replay and lookup. */
final class RejectionAccess {
    private RejectionAccess() {}

    static void require(PostgresCatalog catalog, RepositoryCaller caller, CatalogRecords.Outcome recorded) {
        if (caller.processAuthority()) return;
        var access = recorded.rejectionAccess().orElseThrow(RejectionAccess::denied);
        DocumentReads.requireAccount(caller, access.address().getAccountId());
        var current = catalog.current(access.address());
        if (current.isPresent()) {
            var manifest = catalog.manifest(access.address(), current.get().revision())
                    .orElseThrow(() -> new AccessionException(Status.Code.DATA_LOSS, "Current revision has no manifest", null));
            if (!DocumentReads.canRead(caller, access.address(), manifest.getOwnership())) throw denied();
            return;
        }
        // Never use credential ownership as a fallback for an existing document that disappeared.
        if (!access.create()) throw denied();
        var creator = access.creator().orElseThrow(RejectionAccess::denied);
        var binding = caller.credentialBinding().orElseThrow(RejectionAccess::denied);
        if (!creator.issuer().equals(binding.issuer()) || !creator.credentialId().equals(binding.credentialId())) throw denied();
        var credential = catalog.credential(binding.issuer(), binding.credentialId()).orElseThrow(RejectionAccess::denied);
        if (credential.revoked() || credential.generation() != binding.generation()) throw denied();
    }

    private static AccessionException denied() {
        return new AccessionException(Status.Code.PERMISSION_DENIED, "The caller may not read this outcome", null);
    }
}
