package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.spi.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Samples only the explicitly configured backend; admission rechecks these snapshots under locks. */
final class ManagedPublicationSelection implements DocumentPublicationRuntime.PublicationSelector {
    private final DriveLedger drives;
    private final ManagedBackendLedger profiles;
    private final String generation;
    private final ManagedBackendLedger.Profile expected;
    private final ManagedSchemaAccess schemas;
    private final ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition container;

    ManagedPublicationSelection(DriveLedger drives, ManagedBackendLedger profiles, String generation,
            ManagedBackendLedger.Profile expected, ManagedSchemaAccess schemas) {
        this.drives=Objects.requireNonNull(drives); this.profiles=Objects.requireNonNull(profiles);
        this.generation=Objects.requireNonNull(generation); this.expected=Objects.requireNonNull(expected);
        this.schemas=Objects.requireNonNull(schemas);
        container=BuiltinDocumentSchema.definition();
    }

    @Override public DocumentPublicationRuntime.PublicationSelection select(RepositoryCaller caller,
            DocumentPublicationCommand command, RepositoryReadControl control) {
        control.check();
        if (!caller.processAuthority() && !caller.accountIds().contains(command.intent().getAccountId())) throw unavailable();
        var placements=new HashMap<UUID,DocumentPublicationRuntime.Placement>();
        var profile=profiles.find(generation).filter(expected::equals).orElseThrow(() ->
                new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,"Configured document backend profile is unavailable"));
        var ids=command.intent().getMembersList().stream().map(member -> UUID.fromString(member.getDriveId()))
                .collect(java.util.stream.Collectors.toSet());
        var selected=drives.findByIds(command.intent().getAccountId(),ids).orElseThrow(ManagedPublicationSelection::unavailable);
        for (var drive:selected.values()) {
            control.check();
            placements.put(drive.driveId,new DocumentPublicationRuntime.Placement(drive,generation,profile));
        }
        control.check();
        return new DocumentPublicationRuntime.PublicationSelection(placements,Map.of(),Optional.of(container),schemas::open);
    }

    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.NOT_FOUND,"Document drive is unavailable");
    }
}
