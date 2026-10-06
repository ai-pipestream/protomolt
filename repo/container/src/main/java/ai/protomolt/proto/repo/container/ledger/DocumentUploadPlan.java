package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.codec.RepositoryNamespaces;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.DocumentRevisionCondition;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Pure mapping from reviewed command to new-byte staging intent. No admission,
 * provider I/O, permission, retention or publication occurs here. In particular,
 * these plans cannot be passed to the legacy full-revision attempt API.
 */
final class DocumentUploadPlan {
    private DocumentUploadPlan() {}

    /** Selected immutable configuration, to be rechecked under SQL locks at admission. */
    record Placement(DocumentDriveSnapshot drive, String generation, ManagedBackendLedger.Profile profile) {
        Placement {
            Objects.requireNonNull(drive); Objects.requireNonNull(generation); Objects.requireNonNull(profile);
            if (!"ACTIVE".equals(drive.status()) || !profile.identity().provider().equals(drive.provider()))
                throw new IllegalArgumentException("Selected drive is inactive or differs from backend provider");
        }
        static Placement sample(DriveRecord drive, String generation, ManagedBackendLedger.Profile profile) {
            return new Placement(DocumentDriveSnapshot.of(drive), generation, profile);
        }
        @Override public String toString() { return "Placement[driveId=" + drive.id() + ", generation=" + generation + "]"; }
    }

    /** A compact upload ordinal and its position in the complete revision are distinct. */
    record Upload(int revisionOrdinal, DocumentPartAttemptLedger.PlannedObject object) {
        Upload { Objects.requireNonNull(object); if (revisionOrdinal < 0) throw new IllegalArgumentException("Negative slot ordinal"); }
    }

    /** Nonempty new-byte subset. It may contain no CORE; the complete member still must. */
    record Attempt(UUID id, DocumentPartAttemptLedger.Location location, List<Upload> uploads) {
        Attempt {
            Objects.requireNonNull(id); Objects.requireNonNull(location);
            uploads = List.copyOf(uploads);
            if (uploads.isEmpty()) throw new IllegalArgumentException("No attempt exists for a zero-upload revision");
        }
    }

    record Member(DocumentPublicationMember intent, UUID nodeId, Placement placement,
            Map<UUID, Long> sources, Optional<Attempt> attempt) {
        Member {
            Objects.requireNonNull(intent); Objects.requireNonNull(nodeId); Objects.requireNonNull(placement);
            sources = Map.copyOf(sources); Objects.requireNonNull(attempt);
        }
    }

    /** Retains the same executable/canonical command; generated keys never change its identity. */
    record Prepared(DocumentPublicationCommand command, List<Member> members,
            List<DocumentHistoricalReferenceAdmission.Prepared> historical) {
        Prepared { Objects.requireNonNull(command); members = List.copyOf(members); historical = List.copyOf(historical); }
        Prepared(DocumentPublicationCommand command, List<Member> members) { this(command, members, List.of()); }
    }

    /**
     * Attempt UUIDs are coordinator-minted and retained across uncertain admission.
     * Exactly one is required for each member with new bytes, none for other members.
     * Placement is selected by stable drive UUID, not a name or provider fallback.
     */
    static Prepared prepare(DocumentPublicationCommand command, Map<UUID, Placement> selected,
            Map<String, UUID> attempts) {
        Objects.requireNonNull(command);
        command.requireExecutionSupported();
        return prepare(command, selected, attempts, List.of(), () -> {});
    }

    /** Internal historical plan; no admission or execution capability is granted. */
    static Prepared prepare(DocumentPublicationCommand command, Map<UUID, Placement> selected,
            Map<String, UUID> attempts, List<DocumentHistoricalReferenceAdmission.Prepared> historical, Runnable control) {
        historical = DocumentHistoricalReferenceAdmission.requireComplete(command, historical, control);
        Objects.requireNonNull(selected); Objects.requireNonNull(attempts);
        int memberCount = command.intent().getMembersCount();
        if (selected.size() > memberCount || attempts.size() > memberCount)
            throw new IllegalArgumentException("Extraneous drive selection or attempt for a non-uploading member");
        selected = Map.copyOf(selected); attempts = Map.copyOf(attempts);
        var usedDrives = new HashSet<UUID>();
        var usedMembers = new HashSet<String>();
        var usedAttempts = new HashSet<UUID>();
        var identities = new HashMap<UUID, NodeAddress>();
        var members = new ArrayList<Member>(memberCount);
        for (var member : command.intent().getMembersList()) {
            control.run();
            UUID driveId = UUID.fromString(member.getDriveId());
            usedDrives.add(driveId);
            var placement = selected.get(driveId);
            if (placement == null || !driveId.equals(placement.drive.id())
                    || !command.intent().getAccountId().equals(placement.drive.account()))
                throw new IllegalArgumentException("Selected drive is missing or differs from command scope");
            UUID node = nodeId(member.getDestination().getAddress(), identities);
            // Reuse-only members still select a repository placement; validate
            // it without manufacturing an upload attempt or a physical object.
            var location = new DocumentPartAttemptLedger.Location(node, command.intent().getAccountId(),
                    placement.generation, placement.drive.namespace());
            var sources = new HashMap<UUID, Long>();
            for (var source : member.getSourcesList()) addSource(sources, source, identities);
            for (var part : member.getPartsList()) {
                if (part.hasReuse()) addSource(sources, part.getReuse().getSource(), identities);
                if (part.hasHistoricalReuse()) nodeId(part.getHistoricalReuse().getSource(), identities);
            }
            boolean hasUploads = member.getPartsList().stream().anyMatch(p -> p.hasUpload());
            Optional<Attempt> attempt = Optional.empty();
            if (hasUploads) {
                UUID id = attempts.get(member.getMemberId());
                if (id == null || !usedAttempts.add(id))
                    throw new IllegalArgumentException("A distinct attempt UUID is required for each uploading member");
                usedMembers.add(member.getMemberId());
                String prefix = RepositoryNamespaces.under(placement.drive.prefix(), "documents/"
                        + location.accountId() + "/" + node + "/attempts/" + id + "/");
                var uploads = new ArrayList<Upload>();
                for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                    var part = member.getParts(ordinal);
                    if (!part.hasUpload()) continue;
                    var declaration = part.getUpload();
                    uploads.add(new Upload(ordinal, new DocumentPartAttemptLedger.PlannedObject(part.getSlot().getPart(),
                            part.getSlot().getSubKey(), prefix + "part-" + ordinal, declaration.getSizeBytes(),
                            declaration.getSha256(), declaration.getContentType())));
                }
                attempt = Optional.of(new Attempt(id, location, uploads));
            }
            members.add(new Member(member, node, placement, sources, attempt));
        }
        if (!usedDrives.equals(selected.keySet()) || !usedMembers.equals(attempts.keySet()))
            throw new IllegalArgumentException("Extraneous drive selection or attempt for a non-uploading member");
        control.run(); historical.forEach(source -> source.selectors());
        return new Prepared(command, members, historical);
    }

    private static void addSource(Map<UUID, Long> sources, DocumentRevisionCondition source, Map<UUID, NodeAddress> identities) {
        var prior = sources.putIfAbsent(nodeId(source.getAddress(), identities), source.getExpectedMutationRevision());
        if (prior != null && prior != source.getExpectedMutationRevision())
            throw new IllegalArgumentException("Conflicting upload source revision");
    }

    private static UUID nodeId(NodeAddress address, Map<UUID, NodeAddress> identities) {
        // Legacy IDs concatenate with '|'. Do not admit an ambiguous tuple or
        // silently change stored node identity while introducing this new path.
        if (address.getDocId().contains("|") || address.getGraphAddressId().contains("|")
                || address.getAccountId().contains("|") || address.getGraphId().contains("|"))
            throw new IllegalArgumentException("Address delimiter requires explicit identity migration");
        UUID id = DocumentIds.nodeId(address);
        var prior = identities.putIfAbsent(id, address);
        if (prior != null && !prior.equals(address)) throw new IllegalArgumentException("Distinct addresses map to one node identity");
        return id;
    }
}
