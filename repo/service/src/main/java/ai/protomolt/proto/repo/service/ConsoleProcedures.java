package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;

import java.util.Set;

/**
 * The procedures the repository host exposes to the console through the Connect gateway:
 * exactly the calls the console's screens make, and nothing else. Every other method of the
 * three services stays reachable over gRPC and answers {@code unimplemented} at
 * {@code /connect}. A screen that needs a new call adds it here, and the pinning test fails
 * until the list matches.
 */
public final class ConsoleProcedures {

    /** The exposed procedures, as {@code package.Service/Method}. */
    public static final Set<String> ALL = Set.of(
            DocumentServiceGrpc.getListDocumentsMethod().getFullMethodName(),
            DocumentServiceGrpc.getGetManifestMethod().getFullMethodName(),
            DocumentServiceGrpc.getGetDocumentMethod().getFullMethodName(),
            DocumentServiceGrpc.getListRevisionsMethod().getFullMethodName(),
            DocumentServiceGrpc.getReadDocumentMethod().getFullMethodName(),
            DocumentServiceGrpc.getGetSchemaDescriptorMethod().getFullMethodName(),
            DocumentServiceGrpc.getReadBlobMethod().getFullMethodName(),
            DocumentServiceGrpc.getGetOperationOutcomeMethod().getFullMethodName(),
            DocumentServiceGrpc.getDeleteDocumentMethod().getFullMethodName(),
            DocumentServiceGrpc.getListLinksMethod().getFullMethodName(),
            DocumentServiceGrpc.getResolveLinkMethod().getFullMethodName(),
            DocumentServiceGrpc.getListBacklinksMethod().getFullMethodName(),
            DriveServiceGrpc.getListDrivesMethod().getFullMethodName(),
            ArchiveServiceGrpc.getListArchivesMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetArchiveMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetArchiveStatsMethod().getFullMethodName(),
            ArchiveServiceGrpc.getListEntriesMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetEntryManifestMethod().getFullMethodName(),
            ArchiveServiceGrpc.getListVersionsMethod().getFullMethodName(),
            ArchiveServiceGrpc.getReadRenditionMethod().getFullMethodName());

    private ConsoleProcedures() {
    }
}
