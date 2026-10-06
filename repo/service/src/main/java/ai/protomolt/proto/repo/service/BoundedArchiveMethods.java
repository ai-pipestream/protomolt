package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import java.util.Set;

/** Reviewed synchronous handlers only; new RPCs require an explicit lifetime review. */
final class BoundedArchiveMethods {
    private BoundedArchiveMethods() {}

    static final Set<String> UNARY = Set.of(
            ArchiveServiceGrpc.getCreateArchiveMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetArchiveMethod().getFullMethodName(),
            ArchiveServiceGrpc.getListArchivesMethod().getFullMethodName(),
            ArchiveServiceGrpc.getPutEntryMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetEntryMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetEntryManifestMethod().getFullMethodName(),
            ArchiveServiceGrpc.getListEntriesMethod().getFullMethodName(),
            ArchiveServiceGrpc.getListVersionsMethod().getFullMethodName(),
            ArchiveServiceGrpc.getGetArchiveStatsMethod().getFullMethodName(),
            ArchiveServiceGrpc.getClassifyEntryMethod().getFullMethodName());
}
