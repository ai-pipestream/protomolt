package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import java.util.Set;

/** Reviewed synchronous handlers only; new RPCs require an explicit lifetime review. */
final class BoundedArchiveMethods {
    private BoundedArchiveMethods() {}

    /** Read replies may be refused without making a committed mutation ambiguous. */
    static java.util.Map<String, Integer> readResponseLimits(int maxResponseBytes) {
        return java.util.Map.of(
                ArchiveServiceGrpc.getGetArchiveMethod().getFullMethodName(), maxResponseBytes,
                ArchiveServiceGrpc.getListArchivesMethod().getFullMethodName(), maxResponseBytes,
                ArchiveServiceGrpc.getGetEntryMethod().getFullMethodName(), maxResponseBytes,
                ArchiveServiceGrpc.getGetEntryManifestMethod().getFullMethodName(), maxResponseBytes,
                ArchiveServiceGrpc.getListEntriesMethod().getFullMethodName(), maxResponseBytes,
                ArchiveServiceGrpc.getListVersionsMethod().getFullMethodName(), maxResponseBytes,
                ArchiveServiceGrpc.getGetArchiveStatsMethod().getFullMethodName(), maxResponseBytes);
    }

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
