-- Retire the legacy v1 archive ledger tables.
--
-- ArchiveService and ArchiveMutationService now run over v2 DocumentService
-- (DocumentArchiveGrpcService and DocumentArchiveMutationGrpcService) backed by
-- repo_catalog. No live deployment holds legacy archive_* ledger rows at cutover.

DROP TABLE
    archive_mutation_observations,
    archive_mutation_targets,
    archive_mutations,
    archive_read_pins,
    archive_version_object_refs,
    archive_object_uploads,
    archive_object_bindings,
    archive_rendition_stats,
    archive_stats,
    archive_versions,
    archive_entries,
    archives;
