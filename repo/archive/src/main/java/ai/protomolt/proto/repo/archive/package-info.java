/**
 * The archive over v2 documents (docs/design/archive.md, "The archive over v2
 * documents"): {@code ArchiveService} entries, renditions and versions stored as
 * v2 documents through {@code DocumentService}.
 *
 * <p>Reads are one v2 read per revision served, projected into the archive view
 * messages by the {@code (sources)} and {@code (from)} options on their
 * {@code .proto} and checked against their own validate.v1 rules
 * ({@link ai.protomolt.proto.repo.archive.ArchiveViews}). Writes are thin
 * translators into one v2 save ({@link ai.protomolt.proto.repo.archive.DocumentArchive}),
 * with v2 save validation as the only gate. A blank operation ID is derived by
 * {@link ai.protomolt.proto.repo.archive.ArchiveOperationKeys}.</p>
 */
package ai.protomolt.proto.repo.archive;
