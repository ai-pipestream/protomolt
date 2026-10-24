package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.Archive;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Looks up, records and lists the {@link ArchiveBinding} and {@link Archive}
 * definitions of an account's archives.
 *
 * <p>Remains a {@link FunctionalInterface} over {@link #find(String, String)} so
 * static or test-supplied lookup functions compose directly, while production
 * hosts back definitions with v2 documents via {@link DocumentArchiveBindings}.</p>
 */
@FunctionalInterface
public interface ArchiveBindings {

    /**
     * One page of an account's archive definitions.
     *
     * @param archives the archives on this page, ordered by archive name
     * @param nextContinuationToken token for the next page, or empty on the last page
     */
    record Page(List<Archive> archives, String nextContinuationToken) {

        /** Validates and defensively copies the page contents. */
        public Page {
            archives = List.copyOf(Objects.requireNonNull(archives, "archives"));
            Objects.requireNonNull(nextContinuationToken, "nextContinuationToken");
        }
    }

    /**
     * Resolves the runtime binding for an archive in an account.
     *
     * @param accountId the owning account ID
     * @param archive the archive slug
     * @return the archive's binding, or empty when the account has no such archive
     */
    Optional<ArchiveBinding> find(String accountId, String archive);

    /**
     * Reads the full {@link Archive} definition for an archive in an account.
     *
     * @param accountId the owning account ID
     * @param archive the archive slug
     * @return the archive definition, or empty when the account has no such archive
     */
    default Optional<Archive> get(String accountId, String archive) {
        return find(accountId, archive).map(binding -> Archive.newBuilder()
                .setAccountId(binding.accountId())
                .setName(binding.archive())
                .setDriveName(binding.drive().isEmpty() ? "default" : binding.drive())
                .setVersioning(binding.versioning())
                .build());
    }

    /**
     * Records a new archive definition with the server-stamped creation timestamp.
     *
     * @param archive the validated archive definition to record
     * @param createdAt the server-stamped creation timestamp
     * @return the stored archive definition
     */
    default Archive create(Archive archive, Timestamp createdAt) {
        throw ArchiveErrors.unimplemented(
                "CreateArchive is not supported by this read-only ArchiveBindings instance");
    }

    /**
     * Lists one page of archive definitions in an account.
     *
     * @param accountId the owning account ID
     * @param limit the maximum number of archives to return
     * @param continuationToken the continuation token from a previous page, or empty
     * @return the page of archives
     */
    default Page list(String accountId, int limit, String continuationToken) {
        throw ArchiveErrors.unimplemented(
                "ListArchives is not supported by this read-only ArchiveBindings instance");
    }

    /**
     * Creates a v2 document-backed {@link ArchiveBindings} that stores archive definitions
     * under the reserved {@code archive-def:<account_id>} graph.
     *
     * @param documents the channel to the v2 {@code DocumentService}
     * @param datasourceId the intake datasource ID used for archive definition writes and default entry bindings
     * @return the document-backed archive bindings
     */
    static DocumentArchiveBindings overDocuments(Channel documents, String datasourceId) {
        return new DocumentArchiveBindings(documents, datasourceId, "", (account, drive) -> true, (account, name) -> Optional.empty());
    }

    /**
     * Creates a v2 document-backed {@link ArchiveBindings} that validates the bound drive
     * before recording a new archive definition.
     *
     * @param documents the channel to the v2 {@code DocumentService}
     * @param datasourceId the intake datasource ID used for archive definition writes and default entry bindings
     * @param driveExists predicate testing whether {@code (accountId, driveName)} exists
     * @return the document-backed archive bindings
     */
    static DocumentArchiveBindings overDocuments(
            Channel documents, String datasourceId, BiPredicate<String, String> driveExists) {
        return new DocumentArchiveBindings(documents, datasourceId, "", driveExists, (account, name) -> Optional.empty());
    }

    /**
     * Creates a v2 document-backed {@link ArchiveBindings} with an explicit connector ID,
     * drive check, and fallback bindings.
     *
     * @param documents the channel to the v2 {@code DocumentService}
     * @param datasourceId the intake datasource ID used for archive definition writes and default entry bindings
     * @param connectorId the optional intake connector ID
     * @param driveExists predicate testing whether {@code (accountId, driveName)} exists
     * @param fallback fallback bindings consulted when an archive has no stored definition document
     * @return the document-backed archive bindings
     */
    static DocumentArchiveBindings overDocuments(
            Channel documents,
            String datasourceId,
            String connectorId,
            BiPredicate<String, String> driveExists,
            ArchiveBindings fallback) {
        return new DocumentArchiveBindings(documents, datasourceId, connectorId, driveExists, fallback);
    }
}
