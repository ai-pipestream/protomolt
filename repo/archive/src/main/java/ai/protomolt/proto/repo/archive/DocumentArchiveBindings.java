package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiPredicate;

/**
 * Production {@link ArchiveBindings} backed by v2 documents in {@code DocumentService}.
 *
 * <p>Each archive definition is stored as a CORE-only v2 document at
 * {@code (account_id, "archive-def:" + account_id, "archives", archive_name)}.
 * Creation uses {@code ExpectedRevision.none} so duplicate archive names in the
 * same account fail atomically with {@code ALREADY_EXISTS}. Listing reads the
 * catalog's {@link DocumentLookup} rows directly through {@code ListDocuments}.</p>
 */
public final class DocumentArchiveBindings implements ArchiveBindings {

    /** Graph ID prefix for v2 archive definition documents: {@value}. */
    public static final String DEFINITION_GRAPH_PREFIX = "archive-def:";

    /** Graph address ID for v2 archive definition documents: {@value}. */
    public static final String DEFINITION_GRAPH_ADDRESS = "archives";

    private static final String DRIVE_NAME_KEY = "archive.drive_name";
    private static final String STORAGE_DRIVE_KEY = "archive.storage_drive";
    private static final String DATASOURCE_ID_KEY = "archive.datasource_id";
    private static final String VERSIONING_KEY = "archive.versioning";
    private static final String DESCRIPTION_KEY = "archive.description";
    private static final String USER_META_PREFIX = "meta.";

    private static final PartSelection CORE_ONLY =
            PartSelection.newBuilder().addSelectors(ArchiveCoordinates.CORE).build();

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String datasourceId;
    private final String storageDrive;
    private final BiPredicate<String, String> driveExists;
    private final ArchiveBindings fallback;

    /**
     * Constructs a v2 document-backed {@link ArchiveBindings}.
     *
     * @param documents the channel to the v2 {@code DocumentService}
     * @param datasourceId the intake datasource ID used for archive definition writes and entry bindings
     * @param storageDrive the v2 drive name for definition writes (blank uses the repository default drive)
     * @param driveExists predicate testing whether {@code (accountId, driveName)} exists
     * @param fallback optional fallback bindings for pre-configured static archives
     */
    public DocumentArchiveBindings(
            Channel documents,
            String datasourceId,
            String storageDrive,
            BiPredicate<String, String> driveExists,
            ArchiveBindings fallback) {
        this.documents = DocumentServiceGrpc.newBlockingStub(Objects.requireNonNull(documents, "documents"));
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId must not be blank");
        }
        this.datasourceId = datasourceId;
        this.storageDrive = Objects.requireNonNull(storageDrive, "storageDrive");
        this.driveExists = Objects.requireNonNull(driveExists, "driveExists");
        this.fallback = Objects.requireNonNull(fallback, "fallback");
    }

    /**
     * Returns the v2 {@link NodeAddress} where an archive definition document is stored.
     *
     * @param accountId the owning account ID
     * @param archive the archive slug
     * @return the definition document's node address
     */
    public static NodeAddress definitionAddress(String accountId, String archive) {
        return NodeAddress.newBuilder()
                .setAccountId(accountId)
                .setGraphId(DEFINITION_GRAPH_PREFIX + accountId)
                .setGraphAddressId(DEFINITION_GRAPH_ADDRESS)
                .setDocId(archive)
                .build();
    }

    @Override
    public Optional<ArchiveBinding> find(String accountId, String archive) {
        Optional<ArchiveBinding> configured = fallback.find(accountId, archive);
        if (configured.isPresent()) {
            return configured;
        }
        Optional<GetDocumentResponse> stored = readDefinition(accountId, archive);
        if (stored.isPresent()) {
            Map<String, String> meta = stored.get().getDocument().getSearchMetadata().getMetadataMap();
            String ds = meta.getOrDefault(DATASOURCE_ID_KEY, datasourceId);
            String drive = meta.getOrDefault(DRIVE_NAME_KEY, "");
            if (drive.isBlank()) {
                drive = meta.getOrDefault(STORAGE_DRIVE_KEY, storageDrive);
            }
            VersioningPolicy policy = parseVersioning(meta.getOrDefault(VERSIONING_KEY, ""));
            return Optional.of(new ArchiveBinding(accountId, archive, ds, drive, policy));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Archive> get(String accountId, String archive) {
        Optional<GetDocumentResponse> stored = readDefinition(accountId, archive);
        if (stored.isPresent()) {
            SearchMetadata search = stored.get().getDocument().getSearchMetadata();
            return Optional.of(fromMetadata(
                    accountId, archive, search.getCreationDate(), search.getMetadataMap()));
        }
        return fallback.get(accountId, archive);
    }

    @Override
    public Archive create(Archive archive, Timestamp createdAt) {
        String accountId = archive.getAccountId();
        String name = archive.getName();
        if (!driveExists.test(accountId, archive.getDriveName())) {
            throw ArchiveErrors.failedPrecondition(
                    "drive '" + archive.getDriveName() + "' not found for account '" + accountId + "'");
        }
        if (fallback.find(accountId, name).isPresent()) {
            throw ArchiveErrors.alreadyExists(
                    "archive", "archive '" + name + "' already exists for account '" + accountId + "'");
        }
        NodeAddress address = definitionAddress(accountId, name);
        SearchMetadata.Builder search = SearchMetadata.newBuilder()
                .setTitle(name)
                .setCreationDate(createdAt)
                .putMetadata(DRIVE_NAME_KEY, archive.getDriveName())
                .putMetadata(STORAGE_DRIVE_KEY, storageDrive)
                .putMetadata(DATASOURCE_ID_KEY, datasourceId)
                .putMetadata(VERSIONING_KEY, archive.getVersioning().name());
        if (!archive.getDescription().isBlank()) {
            search.putMetadata(DESCRIPTION_KEY, archive.getDescription());
        }
        for (Map.Entry<String, String> entry : archive.getMetadataMap().entrySet()) {
            search.putMetadata(USER_META_PREFIX + entry.getKey(), entry.getValue());
        }
        Document document = Document.newBuilder()
                .setDocId(name)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId))
                .setSearchMetadata(search)
                .build();
        SaveDocumentRequest save = SaveDocumentRequest.newBuilder()
                .setOperationId("archive.create:" + accountId + ":" + name + ":" + UUID.randomUUID())
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(ArchiveCoordinates.CORE)
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(SaveOwner.newBuilder()
                        .setIntake(IntakeOrigin.newBuilder().setDatasourceId(datasourceId)))
                .setValidationMode(ValidationMode.VALIDATION_MODE_TYPED)
                .setDrive(storageDrive)
                .build();
        try {
            documents.saveDocument(save);
        } catch (StatusRuntimeException failure) {
            Status.Code code = failure.getStatus().getCode();
            if (code == Status.Code.ABORTED || code == Status.Code.ALREADY_EXISTS) {
                throw ArchiveErrors.alreadyExists(
                        "archive", "archive '" + name + "' already exists for account '" + accountId + "'");
            }
            throw failure;
        }
        return fromMetadata(accountId, name, createdAt, search.getMetadataMap());
    }

    @Override
    public Page list(String accountId, int limit, String continuationToken) {
        ListDocumentsResponse listed = documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(accountId)
                .setGraphId(DEFINITION_GRAPH_PREFIX + accountId)
                .setGraphAddressId(DEFINITION_GRAPH_ADDRESS)
                .setPageSize(limit)
                .setPageToken(continuationToken)
                .build());
        List<Archive> archives = new ArrayList<>(listed.getDocumentsCount());
        for (DocumentSummary row : listed.getDocumentsList()) {
            DocumentLookup lookup = row.getLookup();
            Timestamp createdAt = lookup.hasCreationDate() ? lookup.getCreationDate() : row.getCommittedAt();
            archives.add(fromMetadata(
                    row.getAddress().getAccountId(),
                    row.getAddress().getDocId(),
                    createdAt,
                    lookup.getMetadataMap()));
        }
        return new Page(archives, listed.getNextPageToken());
    }

    private Optional<GetDocumentResponse> readDefinition(String accountId, String archive) {
        try {
            return Optional.of(documents.getDocument(GetDocumentRequest.newBuilder()
                    .setAddress(definitionAddress(accountId, archive))
                    .setRevision(RevisionSelector.newBuilder().setCurrent(true))
                    .setSelection(CORE_ONLY)
                    .build()));
        } catch (StatusRuntimeException failure) {
            if (failure.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw failure;
        }
    }

    private static Archive fromMetadata(
            String accountId, String name, Timestamp createdAt, Map<String, String> metadata) {
        Archive.Builder builder = Archive.newBuilder()
                .setAccountId(accountId)
                .setName(name)
                .setDriveName(metadata.getOrDefault(DRIVE_NAME_KEY, "default"))
                .setVersioning(parseVersioning(metadata.getOrDefault(VERSIONING_KEY, "")))
                .setCreatedAt(createdAt);
        String description = metadata.getOrDefault(DESCRIPTION_KEY, "");
        if (!description.isEmpty()) {
            builder.setDescription(description);
        }
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey().startsWith(USER_META_PREFIX)) {
                builder.putMetadata(
                        entry.getKey().substring(USER_META_PREFIX.length()), entry.getValue());
            }
        }
        return builder.build();
    }

    private static VersioningPolicy parseVersioning(String value) {
        if (VersioningPolicy.VERSIONING_POLICY_NONE.name().equals(value)) {
            return VersioningPolicy.VERSIONING_POLICY_NONE;
        }
        return VersioningPolicy.VERSIONING_POLICY_RETAINED;
    }
}
