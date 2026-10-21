package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.semantic.v1.DerivedText;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.Lineage;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;

import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A deterministic, seeded corpus of v2 documents and archive entries, as the
 * requests that write it. The corpus is a script: every document is a sequence
 * of saves and deletes through the real {@code DocumentService} save path, so
 * loading it exercises admission, validation, the catalog projection and the
 * stacks exactly as a client would. Nothing here touches storage; see
 * {@link CorpusLoader} for the documents and the repository fixtures module for
 * the archive entries and the dev seeder.
 *
 * <p>Shape: three accounts, each with an {@code intake} and a {@code pipeline}
 * drive, two intake datasources and two step modules. The document mix differs
 * per account on purpose, so a bug that hides behind a uniform fixture shape
 * has somewhere to show: multi-revision histories whose parts change between
 * revisions, tombstones, a document whose schema bindings change between
 * revisions, a document on the pipeline drive, documents with several blobs of
 * several media types, one large streamed blob, semantic parts written by a step
 * writer, lookup fields left unset in some documents and set in others, two
 * documents sharing a title, unicode metadata, and metadata values shaped like
 * the house formats (slugs, media types, SHA-256 hex, path-safe names, region
 * and postal codes).
 *
 * <p>Determinism: operation IDs are name-based UUIDs over the seed and the
 * document, blob bytes come from seeded generators, and dates are fixed. The
 * same seed and parameters build the same messages and the same
 * {@link #digest()}. Commit times are not part of the corpus: they come from
 * the {@link SteppedClock} the loader runs the saves under.
 */
public final class RepositoryCorpus {

    /** Seed of the corpus the fixtures load unless told otherwise. */
    public static final long DEFAULT_SEED = 542L;

    /** Length of the large streamed blob unless told otherwise: three MiB plus a few bytes. */
    public static final long DEFAULT_LARGE_BLOB_BYTES = 3L * 1024 * 1024 + 17;

    /** The first commit time a loader's stepped clock hands out. */
    public static final Instant EPOCH = Instant.parse("2025-03-01T08:00:00Z");

    /** How far each commit moves the stepped clock: an uneven step, so commits land on many days and hours. */
    public static final Duration STEP = Duration.ofHours(6).plusMinutes(37);

    /** The three accounts, in load order. */
    public static final List<String> ACCOUNTS = List.of("corpus-alpha", "corpus-beta", "corpus-gamma");

    /** Credential issuer of the step writers. */
    public static final String ISSUER = "corpus";

    /** Step module that writes PARSED parts. */
    public static final String PARSER_MODULE = "parser";

    /** Step module that writes CHUNKS and EMBEDDINGS parts. */
    public static final String CHUNKER_MODULE = "chunker";

    /** Drive names every account has. */
    public static final String INTAKE_DRIVE = "intake";

    /** The second document drive of every account. */
    public static final String PIPELINE_DRIVE = "pipeline";

    /** The custom drive the archives live on. */
    public static final String ARCHIVE_DRIVE = "archive";

    /** Service identity that may read and write every corpus document in its account. */
    public static final String READER_IDENTITY_PREFIX = "reader-";

    /** User principal that may read the restricted documents only. */
    public static final String AUDITOR_UPN = "auditor@example.com";

    /** User principal with read access to most documents. */
    public static final String ANALYST_UPN = "analyst@example.com";

    private static final String SEARCH_METADATA_TYPE_URL =
            "type.googleapis.com/" + SearchMetadata.getDescriptor().getFullName();

    /** What varies between builds: the seed and the large blob's length. */
    public record Parameters(long seed, long largeBlobBytes) {
        public Parameters {
            if (largeBlobBytes < 1) throw new IllegalArgumentException("The large blob needs at least one byte");
        }

        /** The default seed and large blob length. */
        public static Parameters defaults() {
            return new Parameters(DEFAULT_SEED, DEFAULT_LARGE_BLOB_BYTES);
        }
    }

    /** An intake datasource and the connector that fills it. */
    public record Datasource(String datasourceId, String connectorId) {}

    /** One account and its datasources. */
    public record Account(String accountId, List<Datasource> datasources) {}

    /** A step module's credential; the loader registers it and installs its grants. */
    public record StepWriter(String module, UUID credentialId) {}

    /** One write in a document's script. */
    public sealed interface Step permits Save, Delete {
        /** The revision the write produces. */
        long expectedRevision();
    }

    /**
     * A save. {@code streamed} carries the bytes of a blob the request names
     * without data: the loader uploads them as a stream. {@code stepModule} is
     * the step writer that performs the save; empty means process authority
     * saving on behalf of the intake datasource.
     */
    public record Save(SaveDocumentRequest request, long expectedRevision, Optional<StreamedBlob> streamed,
            Optional<String> stepModule) implements Step {}

    /** A tombstone. */
    public record Delete(DeleteDocumentRequest request, long expectedRevision) implements Step {}

    /** A blob uploaded as a stream; {@code body} opens a fresh stream of the same bytes each time. */
    public record StreamedBlob(String blobId, long length, String sha256, Supplier<InputStream> body) {}

    /** A document's writes, in order. */
    public record DocumentScript(NodeAddress address, String drive, List<Step> steps) {
        /** The revision the document ends at. */
        public long finalRevision() {
            return steps.getLast().expectedRevision();
        }

        /** Whether the script ends in a tombstone. */
        public boolean tombstoned() {
            return steps.getLast() instanceof Delete;
        }
    }

    /** An archive and its entry writes in order; a repeated address writes another version. */
    public record ArchiveScript(Archive archive, List<PutEntryRequest> entries) {}

    private final Parameters parameters;
    private final List<Account> accounts;
    private final List<StepWriter> stepWriters;
    private final List<DocumentScript> documents;
    private final List<ArchiveScript> archives;
    private final String digest;

    private RepositoryCorpus(Parameters parameters) {
        this.parameters = parameters;
        this.accounts = ACCOUNTS.stream().map(account -> new Account(account, List.of(
                new Datasource("ds-crawler", "file-crawler"),
                new Datasource("ds-share", "s3")))).toList();
        this.stepWriters = List.of(
                new StepWriter(PARSER_MODULE, nameUuid(parameters.seed(), "credential", PARSER_MODULE)),
                new StepWriter(CHUNKER_MODULE, nameUuid(parameters.seed(), "credential", CHUNKER_MODULE)));
        var scripts = new ArrayList<DocumentScript>();
        var archiveScripts = new ArrayList<ArchiveScript>();
        new Alpha(this, accounts.get(0)).write(scripts, archiveScripts);
        new Beta(this, accounts.get(1)).write(scripts, archiveScripts);
        new Gamma(this, accounts.get(2)).write(scripts, archiveScripts);
        this.documents = List.copyOf(scripts);
        this.archives = List.copyOf(archiveScripts);
        this.digest = digest(documents, archives);
    }

    /** Builds the corpus for the given parameters. */
    public static RepositoryCorpus build(Parameters parameters) {
        return new RepositoryCorpus(parameters);
    }

    /** Builds the default corpus. */
    public static RepositoryCorpus build() {
        return build(Parameters.defaults());
    }

    public Parameters parameters() {
        return parameters;
    }

    public List<Account> accounts() {
        return accounts;
    }

    public List<StepWriter> stepWriters() {
        return stepWriters;
    }

    public List<DocumentScript> documents() {
        return documents;
    }

    public List<ArchiveScript> archives() {
        return archives;
    }

    /** The documents of one account, in load order. */
    public List<DocumentScript> documents(String accountId) {
        return documents.stream().filter(script -> script.address().getAccountId().equals(accountId)).toList();
    }

    /** The document IDs the corpus writes into one account. */
    public List<String> documentIds(String accountId) {
        return documents(accountId).stream().map(script -> script.address().getDocId()).toList();
    }

    /** The reader service identity of an account. */
    public static String readerIdentity(String accountId) {
        return READER_IDENTITY_PREFIX + accountId;
    }

    /**
     * SHA-256 over the deterministic encoding of every request in load order,
     * with each streamed blob's digest and length in place of its bytes.
     */
    public String digest() {
        return digest;
    }

    private static String digest(List<DocumentScript> documents, List<ArchiveScript> archives) {
        var sha = sha256();
        for (var script : documents) {
            for (var step : script.steps()) {
                switch (step) {
                    case Save save -> {
                        sha.update(MessageFingerprints.deterministicBytes(save.request()));
                        save.streamed().ifPresent(blob -> sha.update(
                                (blob.blobId() + ":" + blob.length() + ":" + blob.sha256()).getBytes(StandardCharsets.UTF_8)));
                    }
                    case Delete delete -> sha.update(MessageFingerprints.deterministicBytes(delete.request()));
                }
            }
        }
        for (var script : archives) {
            sha.update(MessageFingerprints.deterministicBytes(script.archive()));
            for (var entry : script.entries()) sha.update(MessageFingerprints.deterministicBytes(entry));
        }
        return HexFormat.of().formatHex(sha.digest());
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException("SHA-256 unavailable", missing);
        }
    }

    /** A name-based UUID over the seed and the parts; the same inputs give the same ID. */
    static UUID nameUuid(long seed, String... parts) {
        return UUID.nameUUIDFromBytes(("protomolt-corpus:" + seed + ":" + String.join(":", parts))
                .getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------ shared building blocks

    /** The per-account writer: the document and archive scripts of one account. */
    private abstract static class AccountScripts {
        final RepositoryCorpus corpus;
        final Account account;
        final long seed;
        final Datasource crawler;
        final Datasource share;

        AccountScripts(RepositoryCorpus corpus, Account account) {
            this.corpus = corpus;
            this.account = account;
            this.seed = corpus.parameters.seed();
            this.crawler = account.datasources().get(0);
            this.share = account.datasources().get(1);
        }

        abstract void write(List<DocumentScript> documents, List<ArchiveScript> archives);

        String account() {
            return account.accountId();
        }

        long seed(String... parts) {
            return SyntheticBytes.seedFor(seed, account() + ":" + String.join(":", parts));
        }

        String operation(String docId, int step) {
            return nameUuid(seed, account(), docId, "op-" + step).toString();
        }

        NodeAddress address(String docId, Datasource datasource) {
            return NodeAddress.newBuilder().setDocId(docId).setGraphAddressId(datasource.datasourceId())
                    .setAccountId(account()).setGraphId("intake:" + account()).build();
        }

        /** Read and write for the account reader and the step modules, read for the analyst. */
        DocumentSecurity openSecurity() {
            return DocumentSecurity.newBuilder()
                    .addPermissions(rule("service", readerIdentity(account()), Access.ACCESS_READ))
                    .addPermissions(rule("service", readerIdentity(account()), Access.ACCESS_WRITE))
                    .addPermissions(rule("service", PARSER_MODULE, Access.ACCESS_READ))
                    .addPermissions(rule("service", PARSER_MODULE, Access.ACCESS_WRITE))
                    .addPermissions(rule("service", CHUNKER_MODULE, Access.ACCESS_READ))
                    .addPermissions(rule("service", CHUNKER_MODULE, Access.ACCESS_WRITE))
                    .addPermissions(rule("user-principal-name", ANALYST_UPN, Access.ACCESS_READ))
                    .build();
        }

        /** Only the auditor may read; the account reader is not on it. */
        DocumentSecurity restrictedSecurity() {
            return DocumentSecurity.newBuilder()
                    .addPermissions(rule("user-principal-name", AUDITOR_UPN, Access.ACCESS_READ))
                    .build();
        }

        static AccessRule rule(String type, String identity, Access access) {
            return AccessRule.newBuilder().setIdentityType(type).setIdentity(identity).setAccess(access).build();
        }

        OwnershipContext ownership(Datasource datasource, DocumentSecurity security, boolean withConnector) {
            var ownership = OwnershipContext.newBuilder().setAccountId(account())
                    .setDatasourceId(datasource.datasourceId()).setSecurity(security);
            if (withConnector) ownership.setConnectorId(datasource.connectorId());
            return ownership.build();
        }

        SaveOwner intakeOwner(Datasource datasource, boolean withConnector) {
            var origin = IntakeOrigin.newBuilder().setDatasourceId(datasource.datasourceId());
            if (withConnector) origin.setConnectorId(datasource.connectorId());
            return SaveOwner.newBuilder().setIntake(origin).build();
        }

        SaveOwner stepOwner(String module) {
            return SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setModuleId(module)
                    .setNodeId(module + "-1").setGraphId("pipeline:" + account()).setGraphVersion(1)).build();
        }

        static PartWrite put(DocumentPart part) {
            return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part))
                    .setAction(WriteAction.WRITE_ACTION_PUT).build();
        }

        static PartWrite putSegmentation(String id) {
            return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setSegmentationId(id))
                    .setAction(WriteAction.WRITE_ACTION_PUT).build();
        }

        static PartWrite putEmbeddings(String segmentation, String set) {
            return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setEmbeddingSet(
                    EmbeddingSetKey.newBuilder().setSegmentationId(segmentation).setEmbeddingSetId(set)))
                    .setAction(WriteAction.WRITE_ACTION_PUT).build();
        }

        static List<PartWrite> coreBlobsParsed() {
            return List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_BLOBS),
                    put(DocumentPart.DOCUMENT_PART_PARSED));
        }

        static Timestamp date(String iso) {
            var instant = Instant.parse(iso);
            return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
        }

        static ParserResult parsed(String parser, String version) {
            return ParserResult.newBuilder().setParserName(parser).setParserVersion(version)
                    .setStatus(ParseStatus.PARSE_STATUS_OK).build();
        }

        static Blob blob(String id, String mime, String filename, byte[] bytes) {
            return Blob.newBuilder().setBlobId(id).setMimeType(mime).setFilename(filename)
                    .setSizeBytes(bytes.length).setData(ByteString.copyFrom(bytes)).build();
        }

        /** A blob that also declares its SHA-256 checksum. */
        static Blob checksummedBlob(String id, String mime, String filename, byte[] bytes) {
            return blob(id, mime, filename, bytes).toBuilder().setChecksum(SyntheticBytes.sha256(bytes))
                    .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256).build();
        }

        static BlobBag oneBlob(Blob blob) {
            return BlobBag.newBuilder().setBlob(blob).build();
        }

        static BlobBag blobs(Blob... blobs) {
            var list = Blobs.newBuilder();
            for (var blob : blobs) list.addBlob(blob);
            return BlobBag.newBuilder().setBlobs(list).build();
        }

        SaveDocumentRequest.Builder save(String docId, NodeAddress address, int step, ExpectedRevision expected,
                Document document, SaveOwner owner, List<PartWrite> writes) {
            return SaveDocumentRequest.newBuilder().setOperationId(operation(docId, step))
                    .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(expected)
                    .setDocument(document).setOwner(owner).addAllWrites(writes);
        }

        static ExpectedRevision none() {
            return ExpectedRevision.newBuilder().setNone(true).build();
        }

        static ExpectedRevision at(long revision) {
            return ExpectedRevision.newBuilder().setRevision(revision).build();
        }

        Save intakeSave(SaveDocumentRequest.Builder request, long revision) {
            return new Save(request.build(), revision, Optional.empty(), Optional.empty());
        }

        Save stepSave(SaveDocumentRequest.Builder request, long revision, String module) {
            return new Save(request.build(), revision, Optional.empty(), Optional.of(module));
        }

        Delete delete(String docId, NodeAddress address, int step, long base, String reason) {
            return new Delete(DeleteDocumentRequest.newBuilder().setOperationId(operation(docId, step))
                    .setAddress(address).setExpected(at(base)).setReason(reason).build(), base + 1);
        }

        /** A segmentation over {@code search_metadata.body} in UTF-8 byte offsets, one segment per word window. */
        static Segmentation bodySegmentation(String id, String body, int wordsPerSegment) {
            var utf8 = body.getBytes(StandardCharsets.UTF_8);
            var segmentation = Segmentation.newBuilder().setSegmentationId(id)
                    .setKind(SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW)
                    .setSource(TextSource.newBuilder().setFieldPath("search_metadata.body")
                            .setBinding(binding(utf8)))
                    .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                    .setLineage(Lineage.newBuilder().setProducerId(CHUNKER_MODULE).setProducerVersion("1"));
            addWordWindows(segmentation, body, wordsPerSegment, "w");
            return segmentation.build();
        }

        /** A segmentation over text the producer derived itself; the text travels inside the segmentation. */
        static Segmentation derivedSegmentation(String id, String derivationId, String text) {
            var utf8 = text.getBytes(StandardCharsets.UTF_8);
            var segmentation = Segmentation.newBuilder().setSegmentationId(id)
                    .setKind(SegmentationKind.SEGMENTATION_KIND_SEMANTIC_BOUNDARY)
                    .setSource(TextSource.newBuilder()
                            .setDerived(DerivedText.newBuilder().setDerivationId(derivationId).setText(text))
                            .setBinding(binding(utf8)))
                    .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                    .setLineage(Lineage.newBuilder().setProducerId(CHUNKER_MODULE).setProducerVersion("1"));
            addWordWindows(segmentation, text, 1, "d");
            return segmentation.build();
        }

        private static SourceTextBinding binding(byte[] utf8) {
            return SourceTextBinding.newBuilder().setResolvedTextSha256(SyntheticBytes.sha256(utf8))
                    .setUtf8Length(utf8.length).build();
        }

        /** Cuts the text on single spaces into windows of {@code words} words, with UTF-8 byte offsets. */
        private static void addWordWindows(Segmentation.Builder segmentation, String text, int words, String idPrefix) {
            var tokens = text.split(" ");
            int ordinal = 0;
            long byteOffset = 0;
            int index = 0;
            while (index < tokens.length) {
                int end = Math.min(index + words, tokens.length);
                var window = String.join(" ", List.of(tokens).subList(index, end));
                long length = window.getBytes(StandardCharsets.UTF_8).length;
                segmentation.addSegments(Segment.newBuilder().setSegmentId(idPrefix + ordinal).setOrdinal(ordinal)
                        .setText(window).setStart(byteOffset).setEnd(byteOffset + length));
                byteOffset += length + 1;
                ordinal++;
                index = end;
            }
        }

        /** An embedding set bound to the segmentation's digest, one seeded vector per segment. */
        EmbeddingSet embeddingSet(Segmentation segmentation, String setId, String model, int dimensions) {
            var random = new Random(seed("embeddings", segmentation.getSegmentationId(), setId));
            var set = EmbeddingSet.newBuilder().setEmbeddingSetId(setId)
                    .setSegmentationId(segmentation.getSegmentationId())
                    .setSegmentationDigest(SegmentationDigests.digest(segmentation))
                    .setModel(model).setDimensions(dimensions).setSimilarity(Similarity.SIMILARITY_COSINE)
                    .setLineage(Lineage.newBuilder().setProducerId(CHUNKER_MODULE).setProducerVersion("1"));
            for (var segment : segmentation.getSegmentsList()) {
                var vector = SegmentVector.newBuilder().setSegmentId(segment.getSegmentId());
                for (int i = 0; i < dimensions; i++) vector.addValues(random.nextFloat() * 2 - 1);
                set.addVectors(vector);
            }
            return set.build();
        }

        static Document semanticFragment(List<Segmentation> segmentations, List<EmbeddingSet> sets) {
            return Document.newBuilder().setSearchMetadata(SearchMetadata.newBuilder()
                    .addAllSegmentations(segmentations).addAllEmbeddingSets(sets)).build();
        }

        // -------------------------------------------------------------- archive building blocks

        EntryAddress entry(String archive, String entryId) {
            return EntryAddress.newBuilder().setAccountId(account()).setArchive(archive).setEntryId(entryId).build();
        }

        static RenditionContent rendition(String name, String mediaType, byte[] bytes) {
            return RenditionContent.newBuilder()
                    .setRendition(RenditionDescriptor.newBuilder().setName(name).setMediaType(mediaType))
                    .setData(ByteString.copyFrom(bytes)).build();
        }

        static RenditionContent rendition(String name, String subKey, String mediaType, byte[] bytes) {
            return RenditionContent.newBuilder()
                    .setRendition(RenditionDescriptor.newBuilder().setName(name).setSubKey(subKey).setMediaType(mediaType))
                    .setData(ByteString.copyFrom(bytes)).build();
        }

        static WriteAttribution writtenBy(String module, String actor) {
            return WriteAttribution.newBuilder().setModule(module).setActor(actor).build();
        }

        Archive archive(String name, VersioningPolicy policy, String description) {
            return Archive.newBuilder().setName(name).setAccountId(account()).setDriveName(ARCHIVE_DRIVE)
                    .setVersioning(policy).setDescription(description).build();
        }
    }

    // ------------------------------------------------------------------ corpus-alpha

    /** The widest account: every shape the corpus knows, nine documents and one retained archive. */
    private static final class Alpha extends AccountScripts {
        Alpha(RepositoryCorpus corpus, Account account) {
            super(corpus, account);
        }

        @Override
        void write(List<DocumentScript> documents, List<ArchiveScript> archives) {
            documents.add(annualReport());
            documents.add(budget());
            documents.add(charter());
            documents.add(minutesJa());
            documents.add(manualLarge());
            documents.add(photo());
            documents.add(schemaShift());
            documents.add(anthology());
            documents.add(restrictedLedger());
            archives.add(docsArchive());
        }

        /** Three revisions: intake creates CORE, BLOBS and PARSED; the parser replaces PARSED; intake retitles CORE. */
        DocumentScript annualReport() {
            var docId = "annual-report-2025";
            var address = address(docId, crawler);
            var pdf = SyntheticBytes.pdf("Annual report 2025, corpus-alpha");
            var metadata = SearchMetadata.newBuilder().setTitle("Annual report").setBody(
                    "Revenue grew in every region while costs held flat across the fiscal year.")
                    .setDocumentType("PDF").setSourceUri("file:///share/finance/annual-report-2025.pdf")
                    .setSourceMimeType("application/pdf").setLanguage("en").setAuthor("Ada Byron")
                    .setCategory("finance").setCreationDate(date("2025-01-10T09:30:00Z"))
                    .setLastModifiedDate(date("2025-02-01T17:45:00Z"))
                    .putMetadata("region", "DE").putMetadata("postal_code", "10115")
                    .putMetadata("slug", "annual-report-2025")
                    .putMetadata("sha256", SyntheticBytes.sha256(pdf)).putMetadata("fiscal_year", "2025");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), true)).setSearchMetadata(metadata)
                    .setBlobBag(oneBlob(checksummedBlob("original", "application/pdf", "annual-report-2025.pdf", pdf)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(crawler, true), coreBlobsParsed()), 1);
            var reparsed = Document.newBuilder().putParserResults("docling", parsed("docling", "2.4")).build();
            var parse = stepSave(save(docId, address, 2, at(1), reparsed, stepOwner(PARSER_MODULE),
                    List.of(put(DocumentPart.DOCUMENT_PART_PARSED))), 2, PARSER_MODULE);
            var retitled = Document.newBuilder().setDocId(docId).setOwnership(ownership(crawler, openSecurity(), true))
                    .setSearchMetadata(metadata.setTitle("Annual report, final").setAuthor("Ada Byron, Charles Babbage")
                            .setLastModifiedDate(date("2025-02-20T08:00:00Z")).putMetadata("fiscal_year", "2025")
                            .putMetadata("status", "final")).build();
            var retitle = intakeSave(save(docId, address, 3, at(2), retitled, intakeOwner(crawler, true),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE))), 3);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create, parse, retitle));
        }

        /** One revision, CSV from the share datasource, no author. */
        DocumentScript budget() {
            var docId = "budget-q1";
            var address = address(docId, share);
            var csv = SyntheticBytes.csv(seed("budget"), 12);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Budget").setBody(
                            "Twelve quarterly lines of revenue and units by region.")
                            .setDocumentType("CSV").setSourceUri("s3://share/finance/budget-q1.csv")
                            .setSourceMimeType("text/csv").setLanguage("en").setCategory("finance")
                            .setCreationDate(date("2025-01-20T12:00:00Z"))
                            .putMetadata("region", "FR").putMetadata("postal_code", "75001"))
                    .setBlobBag(oneBlob(blob("original", "text/csv", "budget-q1.csv", csv)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1)));
        }

        /** HTML in German, then the chunker adds a body segmentation, a derived segmentation and vectors in two steps. */
        DocumentScript charter() {
            var docId = "charter";
            var address = address(docId, crawler);
            var body = "The charter binds every member to the shared rules of the council and names its officers.";
            var html = SyntheticBytes.html("Satzung", "Die Satzung bindet jedes Mitglied an die gemeinsamen Regeln.");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), false))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Satzung").setBody(body)
                            .setDocumentType("HTML").setSourceUri("file:///share/legal/satzung.html")
                            .setSourceMimeType("text/html").setLanguage("de").setCategory("legal")
                            .setCreationDate(date("2024-11-05T10:15:00Z"))
                            .putMetadata("region", "AT").putMetadata("postal_code", "1010"))
                    .setBlobBag(oneBlob(blob("original", "text/html", "satzung.html", html)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(crawler, false), coreBlobsParsed()), 1);
            var sentences = bodySegmentation("sentences", body, 4);
            var normalized = derivedSegmentation("normalized", "lowercase-de", "die satzung bindet jedes mitglied");
            var firstSets = List.of(embeddingSet(sentences, "model2vec", "model2vec-base-8m", 4),
                    embeddingSet(normalized, "model2vec", "model2vec-base-8m", 4));
            var chunk = stepSave(save(docId, address, 2, at(1), semanticFragment(List.of(sentences, normalized), firstSets),
                    stepOwner(CHUNKER_MODULE), List.of(putSegmentation("sentences"), putSegmentation("normalized"),
                            putEmbeddings("sentences", "model2vec"), putEmbeddings("normalized", "model2vec"))),
                    2, CHUNKER_MODULE);
            var secondSet = embeddingSet(sentences, "minilm", "all-minilm-l6-v2", 6);
            var embed = stepSave(save(docId, address, 3, at(2), semanticFragment(List.of(), List.of(secondSet)),
                    stepOwner(CHUNKER_MODULE), List.of(putEmbeddings("sentences", "minilm"))), 3, CHUNKER_MODULE);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create, chunk, embed));
        }

        /** Japanese meeting minutes with unicode title, body and metadata keys and values. */
        DocumentScript minutesJa() {
            var docId = "minutes-2025-02";
            var address = address(docId, share);
            var text = SyntheticBytes.plainText("議事録 2025年2月", "出席者は全員、次の四半期の計画に合意した。");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("議事録 2025年2月")
                            .setBody("出席者は全員、次の四半期の計画に合意した。").setDocumentType("TXT")
                            .setSourceUri("s3://share/minutes/2025-02.txt").setSourceMimeType("text/plain")
                            .setLanguage("ja").setAuthor("山田 花子").setCategory("minutes")
                            .setCreationDate(date("2025-02-14T01:00:00Z"))
                            .putMetadata("地域", "関東").putMetadata("région", "Kantō").putMetadata("emoji", "📝")
                            .putMetadata("region", "JP").putMetadata("postal_code", "100-0001"))
                    .setBlobBag(oneBlob(blob("original", "text/plain", "2025-02.txt", text)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1)));
        }

        /** The large blob: named in the request without data and uploaded as a stream. */
        DocumentScript manualLarge() {
            var docId = "service-manual";
            var address = address(docId, crawler);
            long length = corpus.parameters.largeBlobBytes();
            long blobSeed = seed("large-blob");
            String sha256;
            try {
                sha256 = SyntheticBytes.sha256(SyntheticBytes.opaqueStream(blobSeed, length));
            } catch (java.io.IOException impossible) {
                throw new IllegalStateException("An in-memory stream failed", impossible);
            }
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Service manual")
                            .setBody("Maintenance procedures for the field units, with torque tables and wiring diagrams.")
                            .setDocumentType("BIN").setSourceUri("file:///share/engineering/service-manual.bin")
                            .setSourceMimeType("application/octet-stream").setLanguage("en").setAuthor("Grace Hopper")
                            .setCategory("engineering").setCreationDate(date("2023-06-30T15:00:00Z"))
                            .putMetadata("sha256", sha256).putMetadata("path_safe_name", "service_manual.v3"))
                    .setBlobBag(oneBlob(Blob.newBuilder().setBlobId("original").setMimeType("application/octet-stream")
                            .setFilename("service-manual.bin").setSizeBytes(length).setChecksum(sha256)
                            .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256).build()))
                    .build();
            var request = save(docId, address, 1, none(), document, intakeOwner(crawler, true),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_BLOBS))).build();
            var streamed = new StreamedBlob("original", length, sha256, () -> SyntheticBytes.opaqueStream(blobSeed, length));
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    new Save(request, 1, Optional.of(streamed), Optional.empty())));
        }

        /** An image, then a tombstone. */
        DocumentScript photo() {
            var docId = "site-photo-0007";
            var address = address(docId, share);
            var ppm = SyntheticBytes.ppm(seed("photo"), 16, 12);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Site photo 7")
                            .setDocumentType("PPM").setSourceUri("s3://share/photos/0007.ppm")
                            .setSourceMimeType("image/x-portable-pixmap").setCategory("media")
                            .setCreationDate(date("2025-02-28T06:20:00Z")))
                    .setBlobBag(oneBlob(blob("original", "image/x-portable-pixmap", "0007.ppm", ppm)))
                    .build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_BLOBS))), 1);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create,
                    delete(docId, address, 2, 1, "withdrawn at the source")));
        }

        /** Revision 1 has no typed payload; revision 2 adds one, so the retained schema bindings differ. */
        DocumentScript schemaShift() {
            var docId = "product-sheet";
            var address = address(docId, crawler);
            var text = SyntheticBytes.plainText("Product sheet", "Model X200, 12 kg, 230 V.");
            var metadata = SearchMetadata.newBuilder().setTitle("Product sheet").setBody("Model X200, 12 kg, 230 V.")
                    .setDocumentType("TXT").setSourceUri("file:///share/catalog/x200.txt").setSourceMimeType("text/plain")
                    .setLanguage("en").setCategory("catalog").setCreationDate(date("2025-01-02T08:00:00Z"))
                    .putMetadata("gtin", "04006381333931");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), true)).setSearchMetadata(metadata)
                    .setBlobBag(oneBlob(blob("original", "text/plain", "x200.txt", text)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(crawler, true), coreBlobsParsed()), 1);
            var typed = Document.newBuilder().setDocId(docId).setOwnership(ownership(crawler, openSecurity(), true))
                    .setSearchMetadata(metadata)
                    .setStructuredData(Any.pack(SearchMetadata.newBuilder().setTitle("X200").setCategory("catalog").build()))
                    .build();
            var attach = intakeSave(save(docId, address, 2, at(1), typed, intakeOwner(crawler, true),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE))), 2);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create, attach));
        }

        /** Several blobs of several media types, on the pipeline drive, in French. */
        DocumentScript anthology() {
            var docId = "anthologie";
            var address = address(docId, share);
            var text = SyntheticBytes.plainText("Anthologie", "Recueil de textes choisis.");
            var csv = SyntheticBytes.csv(seed("anthology-index"), 5);
            var pdf = SyntheticBytes.pdf("Anthologie, corpus-alpha");
            var markdown = SyntheticBytes.markdown("Anthologie", "Recueil de textes choisis.");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Anthologie")
                            .setBody("Recueil de textes choisis, avec index et notes.").setDocumentType("PDF")
                            .setSourceUri("s3://share/litterature/anthologie.pdf").setSourceMimeType("application/pdf")
                            .setLanguage("fr").setAuthor("Émile Zola").setCategory("literature")
                            .setCreationDate(date("2022-09-15T14:30:00Z")).setLastModifiedDate(date("2025-01-30T09:00:00Z"))
                            .putMetadata("region", "FR").putMetadata("postal_code", "69001")
                            .putMetadata("mime", "application/pdf"))
                    .setBlobBag(blobs(checksummedBlob("original", "application/pdf", "anthologie.pdf", pdf),
                            blob("text", "text/plain", "anthologie.txt", text),
                            blob("index", "text/csv", "index.csv", csv),
                            blob("markdown", "text/markdown", "anthologie.md", markdown)))
                    .putParserResults("tika", parsed("tika", "3.1"))
                    .putParserResults("docling", parsed("docling", "2.4")).build();
            return new DocumentScript(address, PIPELINE_DRIVE, List.of(intakeSave(
                    save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed())
                            .setDrive(PIPELINE_DRIVE), 1)));
        }

        /** Same category as the annual report, readable by the auditor only. */
        DocumentScript restrictedLedger() {
            var docId = "ledger-restricted";
            var address = address(docId, crawler);
            var csv = SyntheticBytes.csv(seed("ledger"), 8);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, restrictedSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Ledger").setBody("Eight ledger lines.")
                            .setDocumentType("CSV").setSourceUri("file:///share/finance/ledger.csv")
                            .setSourceMimeType("text/csv").setLanguage("en").setAuthor("Ada Byron").setCategory("finance")
                            .setCreationDate(date("2025-02-03T11:00:00Z")).putMetadata("region", "DE"))
                    .setBlobBag(oneBlob(blob("original", "text/csv", "ledger.csv", csv)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    intakeSave(save(docId, address, 1, none(), document, intakeOwner(crawler, true), coreBlobsParsed()), 1)));
        }

        /** A retained archive: an entry with three renditions and two versions, and a sub-keyed entry. */
        ArchiveScript docsArchive() {
            var report = entry("docs", "reports/annual-2025");
            var pdf = SyntheticBytes.pdf("Annual report 2025, archived");
            var first = PutEntryRequest.newBuilder().setAddress(report).setTitle("Annual report 2025")
                    .setFilename("annual-2025.pdf").setContentType("application/pdf")
                    .setSourceUri("file:///share/finance/annual-report-2025.pdf")
                    .setSourceModifiedAt(date("2025-02-01T17:45:00Z"))
                    .addRenditions(rendition("original", "application/pdf", pdf))
                    .addRenditions(rendition("markdown", "text/markdown",
                            SyntheticBytes.markdown("Annual report 2025", "Revenue grew in every region.")))
                    .addRenditions(rendition("thumbnail", "image/x-portable-pixmap", SyntheticBytes.ppm(seed("thumb"), 8, 8)))
                    .setWrittenBy(writtenBy("corpus-seeder", "fable"))
                    .putMetadata("fiscal_year", "2025").putMetadata("slug", "annual-report-2025").build();
            var second = first.toBuilder().clearRenditions()
                    .addRenditions(rendition("original", "application/pdf", pdf))
                    .addRenditions(rendition("markdown", "text/markdown",
                            SyntheticBytes.markdown("Annual report 2025", "Revenue grew in every region; costs held flat.")))
                    .setSourceModifiedAt(date("2025-02-20T08:00:00Z")).build();
            var pages = entry("docs", "scans/contract-0042");
            var scan = PutEntryRequest.newBuilder().setAddress(pages).setTitle("Contract 42")
                    .setFilename("contract-0042.pdf").setContentType("application/pdf")
                    .addRenditions(rendition("original", "application/pdf", SyntheticBytes.pdf("Contract 42")))
                    .addRenditions(rendition("pages", "p1", "text/plain", SyntheticBytes.plainText("Page 1", "Whereas the parties agree.")))
                    .addRenditions(rendition("pages", "p2", "text/plain", SyntheticBytes.plainText("Page 2", "Signed and sealed.")))
                    .setWrittenBy(writtenBy("corpus-seeder", "fable")).build();
            return new ArchiveScript(archive("docs", VersioningPolicy.VERSIONING_POLICY_RETAINED, "Finance and legal documents"),
                    List.of(first, second, scan));
        }
    }

    // ------------------------------------------------------------------ corpus-beta

    /** Six documents: two share a title, one is tombstoned, one gets semantic parts, one is restricted. */
    private static final class Beta extends AccountScripts {
        Beta(RepositoryCorpus corpus, Account account) {
            super(corpus, account);
        }

        @Override
        void write(List<DocumentScript> documents, List<ArchiveScript> archives) {
            documents.add(quarterly("quarterly-q1", "2025-01-08T09:00:00Z", "Q1", "en", "apac"));
            documents.add(quarterly("quarterly-q2", "2025-02-11T09:00:00Z", "Q2", "en", "emea"));
            documents.add(handbookDe());
            documents.add(pressRelease());
            documents.add(draftDeleted());
            documents.add(restrictedPayroll());
            archives.add(scansArchive());
        }

        /** Two documents with the same title and different dates, so title sorts tie-break on the address. */
        DocumentScript quarterly(String docId, String created, String quarter, String language, String region) {
            var address = address(docId, share);
            var csv = SyntheticBytes.csv(seed(docId), 6);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Quarterly update")
                            .setBody("Figures for " + quarter + ".").setDocumentType("CSV")
                            .setSourceUri("s3://share/quarterly/" + docId + ".csv").setSourceMimeType("text/csv")
                            .setLanguage(language).setCategory("finance").setCreationDate(date(created))
                            .putMetadata("quarter", quarter).putMetadata("sales_region", region))
                    .setBlobBag(oneBlob(blob("original", "text/csv", docId + ".csv", csv)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1)));
        }

        /** A German handbook on the pipeline drive; the chunker adds one segmentation and one set. */
        DocumentScript handbookDe() {
            var docId = "handbuch";
            var address = address(docId, crawler);
            var body = "Das Handbuch beschreibt die Inbetriebnahme und die Wartung der Anlage.";
            var html = SyntheticBytes.html("Handbuch", body);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Handbuch").setBody(body)
                            .setDocumentType("HTML").setSourceUri("file:///share/technik/handbuch.html")
                            .setSourceMimeType("text/html").setLanguage("de").setAuthor("Konrad Zuse")
                            .setCategory("engineering").setCreationDate(date("2024-05-21T07:30:00Z"))
                            .putMetadata("region", "CH").putMetadata("postal_code", "8001"))
                    .setBlobBag(oneBlob(blob("original", "text/html", "handbuch.html", html)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(crawler, true), coreBlobsParsed())
                    .setDrive(PIPELINE_DRIVE), 1);
            var sentences = bodySegmentation("sentences", body, 3);
            var chunk = stepSave(save(docId, address, 2, at(1), semanticFragment(List.of(sentences),
                    List.of(embeddingSet(sentences, "model2vec", "model2vec-base-8m", 4))),
                    stepOwner(CHUNKER_MODULE), List.of(putSegmentation("sentences"), putEmbeddings("sentences", "model2vec")))
                    .setDrive(PIPELINE_DRIVE), 2, CHUNKER_MODULE);
            return new DocumentScript(address, PIPELINE_DRIVE, List.of(create, chunk));
        }

        /** CORE and PARSED without any blob. */
        DocumentScript pressRelease() {
            var docId = "press-release-2025-03";
            var address = address(docId, crawler);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), false))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Press release")
                            .setBody("The company opens a new office in Lyon.").setDocumentType("HTML")
                            .setSourceUri("https://example.com/press/2025-03").setSourceMimeType("text/html")
                            .setLanguage("fr").setAuthor("Marie Curie").setCategory("communications")
                            .setCreationDate(date("2025-03-03T08:00:00Z")).putMetadata("region", "FR"))
                    .putParserResults("readability", parsed("readability", "0.9")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(intakeSave(save(docId, address, 1, none(), document,
                    intakeOwner(crawler, false), List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_PARSED))), 1)));
        }

        /** Two revisions, then a tombstone. */
        DocumentScript draftDeleted() {
            var docId = "draft-memo";
            var address = address(docId, share);
            var text = SyntheticBytes.plainText("Draft memo", "First draft.");
            var metadata = SearchMetadata.newBuilder().setTitle("Draft memo").setBody("First draft.")
                    .setDocumentType("TXT").setSourceUri("s3://share/drafts/memo.txt").setSourceMimeType("text/plain")
                    .setLanguage("en").setCategory("drafts").setCreationDate(date("2025-02-25T16:00:00Z"));
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true)).setSearchMetadata(metadata)
                    .setBlobBag(oneBlob(blob("original", "text/plain", "memo.txt", text)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1);
            var revised = Document.newBuilder().setDocId(docId).setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(metadata.setBody("Second draft.").putMetadata("status", "superseded")).build();
            var revise = intakeSave(save(docId, address, 2, at(1), revised, intakeOwner(share, true),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE))), 2);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create, revise,
                    delete(docId, address, 3, 2, "draft abandoned")));
        }

        DocumentScript restrictedPayroll() {
            var docId = "payroll-2025-02";
            var address = address(docId, share);
            var csv = SyntheticBytes.csv(seed("payroll"), 4);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, restrictedSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Payroll").setBody("Four payroll lines.")
                            .setDocumentType("CSV").setSourceUri("s3://share/hr/payroll-2025-02.csv")
                            .setSourceMimeType("text/csv").setLanguage("en").setCategory("hr")
                            .setCreationDate(date("2025-02-27T18:00:00Z")))
                    .setBlobBag(oneBlob(blob("original", "text/csv", "payroll-2025-02.csv", csv)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1)));
        }

        /** An archive without retained versions: a second put replaces the first. */
        ArchiveScript scansArchive() {
            var invoice = entry("scans", "invoices/2025/0031");
            var first = PutEntryRequest.newBuilder().setAddress(invoice).setTitle("Invoice 31")
                    .setFilename("invoice-0031.pdf").setContentType("application/pdf")
                    .addRenditions(rendition("original", "application/pdf", SyntheticBytes.pdf("Invoice 31")))
                    .setWrittenBy(writtenBy("scanner", "")).build();
            var second = first.toBuilder().clearRenditions()
                    .addRenditions(rendition("original", "application/pdf", SyntheticBytes.pdf("Invoice 31, rescanned")))
                    .addRenditions(rendition("ocr", "text/plain", SyntheticBytes.plainText("Invoice 31", "Total 1,200.00 EUR"))).build();
            return new ArchiveScript(archive("scans", VersioningPolicy.VERSIONING_POLICY_NONE, "Scanned invoices"),
                    List.of(first, second));
        }
    }

    // ------------------------------------------------------------------ corpus-gamma

    /** The smallest account: four documents and no archive. */
    private static final class Gamma extends AccountScripts {
        Gamma(RepositoryCorpus corpus, Account account) {
            super(corpus, account);
        }

        @Override
        void write(List<DocumentScript> documents, List<ArchiveScript> archives) {
            documents.add(coreOnly());
            documents.add(fieldNotes());
            documents.add(deletedScan());
            documents.add(policyRevised());
        }

        /** CORE alone: no blob, no parser result, no language, no category. */
        DocumentScript coreOnly() {
            var docId = "note-0001";
            var address = address(docId, crawler);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Note").setBody("A single line."))
                    .build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(intakeSave(save(docId, address, 1, none(), document,
                    intakeOwner(crawler, true), List.of(put(DocumentPart.DOCUMENT_PART_CORE))), 1)));
        }

        DocumentScript fieldNotes() {
            var docId = "field-notes-es";
            var address = address(docId, share);
            var text = SyntheticBytes.plainText("Notas de campo", "Lluvia por la mañana, sol por la tarde.");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Notas de campo")
                            .setBody("Lluvia por la mañana, sol por la tarde.").setDocumentType("TXT")
                            .setSourceUri("s3://share/campo/notas.txt").setSourceMimeType("text/plain")
                            .setLanguage("es").setAuthor("Ramón y Cajal").setCategory("research")
                            .setCreationDate(date("2024-12-12T12:12:12Z"))
                            .putMetadata("region", "ES").putMetadata("postal_code", "28001"))
                    .setBlobBag(oneBlob(blob("original", "text/plain", "notas.txt", text)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            return new DocumentScript(address, INTAKE_DRIVE, List.of(
                    intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1)));
        }

        DocumentScript deletedScan() {
            var docId = "scan-0099";
            var address = address(docId, crawler);
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(crawler, openSecurity(), false))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Scan 99").setDocumentType("PPM")
                            .setSourceMimeType("image/x-portable-pixmap").setCategory("media"))
                    .setBlobBag(oneBlob(blob("original", "image/x-portable-pixmap", "0099.ppm",
                            SyntheticBytes.ppm(seed("scan"), 4, 4))))
                    .build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(crawler, false),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_BLOBS))), 1);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create, delete(docId, address, 2, 1, "duplicate scan")));
        }

        /** A policy whose blob is replaced in revision 2 and whose parser result is replaced in revision 3. */
        DocumentScript policyRevised() {
            var docId = "policy-travel";
            var address = address(docId, share);
            var v1 = SyntheticBytes.pdf("Travel policy v1");
            var v2 = SyntheticBytes.pdf("Travel policy v2");
            var metadata = SearchMetadata.newBuilder().setTitle("Travel policy").setBody("Economy class below six hours.")
                    .setDocumentType("PDF").setSourceUri("s3://share/policies/travel.pdf").setSourceMimeType("application/pdf")
                    .setLanguage("en").setCategory("policy").setCreationDate(date("2024-08-01T09:00:00Z"))
                    .putMetadata("version", "1");
            var document = Document.newBuilder().setDocId(docId)
                    .setOwnership(ownership(share, openSecurity(), true)).setSearchMetadata(metadata)
                    .setBlobBag(oneBlob(checksummedBlob("original", "application/pdf", "travel.pdf", v1)))
                    .putParserResults("tika", parsed("tika", "3.1")).build();
            var create = intakeSave(save(docId, address, 1, none(), document, intakeOwner(share, true), coreBlobsParsed()), 1);
            var replaced = Document.newBuilder().setDocId(docId).setOwnership(ownership(share, openSecurity(), true))
                    .setSearchMetadata(metadata.setLastModifiedDate(date("2025-01-15T09:00:00Z")).putMetadata("version", "2"))
                    .setBlobBag(oneBlob(checksummedBlob("original", "application/pdf", "travel.pdf", v2))).build();
            var swap = intakeSave(save(docId, address, 2, at(1), replaced, intakeOwner(share, true),
                    List.of(put(DocumentPart.DOCUMENT_PART_CORE), put(DocumentPart.DOCUMENT_PART_BLOBS))), 2);
            var reparsed = Document.newBuilder().putParserResults("docling", parsed("docling", "2.4")).build();
            var parse = stepSave(save(docId, address, 3, at(2), reparsed, stepOwner(PARSER_MODULE),
                    List.of(put(DocumentPart.DOCUMENT_PART_PARSED))), 3, PARSER_MODULE);
            return new DocumentScript(address, INTAKE_DRIVE, List.of(create, swap, parse));
        }
    }

    /** Every message the corpus holds, in load order, for callers that check them all. */
    public List<Message> messages() {
        var messages = new ArrayList<Message>();
        for (var script : documents) {
            for (var step : script.steps()) {
                messages.add(switch (step) {
                    case Save save -> save.request();
                    case Delete delete -> delete.request();
                });
            }
        }
        for (var script : archives) {
            messages.add(script.archive());
            messages.addAll(script.entries());
        }
        return List.copyOf(messages);
    }

    /** Blob digests by document and blob ID, inline and streamed alike, for the determinism checks. */
    public Map<String, String> blobDigests() {
        var digests = new LinkedHashMap<String, String>();
        for (var script : documents) {
            for (var step : script.steps()) {
                if (!(step instanceof Save save)) continue;
                var bag = save.request().getDocument().getBlobBag();
                var blobs = bag.hasBlob() ? List.of(bag.getBlob()) : bag.getBlobs().getBlobList();
                for (var blob : blobs) {
                    if (!blob.hasData()) continue;
                    digests.put(script.address().getAccountId() + "/" + script.address().getDocId() + "/" + blob.getBlobId(),
                            SyntheticBytes.sha256(blob.getData().toByteArray()));
                }
                save.streamed().ifPresent(streamed -> digests.put(script.address().getAccountId() + "/"
                        + script.address().getDocId() + "/" + streamed.blobId(), streamed.sha256()));
            }
        }
        return Map.copyOf(digests);
    }

    /** The type URL the typed payloads in the corpus carry. */
    public static String structuredDataTypeUrl() {
        return SEARCH_METADATA_TYPE_URL;
    }
}
