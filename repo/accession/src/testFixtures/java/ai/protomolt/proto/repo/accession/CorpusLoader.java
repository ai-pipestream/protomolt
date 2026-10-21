package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.StepWriterScope;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Writes a {@link RepositoryCorpus} into a catalog and its drives through
 * {@link DocumentSaves} and {@link DocumentDeletes}, under a {@link SteppedClock}
 * so every commit lands at its own fixed time. Intake saves and deletes run with
 * process authority; step saves run as the corpus step writers, whose
 * credentials and grants the loader installs first. Every save must produce the
 * revision the script expects, and a replayed save is a failure: the corpus is
 * written into a store that does not hold it yet.
 */
public final class CorpusLoader {

    /** Principal name of the process-authority caller that writes the intake saves. */
    public static final String LOADER_PRINCIPAL = "corpus-loader";

    /** What a load did. */
    public record Loaded(RepositoryCorpus corpus, int saves, int deletes, Instant firstCommit, Instant lastCommit) {}

    private CorpusLoader() {}

    /**
     * Loads the corpus.
     *
     * @param corpus the corpus
     * @param catalog the catalog to write into
     * @param drives drive resolution for every account of the corpus; must resolve {@code intake} and {@code pipeline}
     * @param resolver schema admission; must admit {@link RepositoryCorpus#structuredDataTypeUrl()}
     * @param limits the admission limits
     * @param schemas the container document schema supplier
     * @param clock the stepped clock the saves and deletes commit under
     * @return what was written
     */
    public static Loaded load(RepositoryCorpus corpus, Catalog catalog, DocumentSaves.Drives drives,
            DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Limits limits, DocumentSchemas schemas,
            SteppedClock clock) {
        Objects.requireNonNull(corpus, "corpus");
        Objects.requireNonNull(clock, "clock");
        var saves = new DocumentSaves(catalog, drives, resolver, limits, bytes -> () -> {}, clock, schemas);
        var deletes = new DocumentDeletes(catalog, clock);
        installWriters(catalog, corpus);
        int saved = 0;
        int deleted = 0;
        Instant first = null;
        Instant last = null;
        for (var script : corpus.documents()) {
            var account = script.address().getAccountId();
            var docId = script.address().getDocId();
            for (var step : script.steps()) {
                switch (step) {
                    case RepositoryCorpus.Save save -> {
                        var caller = save.stepModule().map(module -> stepCaller(corpus, module, account))
                                .orElseGet(CorpusLoader::processAuthority);
                        var response = save.streamed().isPresent()
                                ? upload(saves, caller, save)
                                : saves.save(caller, save.request());
                        if (response.getReplayed()) {
                            throw new IllegalStateException("Save " + save.request().getOperationId() + " of " + account
                                    + "/" + docId + " was replayed: the store already holds this corpus");
                        }
                        requireRevision(account, docId, save.expectedRevision(), response.getAccession().getRevision());
                        var committed = instant(response.getAccession().getCommittedAt());
                        first = first == null ? committed : first;
                        last = committed;
                        saved++;
                    }
                    case RepositoryCorpus.Delete delete -> {
                        var response = deletes.delete(processAuthority(), delete.request());
                        if (response.getReplayed()) {
                            throw new IllegalStateException("Delete " + delete.request().getOperationId() + " of " + account
                                    + "/" + docId + " was replayed: the store already holds this corpus");
                        }
                        requireRevision(account, docId, delete.expectedRevision(), response.getAccession().getRevision());
                        last = instant(response.getAccession().getCommittedAt());
                        deleted++;
                    }
                }
            }
        }
        return new Loaded(corpus, saved, deleted, first, last);
    }

    private static SaveDocumentResponse upload(DocumentSaves saves, RepositoryCaller caller, RepositoryCorpus.Save save) {
        var streamed = save.streamed().orElseThrow();
        var header = UploadHeader.newBuilder().setSave(save.request()).setBlobId(streamed.blobId())
                .setDeclaredLength(streamed.length()).setExpectedSha256(streamed.sha256()).build();
        try (var body = streamed.body().get()) {
            return saves.upload(caller, header, body).getSaved();
        } catch (IOException failure) {
            throw new IllegalStateException("Closing the streamed blob " + streamed.blobId() + " failed", failure);
        }
    }

    private static void requireRevision(String account, String docId, long expected, long actual) {
        if (expected != actual) {
            throw new IllegalStateException("Document " + account + "/" + docId + " committed revision " + actual
                    + " where the corpus expects " + expected);
        }
    }

    private static Instant instant(com.google.protobuf.Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    /**
     * Registers the corpus step writers' credentials and installs their step
     * grants in every corpus account. Idempotent: a credential or grant that is
     * already there is left alone.
     */
    public static void installWriters(Catalog catalog, RepositoryCorpus corpus) {
        for (var writer : corpus.stepWriters()) {
            if (catalog.credential(RepositoryCorpus.ISSUER, writer.credentialId()).isEmpty()) {
                catalog.registerCredential(RepositoryCorpus.ISSUER, writer.credentialId(), writer.module());
            }
            for (var account : corpus.accounts()) {
                var grantId = grantId(account.accountId(), writer);
                var installed = catalog.grants(account.accountId(), RepositoryCorpus.ISSUER, writer.credentialId()).stream()
                        .anyMatch(grant -> grant.grant().getGrantId().equals(grantId));
                if (installed) continue;
                catalog.installGrant(WriterGrant.newBuilder().setGrantId(grantId).setAccountId(account.accountId())
                        .setCredential(WriterCredential.newBuilder().setIssuer(RepositoryCorpus.ISSUER)
                                .setCredentialId(writer.credentialId().toString()))
                        .setStep(StepWriterScope.newBuilder().setModuleId(writer.module())).build());
            }
        }
    }

    /** The grant ID of a step writer in an account; it names the credential, so two corpora with different seeds coexist. */
    public static String grantId(String accountId, RepositoryCorpus.StepWriter writer) {
        return "corpus-" + accountId + "-" + writer.module() + "-" + writer.credentialId();
    }

    /** The process-authority caller the intake saves and the deletes run as. */
    public static RepositoryCaller processAuthority() {
        return new RepositoryCaller(LOADER_PRINCIPAL, true);
    }

    /** The credential-bound caller of a step module in one account. */
    public static RepositoryCaller stepCaller(RepositoryCorpus corpus, String module, String accountId) {
        var writer = corpus.stepWriters().stream().filter(candidate -> candidate.module().equals(module)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("The corpus has no step writer " + module));
        return new RepositoryCaller(module, false, Set.of(accountId),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity(module).build()),
                Optional.of(new RepositoryCredentialBinding(RepositoryCorpus.ISSUER, writer.credentialId(), 1)));
    }

    /** The account reader: bound to one account, holding its reader service identity. */
    public static RepositoryCaller reader(String accountId) {
        return new RepositoryCaller(RepositoryCorpus.readerIdentity(accountId), false, Set.of(accountId),
                Set.of(Principal.newBuilder().setIdentityType("service")
                        .setIdentity(RepositoryCorpus.readerIdentity(accountId)).build()));
    }

    /** A user principal bound to one account, for ACL checks. */
    public static RepositoryCaller user(String accountId, String upn) {
        return new RepositoryCaller(upn, false, Set.of(accountId),
                Set.of(Principal.newBuilder().setIdentityType("user-principal-name").setIdentity(upn).build()));
    }

    /**
     * The catalog state a load leaves behind, with the stack references cleared:
     * every revision's stored manifest of every corpus document, in load order,
     * then every account's catalog entries. Object keys are random per upload,
     * so two loads of the same corpus differ in them and nowhere else.
     */
    public static List<com.google.protobuf.Message> normalizedState(Catalog catalog, RepositoryCorpus corpus) {
        var state = new ArrayList<com.google.protobuf.Message>();
        for (var script : corpus.documents()) {
            for (long revision = 1; revision <= script.finalRevision(); revision++) {
                var manifest = catalog.manifest(script.address(), revision).orElseThrow(() -> new IllegalStateException(
                        "No manifest for " + script.address().getAccountId() + "/" + script.address().getDocId()));
                state.add(withoutRefs(manifest));
            }
        }
        for (var account : corpus.accounts()) {
            for (var row : listAll(catalog, account.accountId())) state.add(row.entry());
        }
        return List.copyOf(state);
    }

    /** Every catalog row of an account, tombstones included, in address order. */
    public static List<CatalogRecords.ListedDocument> listAll(Catalog catalog, String accountId) {
        var rows = new ArrayList<CatalogRecords.ListedDocument>();
        Optional<CatalogRecords.Cursor> after = Optional.empty();
        while (true) {
            var page = catalog.documents(CatalogRecords.Listing.of(accountId, "", "", true), after, 100);
            rows.addAll(page);
            if (page.size() < 100) return rows;
            after = Optional.of(page.getLast().cursor());
        }
    }

    private static StoredManifest withoutRefs(StoredManifest manifest) {
        var stripped = manifest.toBuilder();
        for (int i = 0; i < stripped.getPartsCount(); i++) {
            var part = stripped.getPartsBuilder(i).clearRef();
            for (int j = 0; j < part.getBlobRefsCount(); j++) part.getBlobRefsBuilder(j).clearRef();
        }
        return stripped.build();
    }

    /** The catalog entry of a document's current revision, for assertions on the lookup projection. */
    public static Optional<CatalogEntry> entry(Catalog catalog, String accountId, String docId) {
        return listAll(catalog, accountId).stream().filter(row -> row.address().getDocId().equals(docId))
                .map(CatalogRecords.ListedDocument::entry).findFirst();
    }
}
