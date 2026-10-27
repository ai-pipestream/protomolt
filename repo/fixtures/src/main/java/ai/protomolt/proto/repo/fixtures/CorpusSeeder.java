package ai.protomolt.proto.repo.fixtures;

import ai.protomolt.proto.repo.accession.CorpusLoader;
import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.accession.SteppedClock;
import ai.protomolt.proto.repo.archive.ArchiveCoordinates;
import ai.protomolt.proto.repo.archive.DocumentArchiveBindings;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes the {@link RepositoryCorpus} into a running repository composition:
 * the three accounts' drives, the documents through the v2 save path under a
 * stepped clock, and the archives through the archive operations. Before
 * writing anything it checks every corpus account with {@link CorpusAccountGuard}:
 * an account holding documents the corpus did not create refuses the whole run
 * by name, and a store that already holds the corpus is left as it is.
 */
public final class CorpusSeeder {

    /** Principal name of the process-authority caller the seeder provisions and writes with. */
    public static final String SEEDER_PRINCIPAL = "corpus-seeder";

    /** What a run did. */
    public record Report(boolean written, Map<String, CorpusAccountGuard.Decision> decisions, int saves, int deletes,
            int archiveEntries) {}

    private CorpusSeeder() {}

    /**
     * Seeds the corpus into the composition.
     *
     * @param services the repository composition to write into; its catalog, drives and schemas are used as they are
     * @param corpus the corpus
     * @return what was written, or that nothing was
     * @throws CorpusAccountGuard.ForeignDocuments when a corpus account holds documents the corpus did not create
     */
    public static Report seed(RepoServices services, RepositoryCorpus corpus) {
        Objects.requireNonNull(services, "services");
        Objects.requireNonNull(corpus, "corpus");
        var caller = new RepositoryCaller(SEEDER_PRINCIPAL, true);
        var v2 = services.v2Composition();
        var decisions = new LinkedHashMap<String, CorpusAccountGuard.Decision>();
        for (var account : corpus.accounts()) {
            var existing = CorpusLoader.listAll(v2.catalog(), account.accountId()).stream()
                    .map(CatalogRecords.ListedDocument::address).toList();
            decisions.put(account.accountId(), CorpusAccountGuard.check(corpus, account.accountId(), existing));
        }
        var toWrite = decisions.entrySet().stream().filter(entry -> entry.getValue() == CorpusAccountGuard.Decision.WRITE)
                .map(Map.Entry::getKey).toList();
        if (toWrite.isEmpty()) {
            return new Report(false, Map.copyOf(decisions), 0, 0, 0);
        }
        if (toWrite.size() != decisions.size()) {
            var seeded = new ArrayList<>(decisions.keySet());
            seeded.removeAll(toWrite);
            throw new IllegalStateException("The corpus is already in " + seeded + " but not in " + toWrite
                    + "; the seeder writes all of its accounts or none, clear the seeded accounts first");
        }
        for (var account : corpus.accounts()) {
            ensureDrive(services, caller, account.accountId(), RepositoryCorpus.INTAKE_DRIVE, DriveType.DRIVE_TYPE_INTAKE);
            ensureDrive(services, caller, account.accountId(), RepositoryCorpus.PIPELINE_DRIVE, DriveType.DRIVE_TYPE_PIPELINE);
            ensureDrive(services, caller, account.accountId(), RepositoryCorpus.ARCHIVE_DRIVE, DriveType.DRIVE_TYPE_CUSTOM);
        }
        var loaded = CorpusLoader.load(corpus, v2.catalog(), v2.drives(), v2.resolver(), v2.limits(), v2.schemas(),
                new SteppedClock(RepositoryCorpus.EPOCH, RepositoryCorpus.STEP));
        var entries = ArchiveCorpusLoader.load(corpus, services.archiveRepository(), caller);
        return new Report(true, Map.copyOf(decisions), loaded.saves(), loaded.deletes(), entries.size());
    }

    /** The drive provisioner is idempotent: an existing drive of the same name is found, not duplicated. */
    private static void ensureDrive(RepoServices services, RepositoryCaller caller, String accountId, String name, DriveType type) {
        services.driveRepository().createDrive(caller, CreateDriveRequest.newBuilder()
                .setAccountId(accountId).setName(name).setDriveType(type).build());
    }

    /** Every distinct v2 address the corpus writes, including archive definitions and entries. */
    public static List<NodeAddress> addresses(RepositoryCorpus corpus, String accountId) {
        var addresses = new LinkedHashSet<NodeAddress>();
        corpus.documents(accountId).forEach(script -> addresses.add(script.address()));
        for (var script : corpus.archives()) {
            var archive = script.archive();
            if (!archive.getAccountId().equals(accountId)) continue;
            addresses.add(DocumentArchiveBindings.definitionAddress(accountId, archive.getName()));
            script.entries().forEach(entry -> addresses.add(ArchiveCoordinates.address(entry.getAddress())));
        }
        return List.copyOf(addresses);
    }
}
