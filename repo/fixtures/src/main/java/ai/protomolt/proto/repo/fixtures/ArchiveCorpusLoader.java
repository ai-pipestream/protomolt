package ai.protomolt.proto.repo.fixtures;

import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Writes the archive part of a {@link RepositoryCorpus} through the real
 * {@link ArchiveRepository}: creates each archive on the account's archive
 * drive and puts its entries in script order, so a repeated address writes a
 * retained version or a replacement as the archive's policy says. The archive
 * drive must exist (see {@link CorpusSeeder}). Entry timestamps are the wall
 * clock: the archive operations have no clock seam.
 */
public final class ArchiveCorpusLoader {

    /** One put and its answer. */
    public record Written(String accountId, String archive, String entryId, PutEntryResponse response) {}

    private ArchiveCorpusLoader() {}

    /**
     * Loads every archive script of the corpus.
     *
     * @param corpus the corpus
     * @param archives the archive repository
     * @param caller a process-authority caller; archive creation requires it
     * @return every put, in order
     */
    public static List<Written> load(RepositoryCorpus corpus, ArchiveRepository archives, RepositoryCaller caller) {
        Objects.requireNonNull(corpus, "corpus");
        Objects.requireNonNull(archives, "archives");
        var written = new ArrayList<Written>();
        for (var script : corpus.archives()) {
            var archive = script.archive();
            if (!exists(archives, caller, archive.getAccountId(), archive.getName())) {
                archives.createArchive(caller, CreateArchiveRequest.newBuilder().setArchive(archive).build());
            }
            for (var entry : script.entries()) {
                var response = archives.putEntry(caller, entry);
                written.add(new Written(archive.getAccountId(), archive.getName(), entry.getAddress().getEntryId(), response));
            }
        }
        return List.copyOf(written);
    }

    private static boolean exists(ArchiveRepository archives, RepositoryCaller caller, String accountId, String name) {
        try {
            archives.getArchive(caller, GetArchiveRequest.newBuilder().setAccountId(accountId).setArchive(name).build());
            return true;
        } catch (RepositoryException failure) {
            if (failure.code() == RepositoryException.Code.NOT_FOUND) return false;
            throw failure;
        }
    }
}
