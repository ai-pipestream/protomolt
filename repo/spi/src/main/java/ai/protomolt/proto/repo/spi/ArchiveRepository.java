package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import java.io.InputStream;
import java.io.IOException;

/** Archive operations over existing wire contracts and trusted host-resolved identity. */
public interface ArchiveRepository {
    record UploadResult(String entryUuid, long version, String sha256, long sizeBytes,
                        String objectKey, String rootChecksum, boolean deduplicated) {}

    CreateArchiveResponse createArchive(RepositoryCaller caller, CreateArchiveRequest request);
    GetArchiveResponse getArchive(RepositoryCaller caller, GetArchiveRequest request);
    ListArchivesResponse listArchives(RepositoryCaller caller, ListArchivesRequest request);
    GetArchiveStatsResponse stats(RepositoryCaller caller, GetArchiveStatsRequest request);
    PutEntryResponse putEntry(RepositoryCaller caller, PutEntryRequest request);
    GetEntryResponse getEntry(RepositoryCaller caller, GetEntryRequest request);
    GetEntryManifestResponse getManifest(RepositoryCaller caller, GetEntryManifestRequest request);
    ListEntriesResponse listEntries(RepositoryCaller caller, ListEntriesRequest request);
    ListVersionsResponse listVersions(RepositoryCaller caller, ListVersionsRequest request);
    DeleteEntryResponse deleteEntry(RepositoryCaller caller, DeleteEntryRequest request);
    DeleteRenditionResponse deleteRendition(RepositoryCaller caller, DeleteRenditionRequest request);
    PruneVersionsResponse pruneVersions(RepositoryCaller caller, PruneVersionsRequest request);
    ClassifyEntryResponse classifyEntry(RepositoryCaller caller, ClassifyEntryRequest request);
    BridgeEntryResponse bridgeEntry(RepositoryCaller caller, BridgeEntryRequest request);

    /** Consumes the body synchronously and may close it; callers must also close on failure. */
    UploadResult uploadStream(RepositoryCaller caller, EntryAddress address, RenditionDescriptor descriptor,
                              long size, String sha256, WriteAttribution attribution, String filename,
                              FormatFact declared, ObjectStoreOrigin origin, InputStream body) throws IOException;
}
