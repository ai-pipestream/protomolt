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

    /**
     * An open rendition read. {@code body} yields exactly {@code length} bytes from
     * {@code offset} and then end of stream; when {@code wholeObject} is set it also
     * checks the manifest's SHA-256 before reporting end of stream. A mismatch, or a
     * provider stream that ends early or runs long, fails the read with
     * {@code DATA_LOSS}. The caller closes {@code body} on every path; closing it
     * releases the storage read pin and abandons an unfinished provider transfer.
     *
     * @param version the version read
     * @param rendition the rendition's manifest entry
     * @param offset the first byte served
     * @param length the bytes served
     * @param wholeObject whether the range covers the whole object
     * @param body the bytes of the range
     */
    record RenditionStream(long version, RenditionManifestEntry rendition, long offset, long length,
                           boolean wholeObject, InputStream body) {}

    CreateArchiveResponse createArchive(RepositoryCaller caller, CreateArchiveRequest request);
    GetArchiveResponse getArchive(RepositoryCaller caller, GetArchiveRequest request);
    ListArchivesResponse listArchives(RepositoryCaller caller, ListArchivesRequest request);
    GetArchiveStatsResponse stats(RepositoryCaller caller, GetArchiveStatsRequest request);
    PutEntryResponse putEntry(RepositoryCaller caller, PutEntryRequest request);
    GetEntryResponse getEntry(RepositoryCaller caller, GetEntryRequest request);
    GetEntryManifestResponse getManifest(RepositoryCaller caller, GetEntryManifestRequest request);
    ListEntriesResponse listEntries(RepositoryCaller caller, ListEntriesRequest request);
    ListVersionsResponse listVersions(RepositoryCaller caller, ListVersionsRequest request);
    ClassifyEntryResponse classifyEntry(RepositoryCaller caller, ClassifyEntryRequest request);
    BridgeEntryResponse bridgeEntry(RepositoryCaller caller, BridgeEntryRequest request);

    /**
     * Opens a streamed read of one present rendition object of an entry version,
     * whole or as a byte range, without buffering the object.
     *
     * @param caller the trusted caller
     * @param request the read
     * @return the open read; the caller closes its body
     */
    RenditionStream readRendition(RepositoryCaller caller, ReadRenditionRequest request);

    /** Consumes the body synchronously and may close it; callers must also close on failure. */
    UploadResult uploadStream(RepositoryCaller caller, EntryAddress address, RenditionDescriptor descriptor,
                              long size, String sha256, WriteAttribution attribution, String filename,
                              FormatFact declared, ObjectStoreOrigin origin, InputStream body) throws IOException;
}
