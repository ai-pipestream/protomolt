package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;

/** Administrative raw-byte operations. Document ownership is enforced on the document surface. */
public interface BlobRepository {
    GetBlobResponse get(RepositoryCaller caller, GetBlobRequest request);
    PutBlobResponse put(RepositoryCaller caller, PutBlobRequest request);
    DeleteBlobResponse delete(RepositoryCaller caller, DeleteBlobRequest request);
    GetBlobForUpdateResponse getForUpdate(RepositoryCaller caller, GetBlobForUpdateRequest request);
    CompareAndPutBlobResponse compareAndPut(RepositoryCaller caller, CompareAndPutBlobRequest request);
}
