package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;

/** Drive operations with identity supplied by a trusted host. */
public interface DriveRepository {
    CreateDriveResponse createDrive(RepositoryCaller caller, CreateDriveRequest request);
    GetDriveResponse getDrive(RepositoryCaller caller, GetDriveRequest request);
    ListDrivesResponse listDrives(RepositoryCaller caller, ListDrivesRequest request);
}
