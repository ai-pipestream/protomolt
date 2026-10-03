package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.DriveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.engine.DriveOperations;
import ai.protomolt.proto.repo.engine.DriveProvisioner;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.blob.s3.S3NamespaceProvisioner;
import ai.protomolt.proto.repo.v1.*;
import software.amazon.awssdk.services.s3.S3Client;
import io.grpc.stub.StreamObserver;

/** gRPC adapter for shared drive operations. */
public final class DriveGrpcService extends DriveServiceGrpc.DriveServiceImplBase {
    private final DriveRepository operations;

    /** Retains the existing S3 composition entry point; the supplied client remains caller-owned. */
    public DriveGrpcService(DriveLedger drives, S3Client s3, String bucketBase, String region) {
        this(drives, new DriveProvisioner(drives, new S3NamespaceProvisioner(s3), bucketBase, region, "s3"));
    }

    DriveGrpcService(DriveLedger drives, DriveProvisioner provisioner) {
        this(new DriveOperations(drives, provisioner));
    }

    public DriveGrpcService(DriveRepository operations) {
        this.operations = java.util.Objects.requireNonNull(operations);
    }

    private static RepositoryCaller caller() {
        var caller = ai.protomolt.proto.authz.grpc.CallerContexts.current();
        return new RepositoryCaller(caller.name(), caller.unrestricted());
    }

    @Override public void createDrive(CreateDriveRequest request, StreamObserver<CreateDriveResponse> observer) {
        GrpcErrors.run(observer, () -> operations.createDrive(caller(), request));
    }

    @Override public void getDrive(GetDriveRequest request, StreamObserver<GetDriveResponse> observer) {
        GrpcErrors.run(observer, () -> operations.getDrive(caller(), request));
    }

    @Override public void listDrives(ListDrivesRequest request, StreamObserver<ListDrivesResponse> observer) {
        GrpcErrors.run(observer, () -> operations.listDrives(caller(), request));
    }
}
