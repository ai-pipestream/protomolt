package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the procedures the console reaches through the Connect gateway. A change to the list
 * is a change to what a browser can call, so it is spelled out here in full.
 */
class ConsoleProceduresTest {

    @Test
    void theConsoleReachesExactlyTheseProcedures() {
        assertThat(ConsoleProcedures.ALL).containsExactlyInAnyOrder(
                "ai.protomolt.proto.repo.v2.DocumentService/ListDocuments",
                "ai.protomolt.proto.repo.v2.DocumentService/GetManifest",
                "ai.protomolt.proto.repo.v2.DocumentService/GetDocument",
                "ai.protomolt.proto.repo.v2.DocumentService/ListRevisions",
                "ai.protomolt.proto.repo.v2.DocumentService/ReadDocument",
                "ai.protomolt.proto.repo.v2.DocumentService/GetSchemaDescriptor",
                "ai.protomolt.proto.repo.v2.DocumentService/ReadBlob",
                "ai.protomolt.proto.repo.v2.DocumentService/GetOperationOutcome",
                "ai.protomolt.proto.repo.v2.DocumentService/DeleteDocument",
                "ai.protomolt.proto.repo.v1.DriveService/ListDrives",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/ListArchives",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/GetArchive",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/GetArchiveStats",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/ListEntries",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/GetEntryManifest",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/ListVersions",
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/ReadRendition");
    }

    @Test
    void everyProcedureIsAUnaryOrServerStreamingMethodTheGatewayCanServe() {
        Map<String, MethodDescriptor.MethodType> types = new HashMap<>();
        for (ServiceDescriptor service : List.of(DocumentServiceGrpc.getServiceDescriptor(),
                DriveServiceGrpc.getServiceDescriptor(), ArchiveServiceGrpc.getServiceDescriptor())) {
            for (MethodDescriptor<?, ?> method : service.getMethods()) {
                types.put(method.getFullMethodName(), method.getType());
            }
        }
        for (String procedure : ConsoleProcedures.ALL) {
            assertThat(types.get(procedure)).as(procedure)
                    .isIn(MethodDescriptor.MethodType.UNARY, MethodDescriptor.MethodType.SERVER_STREAMING);
        }
    }
}
