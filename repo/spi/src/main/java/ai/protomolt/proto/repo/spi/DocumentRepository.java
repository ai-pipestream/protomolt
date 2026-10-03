package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;

/** Shared document operations using existing wire contracts and trusted caller identity. */
public interface DocumentRepository {
    SaveDocumentResponse saveDocument(RepositoryCaller caller, SaveDocumentRequest request);
    GetDocumentResponse getDocument(RepositoryCaller caller, GetDocumentRequest request);
    GetDocumentResponse getDocumentByReference(RepositoryCaller caller, GetDocumentByReferenceRequest request);
    GetDocumentManifestResponse getDocumentManifest(RepositoryCaller caller, GetDocumentManifestRequest request);
    DeleteDocumentResponse deleteDocument(RepositoryCaller caller, DeleteDocumentRequest request);
    ListDocumentsResponse listDocuments(RepositoryCaller caller, ListDocumentsRequest request);
}
