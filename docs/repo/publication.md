# Save and read document parts

`DocumentService` is the canonical public document API. A document is split into
protobuf parts (CORE, BLOBS, PARSED and chunk sets). Repository storage owns the
content bytes; SQL records ledger state, object references and lookup metadata.

Use `SaveDocument` for a full save or select `parts_written` for a partial save.
For a partial save, `copy_unwritten_parts_from` identifies the source whose
untouched parts must survive. `chunk_sets_written` refines CHUNKS replacement.
An omitted untouched part is not an instruction to delete it.

Use `GetDocument` or `GetDocumentByReference` with `parts` and `chunk_sets` to
request selected content. Empty selection requests full assembly. Consult the
[protobuf contract](../../repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/document_service.proto)
for required fields and the [repository guide](../../repo/README.md)
for host and storage configuration.

Large original source files use the existing streaming HTTP upload route and
`FileStorageReference` claim checks. Unary raw-blob and buffered document paths
have different resource constraints; their limits do not define a document-size
requirement or establish large-document qualification.

## Validation and retained implementation

Descriptor-driven part assembly and CEL/ProtoValidator admission are separate
operations. The internal admission, publication, receipt and recovery libraries
remain available for integration and qualification. Their existence does not
prove that every `DocumentService` save invokes typed admission. That call path
must be traced and tested before claiming full contract enforcement.

The separate `DocumentPublicationService` and remote publication-client module
are removed. They must not be advertised as a second supported public document
API. The prior guide remains available in Git history.
Historical test reports establish behavior at their recorded commits, not the
current public API or a substitute for selective hydration and part updates.
