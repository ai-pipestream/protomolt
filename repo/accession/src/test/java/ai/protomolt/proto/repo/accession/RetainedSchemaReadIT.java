package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import java.sql.SQLTransientConnectionException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §15.14 and the retained-schema scenarios ported from #312 (comment 10797). A typed
 * read decodes with the schemas its revision retained in the catalog and never asks
 * a registry: an unreachable catalog is UNAVAILABLE, a retained artifact absent from
 * a reachable catalog or failing its digest is DATA_LOSS, and a stopped registry
 * changes nothing. Every case counts registry lookups during the read.
 */
class RetainedSchemaReadIT {
    private AccessionFixture f;
    private DocumentRegistry registry;
    private final AtomicReference<Map<String, DocumentSchemaAdmission.Definition>> payloads = new AtomicReference<>(Map.of());
    private DocumentSaves saves;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        registry = new DocumentRegistry();
        registry.registerDocument(DocumentRegistry.documentSource());
        DocumentSchemaAdmission.Resolver live = selection -> payloads.get().get(selection.typeUrl());
        saves = new DocumentSaves(f.catalog, f.drives, registry.resolver(live), limits(), bytes -> () -> {},
                Clock.systemUTC(), registry.schemas());
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    // ------------------------------------------------------------------ §15.14

    @Test void aMissingRetainedArtifactInAReachableCatalogIsDataLoss() throws Exception {
        var address = saved("doc-missing-row", noteV1("first"));
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        stopRegistry();
        // Each retained artifact the CORE read needs, removed in turn: the container's
        // descriptor closure and metadata, and the payload schema's closure.
        var core = coreBinding(stored);
        var payload = stored.getValidation().getResolvedOccurrences(0).getSchema();
        for (var sha : new String[] {core.getDescriptorSha256(), core.getMetadataSha256(), payload.getDescriptorSha256()}) {
            var row = remove(sha);
            assertDataLoss(f, address, 1);
            restore(sha, row);
        }
        assertThat(read(f, address, 1).getDocument().getStructuredData().getTypeUrl()).isEqualTo(NOTE_URL);
        assertThat(registry.lookups.get()).isEqualTo(lookups);
    }

    @Test void corruptRetainedArtifactBytesAreDataLoss() throws Exception {
        var address = saved("doc-corrupt-row", noteV1("first"));
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        stopRegistry();
        allowCorruption();
        for (var sha : new String[] {coreBinding(stored).getDescriptorSha256(),
                stored.getValidation().getResolvedOccurrences(0).getSchema().getDescriptorSha256()}) {
            var original = artifactBytes(sha);
            var damaged = original.clone();
            damaged[damaged.length / 2] ^= 1;
            setArtifactBytes(sha, damaged);
            assertDataLoss(f, address, 1);
            setArtifactBytes(sha, original);
        }
        assertThat(registry.lookups.get()).isEqualTo(lookups);
    }

    @Test void anUnreachableRetainedArtifactStoreIsUnavailable() throws Exception {
        var address = saved("doc-unreachable", noteV1("first"));
        stopRegistry();
        // The catalog answers revision lookups but cannot be reached for retained artifacts.
        var outage = new CatalogException("Schema artifact lookup failed",
                new SQLTransientConnectionException("connection refused", "08001"), true);
        var unreachable = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(), new Class<?>[] {Catalog.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("artifact")) throw outage;
                    try {
                        return method.invoke(f.catalog, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
        var reads = new DocumentReads(unreachable, f.drives, limits(), bytes -> () -> {}, Clock.systemUTC());
        assertThatThrownBy(() -> reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .build())).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(registry.lookups.get()).isEqualTo(lookups);
    }

    // ------------------------------------------------------------------ ported from #312

    // Scenario 1 and §15.14 case 4: a fresh host reads with no registry at all.
    @Test void aFreshHostDecodesFromRetainedSchemasWithTheRegistryStopped() throws Exception {
        var sent = noteV1("fresh host").toBuilder().setDocId("doc-fresh").build();
        var address = saved("doc-fresh", sent);
        stopRegistry();
        try (var restarted = f.restarted()) {
            var read = read(restarted, address, 1);
            assertThat(read.getDocument()).isEqualTo(claimChecked(sent, restarted.catalog.manifest(address, 1).orElseThrow()));
            assertThat(DynamicMessage.parseFrom(NOTE_V1, read.getDocument().getStructuredData().getValue())
                    .getField(NOTE_V1.findFieldByName("a"))).isEqualTo("fresh host");
        }
        assertThat(registry.lookups.get()).isEqualTo(lookups);
    }

    // Scenario 2: two revisions of one type URL stay with their own definitions.
    @Test void eachRevisionIsPinnedToItsOwnDefinitionOfOneTypeUrl() throws Exception {
        var address = saved("doc-pinned", noteV1("in v1"));
        payloads.set(Map.of(NOTE_URL, definition(NOTE_V2)));
        var second = document(account, "doc-pinned", "v2").toBuilder().setStructuredData(Any.newBuilder()
                .setTypeUrl(NOTE_URL).setValue(DynamicMessage.newBuilder(NOTE_V2).setField(NOTE_V2.findFieldByName("b"), "in v2")
                        .build().toByteString())).build();
        saves.save(processAuthority(), create("op-2", address, second).setExpected(at(1)).build());
        stopRegistry();

        var first = f.catalog.manifest(address, 1).orElseThrow().getValidation().getResolvedOccurrences(0).getSchema();
        var later = f.catalog.manifest(address, 2).orElseThrow().getValidation().getResolvedOccurrences(0).getSchema();
        assertThat(first.getTypeUrl()).isEqualTo(later.getTypeUrl());
        assertThat(first.getDescriptorSha256()).isNotEqualTo(later.getDescriptorSha256());
        assertThat(read(f, address, 1).getDocument().getStructuredData().getValue())
                .isEqualTo(noteV1("in v1").getStructuredData().getValue());
        assertThat(read(f, address, 2).getDocument().getStructuredData()).isEqualTo(second.getStructuredData());

        // Revision 1 does not lean on revision 2's definition: without it, only revision 2 fails.
        remove(later.getDescriptorSha256());
        assertThat(read(f, address, 1).getDocument().hasStructuredData()).isTrue();
        assertDataLoss(f, address, 2);
        assertThat(registry.lookups.get()).isEqualTo(lookups);
    }

    // Scenario 3: a closure over several files with a custom Any nested in the payload.
    @Test void aMultiFileClosureWithANestedCustomAnyDecodesFromRetainedSchemas() throws Exception {
        var sent = nested("nested payload").toBuilder().setDocId("doc-nested").build();
        var address = saved("doc-nested", sent);
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        assertThat(stored.getValidation().getResolvedOccurrencesList()).extracting(occurrence -> occurrence.getSchema().getTypeUrl())
                .containsExactlyInAnyOrder(OUTER_URL, LEAF_URL);
        stopRegistry();
        try (var restarted = f.restarted()) {
            var read = read(restarted, address, 1);
            assertThat(read.getDocument()).isEqualTo(claimChecked(sent, restarted.catalog.manifest(address, 1).orElseThrow()));
            var outer = DynamicMessage.parseFrom(OUTER, read.getDocument().getStructuredData().getValue());
            assertThat(((DynamicMessage) outer.getField(OUTER.findFieldByName("inner"))).getField(INNER.findFieldByName("text")))
                    .isEqualTo("nested payload");
        }
        assertThat(registry.lookups.get()).isEqualTo(lookups);
    }

    // Scenario 4: a missing nested retained descriptor is data loss, with no live substitution.
    @Test void aMissingNestedRetainedDescriptorIsDataLossWithoutLiveSubstitution() throws Exception {
        var address = saved("doc-nested-missing", nested("x"));
        var leaf = f.catalog.manifest(address, 1).orElseThrow().getValidation().getResolvedOccurrencesList().stream()
                .filter(occurrence -> occurrence.getSchema().getTypeUrl().equals(LEAF_URL)).findFirst().orElseThrow().getSchema();
        remove(leaf.getDescriptorSha256());
        // The live registry still has the leaf schema; the read must not use it.
        registry.lookups.set(0);
        lookups = 0;
        assertDataLoss(f, address, 1);
        assertThat(registry.lookups.get()).isZero();
    }

    // ------------------------------------------------------------------ schemas built at run time

    private static final String NOTE_URL = "type.googleapis.com/test.retained.Note";
    private static final String OUTER_URL = "type.googleapis.com/test.retained.nested.Outer";
    private static final String LEAF_URL = "type.googleapis.com/test.retained.leaf.Leaf";
    private static final Descriptor NOTE_V1 = message(file("test/retained/note.proto", "test.retained",
            DescriptorProto.newBuilder().setName("Note").addField(string("a", 1)).build()), "Note");
    private static final Descriptor NOTE_V2 = message(file("test/retained/note.proto", "test.retained",
            DescriptorProto.newBuilder().setName("Note").addField(string("b", 2)).build()), "Note");
    private static final FileDescriptor INNER_FILE = file("test/retained/nested/inner.proto", "test.retained.nested",
            DescriptorProto.newBuilder().setName("Inner").addField(string("text", 1)).build());
    private static final Descriptor INNER = message(INNER_FILE, "Inner");
    private static final Descriptor OUTER = message(file("test/retained/nested/outer.proto", "test.retained.nested",
            DescriptorProto.newBuilder().setName("Outer")
                    .addField(FieldDescriptorProto.newBuilder().setName("inner").setNumber(1)
                            .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.retained.nested.Inner"))
                    .addField(FieldDescriptorProto.newBuilder().setName("extra").setNumber(2)
                            .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Any")).build(),
            INNER_FILE, Any.getDescriptor().getFile()), "Outer");
    private static final Descriptor LEAF = message(file("test/retained/leaf/leaf.proto", "test.retained.leaf",
            DescriptorProto.newBuilder().setName("Leaf").addField(string("value", 1)).build()), "Leaf");

    private Document noteV1(String a) {
        payloads.set(Map.of(NOTE_URL, definition(NOTE_V1)));
        return document(account, "doc", "t").toBuilder().setStructuredData(Any.newBuilder().setTypeUrl(NOTE_URL)
                .setValue(DynamicMessage.newBuilder(NOTE_V1).setField(NOTE_V1.findFieldByName("a"), a).build().toByteString()))
                .build();
    }

    private Document nested(String text) {
        payloads.set(Map.of(OUTER_URL, definition(OUTER), LEAF_URL, definition(LEAF)));
        var leaf = DynamicMessage.newBuilder(LEAF).setField(LEAF.findFieldByName("value"), "leaf of " + text).build();
        var outer = DynamicMessage.newBuilder(OUTER)
                .setField(OUTER.findFieldByName("inner"), DynamicMessage.newBuilder(INNER).setField(INNER.findFieldByName("text"), text).build())
                .setField(OUTER.findFieldByName("extra"), DynamicMessage.newBuilder(Any.getDescriptor())
                        .setField(Any.getDescriptor().findFieldByName("type_url"), LEAF_URL)
                        .setField(Any.getDescriptor().findFieldByName("value"), leaf.toByteString()).build())
                .build();
        return document(account, "doc", "t").toBuilder()
                .setStructuredData(Any.newBuilder().setTypeUrl(OUTER_URL).setValue(outer.toByteString())).build();
    }

    private static FileDescriptor file(String name, String pkg, DescriptorProto message, FileDescriptor... dependencies) {
        var proto = FileDescriptorProto.newBuilder().setName(name).setPackage(pkg).setSyntax("proto3").addMessageType(message);
        for (var dependency : dependencies) proto.addDependency(dependency.getName());
        try {
            return FileDescriptor.buildFrom(proto.build(), dependencies);
        } catch (com.google.protobuf.Descriptors.DescriptorValidationException invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static Descriptor message(FileDescriptor file, String name) { return file.findMessageTypeByName(name); }

    private static FieldDescriptorProto.Builder string(String name, int number) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(FieldDescriptorProto.Type.TYPE_STRING);
    }

    private static DocumentSchemaAdmission.Definition definition(Descriptor type) {
        var closure = DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.googleapis.com/" + type.getFullName())
                .setArtifactSha256(DocumentSaves.sha256(bytes))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("built at test run time")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("protobuf-java").setVersion("test")))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    // ------------------------------------------------------------------ helpers

    private int lookups;

    private void stopRegistry() {
        registry.stopped = true;
        lookups = registry.lookups.get();
    }

    private NodeAddress saved(String doc, Document document) {
        var address = address(account, doc);
        saves.save(processAuthority(), create("op-1", address, document.toBuilder().setDocId(doc).build()).build());
        return address;
    }

    private static GetDocumentResponse read(AccessionFixture fixture, NodeAddress address, long revision) {
        return fixture.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(revision)).build());
    }

    private static void assertDataLoss(AccessionFixture fixture, NodeAddress address, long revision) {
        assertThatThrownBy(() -> read(fixture, address, revision)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    private static RepositorySchemaAssetReference coreBinding(StoredManifest stored) {
        return stored.getValidation().getPartSchemasList().stream()
                .filter(schema -> schema.getPart().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst()
                .orElseThrow().getFragmentSchema();
    }

    // Retained rows are immutable by trigger and checked by constraint; these helpers stand in
    // for storage loss and corruption, which no catalog code path can produce.

    private byte[] remove(String sha) throws Exception {
        var bytes = artifactBytes(sha);
        sql("SET session_replication_role = replica; DELETE FROM catalog_schema_artifacts WHERE sha256 = '" + sha + "'");
        return bytes;
    }

    private void restore(String sha, byte[] bytes) throws Exception {
        try (var connection = connection(); var insert = connection.prepareStatement(
                "INSERT INTO catalog_schema_artifacts (sha256, content) VALUES (?, ?)")) {
            insert.setString(1, sha);
            insert.setBytes(2, bytes);
            insert.executeUpdate();
        }
    }

    private void allowCorruption() throws Exception {
        sql("ALTER TABLE catalog_schema_artifacts DROP CONSTRAINT catalog_schema_artifacts_check");
    }

    private void setArtifactBytes(String sha, byte[] bytes) throws Exception {
        try (var connection = connection()) {
            connection.createStatement().execute("SET session_replication_role = replica");
            try (var update = connection.prepareStatement("UPDATE catalog_schema_artifacts SET content = ? WHERE sha256 = ?")) {
                update.setBytes(1, bytes);
                update.setString(2, sha);
                assertThat(update.executeUpdate()).isEqualTo(1);
            }
        }
    }

    private byte[] artifactBytes(String sha) throws Exception {
        try (var connection = connection(); var select = connection.prepareStatement(
                "SELECT content FROM catalog_schema_artifacts WHERE sha256 = ?")) {
            select.setString(1, sha);
            try (var rows = select.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getBytes(1);
            }
        }
    }

    private void sql(String statements) throws Exception {
        try (var connection = connection(); var statement = connection.createStatement()) {
            for (var sql : statements.split(";")) statement.execute(sql);
        }
    }

    private java.sql.Connection connection() throws Exception {
        return DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
    }

}
