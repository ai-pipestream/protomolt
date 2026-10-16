package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.registry.IncompatibleRegistrationException;
import ai.protomolt.proto.repo.admission.DocumentRetainedDefinitions;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §15.7 with a real schema registry (decisions 34 and 35). A save decodes with the
 * latest registered Document version and records it on the revision; a read decodes
 * with the recorded version. A field the latest version does not know, including one
 * only an older version had, is a recorded SCHEMA_MISSING and is never dropped.
 */
class RegistryDocumentEvolutionIT {
    private static final String LEGACY = "string legacy_note = 4243;";
    private static final String REVIEW = "string review_note = 4242;";
    private AccessionFixture f;
    private DocumentRegistry registry;
    private DocumentSaves saves;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        registry = new DocumentRegistry();
        saves = new DocumentSaves(f.catalog, f.drives, registry.resolver(resolver()), limits(), bytes -> () -> {},
                Clock.systemUTC(), registry.schemas());
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void fieldsOfTheLatestVersionSurviveSaveRestartAndReadUnderTheRecordedVersion() throws Exception {
        // v1 has legacy_note; a revision saved under it keeps the field.
        var v1 = registry.registerDocument(DocumentRegistry.withField(DocumentRegistry.documentSource(), LEGACY));
        var address = address(account, "doc-evolve");
        var first = with(document(account, "doc-evolve", "v1"), 4243, "kept from v1");
        saves.save(processAuthority(), create("op-1", address, first).build());

        // v2 removes legacy_note without reserving it and adds review_note; the Document
        // subject's BACKWARD_TRANSITIVE mode allows both.
        var v2 = registry.registerDocument(DocumentRegistry.withField(DocumentRegistry.documentSource(), REVIEW));
        assertThat(v2.version()).isEqualTo(v1.version() + 1);
        var second = with(document(account, "doc-evolve", "v2"), 4242, "added in v2");
        saves.save(processAuthority(), create("op-2", address, second).setExpected(at(1)).build());

        registry.stopped = true;
        int lookups = registry.lookups.get();
        try (var restarted = f.restarted()) {
            for (long revision : new long[] {1, 2}) {
                var sent = revision == 1 ? first : second;
                var stored = restarted.catalog.manifest(address, revision).orElseThrow();
                var read = restarted.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder()
                        .setAddress(address).setRevision(RevisionSelector.newBuilder().setRevision(revision)).build());
                // The compiled Document keeps the newer field as an unknown field, byte for byte.
                assertThat(read.getDocument()).isEqualTo(claimChecked(sent, stored));

                // Message equality under the recorded schema: decode both with that version.
                var binding = stored.getValidation().getPartSchemasList().stream()
                        .filter(schema -> schema.getPart().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
                var recorded = DocumentRetainedDefinitions.definition(binding.getFragmentSchema(), restarted.catalog::artifact);
                var type = ai.protomolt.proto.repo.admission.DocumentCandidateAdmission.containerType(recorded);
                var decoded = DynamicMessage.parseFrom(type, read.getDocument().toByteString());
                assertThat(decoded.getUnknownFields().asMap()).isEmpty();
                assertThat(decoded).isEqualTo(DynamicMessage.parseFrom(type, claimChecked(sent, stored).toByteString()));
                var field = type.findFieldByNumber(revision == 1 ? 4243 : 4242);
                assertThat(decoded.getField(field)).isEqualTo(revision == 1 ? "kept from v1" : "added in v2");
                assertThat(registryVersion(recorded.metadata())).isEqualTo(Integer.toString(revision == 1 ? v1.version() : v2.version()));

                // The stored fragment is the one its SHA-256 names.
                var core = stored.getPartsList().stream()
                        .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
                assertThat(core.getRef().getSha256()).isEqualTo(core.getEntry().getSha256());
                try (var bytes = restarted.stack.open(core.getRef())) {
                    assertThat(SaveIntents.sha256Hex(bytes.readAllBytes())).isEqualTo(core.getEntry().getSha256());
                }
            }
        }
        assertThat(registry.lookups.get()).as("reads make no registry call").isEqualTo(lookups);
    }

    @Test void aFieldOnlyAnOlderVersionHadAndAFieldInNoVersionAreRecordedSchemaMissing() {
        registry.registerDocument(DocumentRegistry.withField(DocumentRegistry.documentSource(), LEGACY));
        registry.registerDocument(DocumentRegistry.withField(DocumentRegistry.documentSource(), REVIEW));
        var address = address(account, "doc-missing");
        int operation = 0;
        for (int number : new int[] {4243, 4999}) {
            var request = create("op-missing-" + ++operation, address,
                    with(document(account, "doc-missing", "x"), number, "dropped?")).build();
            f.stack.reset();
            for (int attempt = 0; attempt < 2; attempt++) {
                assertThatThrownBy(() -> saves.save(processAuthority(), request))
                        .isInstanceOfSatisfying(AccessionException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                            assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                    .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
                        });
            }
            assertThat(f.catalog.outcome(account, request.getOperationId()).orElseThrow().outcome().getRejection()
                    .getReason()).isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
            assertThat(f.catalog.current(address)).isEmpty();
            assertThat(f.stack.puts.get()).isZero();
        }
        // The same document with the latest version's field commits.
        var accepted = saves.save(processAuthority(), create("op-ok", address,
                with(document(account, "doc-missing", "x"), 4242, "known")).build());
        assertThat(accepted.getAccession().getRevision()).isEqualTo(1);
    }

    @Test void theDocumentSubjectIsBackwardTransitiveAndOtherSubjectsKeepTheDefault() {
        var base = DocumentRegistry.documentSource();
        var v1 = DocumentRegistry.withField(base, "string probe = 42;");
        var v3 = DocumentRegistry.withField(base, "int64 probe = 42;");
        registry.registerDocument(v1);
        registry.registerDocument(base);
        // v3 reuses 42, removed unreserved in v2, with a wire-incompatible type: refused
        // because v3 is checked against v1 as well as v2.
        assertThatThrownBy(() -> registry.registerDocument(v3)).isInstanceOf(IncompatibleRegistrationException.class)
                .hasMessageContaining("v1");

        var other = "ai.protomolt.proto.repo.v1.DocumentAtDefault";
        assertThat(registry.store.compatibilityMode(other)).isEmpty();
        assertThat(registry.store.globalCompatibilityMode()).isEqualTo("BACKWARD");
        var references = registry.store.latest(DocumentSchemas.SUBJECT).orElseThrow().references();
        registry.store.register(other, v1, references);
        registry.store.register(other, base, references);
        assertThat(registry.store.register(other, v3, references).version()).isEqualTo(3);
    }

    @Test void aWeakerDocumentModeOrAStoppedRegistryStopsTheSaveWithoutRecordingIt() {
        registry.registerDocument(DocumentRegistry.documentSource());
        var address = address(account, "doc-host");
        var request = create("op-host", address, document(account, "doc-host", "x")).build();
        registry.store.setCompatibilityMode(DocumentSchemas.SUBJECT, "BACKWARD");
        assertThatThrownBy(() -> saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.INTERNAL));
        registry.store.setCompatibilityMode(DocumentSchemas.SUBJECT, "BACKWARD_TRANSITIVE");
        registry.stopped = true;
        assertThatThrownBy(() -> saves.save(processAuthority(), request)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(f.catalog.outcome(account, "op-host")).isEmpty();
        registry.stopped = false;
        assertThat(saves.save(processAuthority(), request).getAccession().getRevision()).isEqualTo(1);
    }

    private static Document with(Document document, int number, String value) {
        var unknown = UnknownFieldSet.newBuilder().addField(number, UnknownFieldSet.Field.newBuilder()
                .addLengthDelimited(com.google.protobuf.ByteString.copyFromUtf8(value)).build()).build();
        return document.toBuilder().setUnknownFields(unknown).build();
    }

    private static String registryVersion(RepositorySchemaAsset metadata) {
        return metadata.getCompilation().getKnownCompiler().getOptionsOrThrow("registry.version");
    }
}
