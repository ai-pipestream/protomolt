package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.config.ConceptSchemeMounts;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogException;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v2.WriterGrant;
import ai.protomolt.proto.types.Concept;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.types.LangLabel;
import ai.protomolt.proto.validate.spi.ConceptSchemeCatalog;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RepoCallerBindings}, {@link RepoBindingWriters}, and
 * {@link RepoServices#loadConceptSchemes(List)} covering gRPC/HTTP writer
 * credential parity (#621), narrowed catalog concurrency handling (#621), and
 * concept-scheme mounting from configuration (#615).
 */
class RepoCallerBindingsTest {

    @Test
    void scopedGrpcCallerWithIntakeWritersReceivesBindingCredentialInBindAndCurrent() {
        var fakeCatalog = new FakeCredentialCatalog();
        var writers = new RepoBindingWriters(fakeCatalog::asCatalog);
        var binding = AccountBinding.newBuilder()
                .setPrincipal("connector-alpha")
                .addAccountIds("acct-1")
                .addIntakeWriters(IntakeWriterScope.newBuilder()
                        .setAccountId("acct-1")
                        .setDatasourceId("ds-1")
                        .setConnectorId("conn-1")
                        .setAllowCreate(true)
                        .build())
                .build();
        AccountBindingResolver resolver = authCaller ->
                "connector-alpha".equals(authCaller.caller().name()) ? Optional.of(binding) : Optional.empty();
        var bindings = new RepoCallerBindings();
        bindings.use(resolver);
        bindings.provisionWriters(writers);

        var caller = Caller.scoped("connector-alpha", Set.of());
        var authenticated = AuthenticatedCaller.unbound(caller);
        RepositoryCaller bound = bindings.bind(authenticated);
        UUID expectedId = RepoBindingWriters.credentialId("connector-alpha");
        assertThat(bound.credentialBinding())
                .contains(new RepositoryCredentialBinding(RepoBindingWriters.ISSUER, expectedId, 1L));
        assertThat(bound.accountIds()).containsExactly("acct-1");
        assertThat(fakeCatalog.asCatalog().grants("acct-1", RepoBindingWriters.ISSUER, expectedId))
                .hasSize(1);

        Context grpcContext = Context.ROOT
                .withValues(CallerContexts.CALLER, caller, CallerContexts.AUTHENTICATED_CALLER, authenticated);
        Context previous = grpcContext.attach();
        try {
            RepositoryCaller fromCurrent = bindings.current();
            assertThat(fromCurrent.credentialBinding())
                    .contains(new RepositoryCredentialBinding(RepoBindingWriters.ISSUER, expectedId, 1L));
        } finally {
            grpcContext.detach(previous);
        }
    }

    @Test
    void bindingWritersPropagatesUnavailableCatalogAndRuntimeExceptionsAndRecoversDuplicate() {
        var duplicateCatalog = new FakeCredentialCatalog();
        duplicateCatalog.failNextRegisterWithDuplicate = true;
        var writers = new RepoBindingWriters(duplicateCatalog::asCatalog);
        var binding = AccountBinding.newBuilder()
                .setPrincipal("connector-race")
                .addAccountIds("acct-1")
                .addIntakeWriters(IntakeWriterScope.newBuilder()
                        .setAccountId("acct-1")
                        .setDatasourceId("ds-1")
                        .setAllowCreate(true)
                        .build())
                .build();

        assertThat(writers.credentialOf(binding)).isPresent();

        var unavailableCatalog = new FakeCredentialCatalog();
        unavailableCatalog.failRegisterWith =
                new CatalogException("db down", new SQLException("down", "08006"), true);
        var unavailableWriters = new RepoBindingWriters(unavailableCatalog::asCatalog);
        assertThatThrownBy(() -> unavailableWriters.credentialOf(binding))
                .isInstanceOf(CatalogException.class)
                .matches(ex -> ((CatalogException) ex).unavailable());

        var bugCatalog = new FakeCredentialCatalog();
        bugCatalog.failRegisterWith = new IllegalStateException("unexpected bug");
        var bugWriters = new RepoBindingWriters(bugCatalog::asCatalog);
        assertThatThrownBy(() -> bugWriters.credentialOf(binding))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("unexpected bug");
    }

    @Test
    void loadsConceptSchemesFromJsonProtobufAndDirectoriesAndRejectsInvalidSchemes(
            @TempDir Path tempDir) throws Exception {
        ConceptScheme topicScheme = ConceptScheme.newBuilder()
                .addConcepts(Concept.newBuilder()
                        .setId("physics")
                        .addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText("Physics"))
                        .build())
                .addConcepts(Concept.newBuilder()
                        .setId("quantum")
                        .addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText("Quantum Mechanics"))
                        .addBroader("physics")
                        .build())
                .build();
        ConceptScheme regionScheme = ConceptScheme.newBuilder()
                .addConcepts(Concept.newBuilder()
                        .setId("eu")
                        .addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText("Europe"))
                        .build())
                .build();

        Path jsonFile = tempDir.resolve("topics.json");
        Files.writeString(jsonFile, JsonFormat.printer().print(topicScheme));

        Path subDir = Files.createDirectories(tempDir.resolve("schemes.d"));
        Path pbFile = subDir.resolve("regions.pb");
        Files.write(pbFile, regionScheme.toByteArray());

        ConceptSchemeMounts loaded = RepoServices.loadConceptSchemes(List.of(
                Path.of("topics=" + jsonFile),
                subDir));
        assertThat(loaded.scheme("topics"))
                .isPresent()
                .get()
                .extracting(ConceptSchemeCatalog.Mounted::conceptIds)
                .isEqualTo(Set.of("physics", "quantum"));
        assertThat(loaded.scheme("regions"))
                .isPresent()
                .get()
                .extracting(ConceptSchemeCatalog.Mounted::conceptIds)
                .isEqualTo(Set.of("eu"));

        RepoServiceConfig envConfig = RepoServiceConfig.fromEnvironment(Map.of(
                RepoServiceConfig.ENV_CONCEPT_SCHEMES, jsonFile + " , " + pbFile));
        assertThat(envConfig.conceptSchemePaths()).containsExactly(jsonFile, pbFile);

        // Cyclic concept scheme is rejected fail-fast during loadConceptSchemes.
        ConceptScheme cyclic = ConceptScheme.newBuilder()
                .addConcepts(Concept.newBuilder()
                        .setId("a")
                        .addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText("A"))
                        .addBroader("b")
                        .build())
                .addConcepts(Concept.newBuilder()
                        .setId("b")
                        .addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText("B"))
                        .addBroader("a")
                        .build())
                .build();
        Path cyclicFile = tempDir.resolve("cyclic.json");
        Files.writeString(cyclicFile, JsonFormat.printer().print(cyclic));
        assertThatThrownBy(() -> RepoServices.loadConceptSchemes(List.of(cyclicFile)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid concept scheme 'concept-scheme:cyclic'");
    }

    private static final class FakeCredentialCatalog {
        private final Map<String, CatalogRecords.Credential> credentials = new ConcurrentHashMap<>();
        private final Map<String, List<CatalogRecords.Grant>> grantsByKey = new ConcurrentHashMap<>();
        boolean failNextRegisterWithDuplicate;
        RuntimeException failRegisterWith;

        Catalog asCatalog() {
            return (Catalog) Proxy.newProxyInstance(
                    Catalog.class.getClassLoader(),
                    new Class<?>[] {Catalog.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "credential" -> Optional.ofNullable(
                                credentials.get(args[0] + "|" + args[1]));
                        case "registerCredential" -> {
                            String issuer = (String) args[0];
                            UUID id = (UUID) args[1];
                            String principal = (String) args[2];
                            if (failRegisterWith != null) {
                                throw failRegisterWith;
                            }
                            var record = new CatalogRecords.Credential(1L, principal, false);
                            credentials.put(issuer + "|" + id, record);
                            if (failNextRegisterWithDuplicate) {
                                failNextRegisterWithDuplicate = false;
                                throw new CatalogException(
                                        "duplicate credential",
                                        new SQLException("duplicate", "23505"),
                                        false);
                            }
                            yield null;
                        }
                        case "grants" -> List.copyOf(grantsByKey.getOrDefault(
                                args[0] + "|" + args[1] + "|" + args[2], List.of()));
                        case "installGrant" -> {
                            WriterGrant grant = (WriterGrant) args[0];
                            String key = grant.getAccountId() + "|"
                                    + grant.getCredential().getIssuer() + "|"
                                    + grant.getCredential().getCredentialId();
                            var record = new CatalogRecords.Grant(
                                    grant, ByteString.EMPTY, "a".repeat(64), false);
                            grantsByKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
                            yield record;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
