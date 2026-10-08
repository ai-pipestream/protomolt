package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import ai.protomolt.repo.consumer.BoundedDocumentPublicConsumer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Fixture setup is separate from the consumer that uses only public composition APIs. */
public final class BoundedDocumentPublicFactoryProbe {
    public static void main(String[] args) throws Exception { run(Path.of(args[0])); }
    public static void run(Path bundle) throws Exception {
        for (boolean hosted:new boolean[]{false,true}) for (boolean rpc:new boolean[]{false,true}) {
            var identity=hosted ? new ReaderHostOptions(UUID.randomUUID(),"bounded-public-consumer",UUID.randomUUID().toString()) : null;
            String mode=(hosted ? "hosted-" : "")+(rpc ? "rpc" : "library");
            long started = System.nanoTime();
            phase(mode, "factory-start", started);
            var config=new RepoServiceConfig(0,new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                    System.getenv("PROTOMOLT_TEST_USER"),System.getenv("PROTOMOLT_TEST_PASSWORD")),
                    "http://127.0.0.1:1","us-east-1","unused","unused","public-bounded",0,
                    "redis",null,null,System.getenv("PROTOMOLT_TEST_REDIS_URI"),0,1024*1024)
                    .withManagedStorage(new ManagedStoragePolicy("public-bounded-"+UUID.randomUUID(),"public-bounded-realm",true));
            var definition=BoundedDocumentHostProbe.definition(BoundedDocumentRejectionProbe.constrainedType());
            Path directory=Files.createTempDirectory("public-bounded-git");
            try (var git=GitSchemaRegistryStore.builder().repositoryDir(directory).build();
                    var database=new LedgerDatabase(config.ledger())) {
                git.putDescriptorSet(definition.metadata().getArtifactSha256(),definition.descriptors());
                var resolver=new RegistrySchemaResolver(git,new DocumentSchemaArtifactCache.Limits(8_000_000,16,4_000_000),4,16);
                ManagedSchemaAccess schemas=new ManagedSchemaAccess() {
                    public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller,DocumentPublicationMember member,RepositoryReadControl control) {
                        return resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(),definition.source()),control::check);
                    }
                    public void close() { resolver.close(); }
                    public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
                };
                var caller=new RepositoryCaller("operator",true);
                var publication=new ManagedPublicationOptions(bundle,Duration.ofMinutes(5),Duration.ofSeconds(5),
                        (account,principal,operation) -> caller);
                if (rpc) publication=publication.withTransport(new ManagedPublicationOptions.Transport(auth -> caller,32L*1024*1024,2));
                phase(mode, "factory-ready", started);
                try {
                    phase(mode, "consumer-start", started);
                    BoundedDocumentPublicConsumer.run(config,schemas,publication,caller,host -> {
                        if (identity!=null) assertHost(database,identity,"ACTIVE","ACTIVE");
                        phase(mode, "fixture-prepare-start", started);
                        var fixture=BoundedDocumentHostProbe.prepare(host,new Tx(database.entityManagerFactory()));
                        phase(mode, "fixture-prepared", started);
                        return new BoundedDocumentPublicConsumer.Fixture(fixture.request(),fixture.document());
                    },rpc,identity);
                    if (identity!=null) assertHost(database,identity,"FENCED","QUIESCED");
                    phase(mode, "consumer-complete", started);
                } finally { schemas.close(); }
            } finally {
                try (var paths=Files.walk(directory)) {
                    for (var path:paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
                }
            }
            phase(mode, "factory-closed", started);
        }
    }

    private static void assertHost(LedgerDatabase database,ReaderHostOptions identity,String hostState,String readerState) {
        new Tx(database.entityManagerFactory()).inTransaction(em -> {
            var rows=em.createNativeQuery("""
                    SELECT h.state,h.host_identity,h.boot_identity,r.state,r.quiescence_source
                    FROM repository_reader_host_executions h JOIN repository_reader_incarnations r
                      ON r.host_execution=h.execution WHERE h.execution=:id
                    """).setParameter("id",identity.execution()).getResultList();
            if (rows.size()!=1) throw new AssertionError("Bounded documents must bind exactly one reader");
            var row=(Object[])rows.getFirst();
            if (!hostState.equals(row[0]) || !identity.hostIdentity().equals(row[1])
                    || !identity.bootIdentity().equals(row[2]) || !readerState.equals(row[3]))
                throw new AssertionError("Host or reader binding/state mismatch");
            if ("QUIESCED".equals(readerState) && !"LOCAL_DRAIN".equals(row[4]))
                throw new AssertionError("Clean close must retain local drain provenance");
            return null;
        });
    }

    private static void phase(String mode, String name, long started) {
        System.out.printf("PHASE name=factory-%s-%s elapsed_ms=%d%n", mode, name,
                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        System.out.flush();
    }
}
