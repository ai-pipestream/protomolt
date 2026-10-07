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
    public static void run(Path bundle) throws Exception {
        for (boolean rpc:new boolean[]{false,true}) {
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
                try {
                    BoundedDocumentPublicConsumer.run(config,schemas,publication,caller,host -> {
                        var fixture=BoundedDocumentHostProbe.prepare(host,new Tx(database.entityManagerFactory()));
                        return new BoundedDocumentPublicConsumer.Fixture(fixture.request(),fixture.document());
                    },rpc);
                } finally { schemas.close(); }
            } finally {
                try (var paths=Files.walk(directory)) {
                    for (var path:paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
                }
            }
        }
    }
}
