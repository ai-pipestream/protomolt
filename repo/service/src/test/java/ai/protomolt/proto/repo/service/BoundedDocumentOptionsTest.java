package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BoundedDocumentOptionsTest {
    private static final long BUDGET=64L*1024*1024;
    // An invocation is a test failure, not a successful resolver substitute.
    private static final ManagedSchemaAccess UNOPENED=new ManagedSchemaAccess() {
        public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller,DocumentPublicationMember member,RepositoryReadControl control) {
            throw new AssertionError("Invalid configuration opened schema access");
        }
        public void close() { throw new AssertionError("Failed construction took schema ownership"); }
        public boolean awaitIdle(Duration timeout) { throw new AssertionError("Invalid configuration drained schemas"); }
    };
    private static ManagedPublicationOptions publication() {
        return new ManagedPublicationOptions(Path.of("unopened-bundle"),Duration.ofMinutes(5),Duration.ofSeconds(5),
                (account,principal,operation) -> { throw new AssertionError("Invalid configuration requested authority"); });
    }
    @Test void publicLimitsPreserveRuntimeBounds() {
        assertThatThrownBy(() -> new BoundedDocumentOptions(0,BUDGET)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedDocumentOptions(8*1024*1024+1,BUDGET)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedDocumentOptions(1024,BUDGET-1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new BoundedDocumentOptions(8*1024*1024,BUDGET).maxObjectBytes()).isEqualTo(8*1024*1024);
    }
    @Test void invalidSettingsFailBeforeOpeningUnreachableBackends() {
        for (String provider:new String[]{"s3","repo"})
            refused(config(provider,0,1024,true,true),"qualified Redis");
        refused(config("redis",0,1024,false,true),"qualified Redis");
        refused(config("redis",0,1024,true,false),"qualified Redis");
        refused(config("redis",1,1024,true,true),"zero TTL");
        refused(config("redis",0,1023,true,true),"provider object cap");
        refused(config("redis",0,9L*1024*1024+1,true,true),"provider object cap");
    }
    @Test void missingCollaboratorsFailBeforeBackendDiscoveryOrOwnershipTransfer() {
        var config=config("redis",0,1024,true,true);
        var bridges=BridgeEngine.standard();
        var publication=publication();
        var options=new BoundedDocumentOptions(1024,BUDGET);
        assertThatThrownBy(() -> RepoServices.buildBoundedDocuments(null,bridges,null,UNOPENED,publication,options))
                .isInstanceOf(NullPointerException.class).hasMessage("config");
        assertThatThrownBy(() -> RepoServices.buildBoundedDocuments(config,null,null,UNOPENED,publication,options))
                .isInstanceOf(NullPointerException.class).hasMessage("bridges");
        assertThatThrownBy(() -> RepoServices.buildBoundedDocuments(config,bridges,null,null,publication,options))
                .isInstanceOf(NullPointerException.class).hasMessage("schemaAccess");
        assertThatThrownBy(() -> RepoServices.buildBoundedDocuments(config,bridges,null,UNOPENED,null,options))
                .isInstanceOf(NullPointerException.class).hasMessage("publication");
        assertThatThrownBy(() -> RepoServices.buildBoundedDocuments(config,bridges,null,UNOPENED,publication,null))
                .isInstanceOf(NullPointerException.class).hasMessage("options");
    }
    private static void refused(RepoServiceConfig config,String message) {
        assertThatThrownBy(() -> RepoServices.buildBoundedDocuments(config,BridgeEngine.standard(),null,
                UNOPENED,publication(),new BoundedDocumentOptions(1024,BUDGET)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message);
    }
    private static RepoServiceConfig config(String provider,int ttl,long max,boolean qualified,boolean lifecycle) {
        var config=new RepoServiceConfig(0,new LedgerConfig("jdbc:postgresql://127.0.0.1:1/unopened","unused","unused"),
                "http://127.0.0.1:1","us-east-1","unused","unused","bounded-test",0,
                provider,"127.0.0.1:1","fixture-drive","redis://127.0.0.1:1",ttl,max,lifecycle,5000,60000,false,true,60000);
        return qualified ? config.withManagedStorage(new ManagedStoragePolicy("bounded-test","realm",true)) : config;
    }
}
