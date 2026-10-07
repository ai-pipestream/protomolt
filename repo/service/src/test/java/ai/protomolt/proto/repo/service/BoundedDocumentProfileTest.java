package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BoundedDocumentProfileTest {
    @Test void requiresFiniteObjectAndAggregateLimits() {
        assertThatThrownBy(() -> new BoundedDocumentProfile(0,64L*1024*1024)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedDocumentProfile(8*1024*1024+1,64L*1024*1024)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedDocumentProfile(1024,64L*1024*1024-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void qualifiesProviderSettingsWithoutOpeningBackends() {
        var profile=new BoundedDocumentProfile(1024,64L*1024*1024);
        assertThatCode(() -> profile.validate(config("redis",0,1024,true))).doesNotThrowAnyException();
        assertThatThrownBy(() -> profile.validate(config("s3",0,1024,true))).hasMessageContaining("qualified Redis");
        assertThatThrownBy(() -> profile.validate(config("redis",0,1024,false))).hasMessageContaining("qualified Redis");
        assertThatThrownBy(() -> profile.validate(config("redis",1,1024,true))).hasMessageContaining("zero TTL");
        assertThatThrownBy(() -> profile.validate(config("redis",0,1023,true))).hasMessageContaining("provider object cap");
        assertThatThrownBy(() -> profile.validate(config("redis",0,9L*1024*1024+1,true))).hasMessageContaining("provider object cap");
    }

    private static RepoServiceConfig config(String provider,int ttl,long max,boolean qualified) {
        var config=new RepoServiceConfig(0,new LedgerConfig("jdbc:postgresql://127.0.0.1:1/unopened","unused","unused"),
                "http://127.0.0.1:1","us-east-1","unused","unused","bounded-test",0,
                provider,null,null,"redis://127.0.0.1:1",ttl,max);
        return qualified ? config.withManagedStorage(new ManagedStoragePolicy("bounded-test","realm",true)) : config;
    }
}
