package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;

class StepWriterRulesTest {
    private static AccessRule rule(String type, String identity, Access access) {
        return AccessRule.newBuilder().setIdentityType(type).setIdentity(identity).setAccess(access).build();
    }

    private static Document document(DocumentSecurity security) {
        var builder = Document.newBuilder();
        builder.getOwnershipBuilder().setSecurity(security);
        return builder.build();
    }

    @Test void writersJoinPermissionsAndEveryRequiredRuleSet() {
        var security = DocumentSecurity.newBuilder()
                .addPermissions(rule("oauth-group", "space-viewers", Access.ACCESS_READ))
                .addRequiredRuleSets(AccessRuleSet.newBuilder().setSource("page:1")
                        .addRules(rule("oauth-group", "hr", Access.ACCESS_READ)))
                .addRequiredRuleSets(AccessRuleSet.newBuilder().setSource("page:2")
                        .addRules(rule("service", "parser", Access.ACCESS_READ))
                        .addRules(rule("service", "parser", Access.ACCESS_WRITE)))
                .build();
        var updated = RawIngests.withStepWriters(document(security), Set.of("parser")).getOwnership().getSecurity();
        var writer = Set.of(rule("service", "parser", Access.ACCESS_READ), rule("service", "parser", Access.ACCESS_WRITE));
        assertThat(updated.getPermissionsList()).containsAll(writer).hasSize(3);
        assertThat(updated.getRequiredRuleSets(0).getRulesList()).containsAll(writer).hasSize(3);
        assertThat(updated.getRequiredRuleSets(0).getSource()).isEqualTo("page:1");
        assertThat(updated.getRequiredRuleSets(1).getRulesList()).hasSize(2);
    }

    @Test void noWritersOrAlreadyNamedLeavesTheDocumentAlone() {
        var named = DocumentSecurity.newBuilder()
                .addPermissions(rule("service", "PARSER", Access.ACCESS_READ)).build();
        var doc = document(named);
        assertThat(RawIngests.withStepWriters(doc, Set.of())).isSameAs(doc);
        assertThat(RawIngests.withStepWriters(doc, Set.of("parser"))).isSameAs(doc);
    }
}
