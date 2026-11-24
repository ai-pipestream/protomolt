package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

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

    @Test
    void writersJoinPermissionsAndEveryRequiredRuleSet() {
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

    @Test
    void writerWithOnlyReadStillReceivesWriteAndRequiredRuleSetGrants() {
        var readOnly = DocumentSecurity.newBuilder()
                .addPermissions(rule("service", "PARSER", Access.ACCESS_READ))
                .addRequiredRuleSets(AccessRuleSet.newBuilder().setSource("page:1")
                        .addRules(rule("oauth-group", "hr", Access.ACCESS_READ)))
                .build();
        var updated = RawIngests.withStepWriters(document(readOnly), Set.of("parser")).getOwnership().getSecurity();

        assertThat(updated.getPermissionsList()).containsExactly(
                rule("service", "PARSER", Access.ACCESS_READ),
                rule("service", "parser", Access.ACCESS_WRITE));
        assertThat(updated.getRequiredRuleSets(0).getRulesList()).contains(
                rule("service", "parser", Access.ACCESS_READ),
                rule("service", "parser", Access.ACCESS_WRITE));

        var parserCaller = new RepositoryCaller(
                "parser",
                false,
                Set.of("acct"),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("parser").build()));
        assertThat(DocumentAccessPolicy.allows(parserCaller, "acct", updated, List.of(), Access.ACCESS_READ)).isTrue();
        assertThat(DocumentAccessPolicy.allows(parserCaller, "acct", updated, List.of(), Access.ACCESS_WRITE)).isTrue();
    }

    @Test
    void emptyWritersAlreadyCompleteGrantsOrExplicitDenyLeavesTheDocumentAlone() {
        var complete = DocumentSecurity.newBuilder()
                .addPermissions(rule("service", "PARSER", Access.ACCESS_READ))
                .addPermissions(rule("service", "PARSER", Access.ACCESS_WRITE))
                .build();
        var completeDoc = document(complete);
        assertThat(RawIngests.withStepWriters(completeDoc, Set.of())).isSameAs(completeDoc);
        assertThat(RawIngests.withStepWriters(completeDoc, Set.of("parser"))).isSameAs(completeDoc);

        var deniedInPermissions = document(DocumentSecurity.newBuilder()
                .addPermissions(rule("service", "PARSER", Access.ACCESS_DENY))
                .build());
        assertThat(RawIngests.withStepWriters(deniedInPermissions, Set.of("parser"))).isSameAs(deniedInPermissions);

        var deniedInGate = document(DocumentSecurity.newBuilder()
                .addPermissions(rule("oauth-group", "staff", Access.ACCESS_READ))
                .addRequiredRuleSets(AccessRuleSet.newBuilder().setSource("page:1")
                        .addRules(rule("service", "PARSER", Access.ACCESS_DENY)))
                .build());
        assertThat(RawIngests.withStepWriters(deniedInGate, Set.of("parser"))).isSameAs(deniedInGate);
    }
}
