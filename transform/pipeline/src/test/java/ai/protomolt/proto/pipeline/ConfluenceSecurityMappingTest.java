package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static ai.protomolt.proto.pipeline.AclMappingTestSupport.allows;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.caller;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadEdge;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.loadRegistry;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapIdentityLinks;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapSecurity;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.mapSingleIdentityLink;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.parseText;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.principal;
import static ai.protomolt.proto.pipeline.AclMappingTestSupport.reads;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for Confluence ACL and Entra-to-Atlassian identity-link mapping configs,
 * covering positive inheritance/restriction rules and fail-closed security invariants.
 */
class ConfluenceSecurityMappingTest {

    private static DescriptorRegistry registry;
    private static TypedEdge edge;
    private static TypedEdge links;
    private static TypedEdge entraLinks;
    private static Descriptor entity;
    private static Descriptor graphUser;

    @BeforeAll
    static void load() throws Exception {
        registry = loadRegistry("acl/confluence-v1.binpb", "acl/microsoft-v1.binpb");
        edge = loadEdge("acl/confluence-document-security.textproto");
        links = loadEdge("acl/confluence-identity-links.textproto");
        entraLinks = loadEdge("acl/entra-identity-links.textproto");
        entity = registry.findDescriptorByFullName("ai.pipestream.confluence.v1.ConfluenceEntity");
        graphUser = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser");
    }

    /** The space: group g-staff and user acc-admin may view it; a role and a delete grant do not count. */
    private static final String SPACE = """
            entity_id: "space:100/access"
            security_completeness: ACL_COMPLETENESS_COMPLETE
            space_access {
              space_id: "100"
              permissions { id: "1" principal { type: SPACE_PERMISSION_PRINCIPAL_TYPE_GROUP id: "g-staff" }
                operation { key: SPACE_PERMISSION_OPERATION_KEY_READ target_type: SPACE_PERMISSION_OPERATION_TARGET_TYPE_SPACE } }
              permissions { id: "2" principal { type: SPACE_PERMISSION_PRINCIPAL_TYPE_USER id: "acc-admin" }
                operation { key: SPACE_PERMISSION_OPERATION_KEY_READ target_type: SPACE_PERMISSION_OPERATION_TARGET_TYPE_SPACE } }
              permissions { id: "3" principal { type: SPACE_PERMISSION_PRINCIPAL_TYPE_USER id: "acc-3" }
                operation { key: SPACE_PERMISSION_OPERATION_KEY_DELETE target_type: SPACE_PERMISSION_OPERATION_TARGET_TYPE_PAGE } }
              permissions { id: "4" principal { type: SPACE_PERMISSION_PRINCIPAL_TYPE_ROLE id: "r-1" }
                operation { key: SPACE_PERMISSION_OPERATION_KEY_READ target_type: SPACE_PERMISSION_OPERATION_TARGET_TYPE_SPACE } }
            }
            """;

    /**
     * The child 300 (open) under parent 200 (view-restricted to acc-1, with UPDATE
     * for group g-hr) under the space homepage 10 (edit-restricted only).
     */
    private static final String CHILD = """
            entity_id: "page:300/access"
            security_completeness: ACL_COMPLETENESS_COMPLETE
            access_chain {
              space_id: "100"
              owners { content { type: CONTENT_TYPE_PAGE id: "300" } restriction_state { state: CONTENT_RESTRICTION_STATE_OPEN } }
              owners { content { type: CONTENT_TYPE_PAGE id: "200" }
                restriction_state { state: CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED }
                permissions { operation: CONTENT_PERMISSION_OPERATION_READ
                  principal { principal_type: CONTENT_PERMISSION_PRINCIPAL_TYPE_USER principal_id: "acc-1" } }
                permissions { operation: CONTENT_PERMISSION_OPERATION_UPDATE
                  principal { principal_type: CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP principal_id: "g-hr" } } }
              owners { content { type: CONTENT_TYPE_PAGE id: "10" }
                restriction_state { state: CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED }
                permissions { operation: CONTENT_PERMISSION_OPERATION_UPDATE
                  principal { principal_type: CONTENT_PERMISSION_PRINCIPAL_TYPE_USER principal_id: "acc-admin" } } }
            }
            """;

    private static final List<String> DIRECTORY = List.of("""
            entity_id: "acc-1"
            user { account_id: "acc-1" email: "Ada@Example.com" display_name: "Ada" }
            """, """
            entity_id: "acc-2"
            user { account_id: "acc-2" display_name: "Hidden" }
            """, """
            entity_id: "group:g-staff/member/acc-1"
            group_member { group_id: "g-staff" user { account_id: "acc-1" } }
            """, """
            entity_id: "group:g-staff/member/acc-2"
            group_member { group_id: "g-staff" user { account_id: "acc-2" } }
            """, """
            entity_id: "g-staff"
            group { id: "g-staff" name: "staff" }
            """);

    @Test
    void spaceGrantsBecomePermissionsAndEachViewRestrictedOwnerOneRequiredSet() throws Exception {
        DocumentSecurity security = map(SPACE, CHILD);

        assertThat(security.getPermissionsList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                .containsExactly("atlassian-group:g-staff", "atlassian-account:acc-admin");
        assertThat(security.getPermissionsList()).allSatisfy(r ->
                assertThat(r.getAccess()).isEqualTo(Access.ACCESS_READ));
        assertThat(security.getRequiredRuleSetsList()).singleElement().satisfies(set -> {
            assertThat(set.getSource()).isEqualTo("confluence-owner:200");
            assertThat(set.getRulesList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                    .containsExactly("atlassian-account:acc-1", "atlassian-group:g-hr");
        });
    }

    @Test
    void theChildIsReadableOnlyWithASpaceGrantAndAGrantOnTheRestrictedParent() throws Exception {
        DocumentSecurity security = map(SPACE, CHILD);

        assertThat(reads(security, "atlassian-account:acc-1", "atlassian-group:g-staff")).isTrue();
        assertThat(reads(security, "atlassian-account:acc-2", "atlassian-group:g-staff", "atlassian-group:g-hr"))
                .as("UPDATE on the parent grants read").isTrue();
        assertThat(reads(security, "atlassian-account:acc-2", "atlassian-group:g-staff"))
                .as("space only, not on the restricted parent").isFalse();
        assertThat(reads(security, "atlassian-account:acc-1"))
                .as("on the parent, but no space grant").isFalse();
        assertThat(reads(security, "atlassian-account:acc-admin"))
                .as("space grant and an edit restriction above, but not on the parent").isFalse();
        assertThat(reads(security, "atlassian-account:acc-3", "atlassian-group:g-hr"))
                .as("a space delete grant is not a view grant").isFalse();
    }

    @Test
    void incompleteMismatchedSpaceOrUnspecifiedOperationFailsClosed() throws Exception {
        DocumentSecurity incomplete = map(SPACE, CHILD.replace(
                "security_completeness: ACL_COMPLETENESS_COMPLETE",
                "security_completeness: ACL_COMPLETENESS_UNAVAILABLE"));
        assertThat(incomplete.getPermissionsList()).isEmpty();
        assertThat(reads(incomplete, "atlassian-account:acc-1", "atlassian-group:g-staff")).isFalse();
        assertThat(map(SPACE.replace("ACL_COMPLETENESS_COMPLETE", "ACL_COMPLETENESS_UNSPECIFIED"),
                CHILD).getPermissionsList()).isEmpty();

        DocumentSecurity wrongSpace = map(SPACE.replace("space_id: \"100\"", "space_id: \"999\""), CHILD);
        assertThat(wrongSpace.getPermissionsList()).isEmpty();
        assertThat(wrongSpace.getRequiredRuleSetsList()).isEmpty();
        assertThat(reads(wrongSpace, "atlassian-account:acc-1", "atlassian-group:g-staff")).isFalse();

        DocumentSecurity missingSpaceArm = map("""
                entity_id: "acc-1"
                security_completeness: ACL_COMPLETENESS_COMPLETE
                user { account_id: "acc-1" }
                """, CHILD);
        assertThat(missingSpaceArm.getPermissionsList()).isEmpty();
        assertThat(reads(missingSpaceArm, "atlassian-account:acc-1", "atlassian-group:g-staff")).isFalse();

        String unspecifiedOp = CHILD.replace(
                "operation: CONTENT_PERMISSION_OPERATION_READ",
                "operation: CONTENT_PERMISSION_OPERATION_UNSPECIFIED");
        DocumentSecurity security = map(SPACE, unspecifiedOp);
        assertThat(reads(security, "atlassian-account:acc-1", "atlassian-group:g-staff"))
                .as("unspecified content permission operation must not grant read")
                .isFalse();
        assertThat(reads(security, "atlassian-account:acc-2", "atlassian-group:g-staff", "atlassian-group:g-hr"))
                .isTrue();
    }

    @Test
    void anOpenItemNeedsOnlyTheSpaceGrant() throws Exception {
        String open = CHILD.replace("CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED", "CONTENT_RESTRICTION_STATE_OPEN");
        DocumentSecurity security = map(SPACE, open);

        assertThat(security.getRequiredRuleSetsList()).isEmpty();
        assertThat(reads(security, "atlassian-account:acc-2", "atlassian-group:g-staff")).isTrue();
        assertThat(reads(security, "atlassian-account:acc-2")).isFalse();
    }

    @Test
    void anEntraCallerJoinsOnlyThroughVerifiedDirectoryAddressesOnEnabledAccounts() throws Exception {
        List<IdentityLink> mapped = new ArrayList<>();
        for (String record : DIRECTORY) {
            IdentityLink link = mapSingleIdentityLink(
                    registry, links, Map.of("entity", parseText(entity, record)), "confluence-identity");
            if (link.hasFrom() && link.hasTo()) {
                mapped.add(link);
            }
        }
        assertThat(mapped).extracting(l -> l.getFrom().getIdentityType() + ":" + l.getFrom().getIdentity()
                + " -> " + l.getTo().getIdentityType() + ":" + l.getTo().getIdentity()).containsExactly(
                "email:Ada@Example.com -> atlassian-account:acc-1",
                "atlassian-account:acc-1 -> atlassian-group:g-staff",
                "atlassian-account:acc-2 -> atlassian-group:g-staff");

        List<IdentityLink> entra = new ArrayList<>();
        entra.addAll(entraLinks("user_principal_name: \"ada@contoso.onmicrosoft.com\" mail: \"ada@example.com\""
                + " proxy_addresses: \"SMTP:ada@example.com\" proxy_addresses: \"x500:/o=ExchangeLabs\""));
        entra.addAll(entraLinks("user_principal_name: \"someone@example.com\""));
        entra.addAll(entraLinks("user_principal_name: \"cy@contoso.com\" mail: \"cy@contoso.com\""
                + " other_mails: \"Ada@Example.com\" proxy_addresses: \"smtp:Cy.Alias@Example.com\""));
        entra.addAll(entraLinks("user_principal_name: \"disabled@contoso.com\" mail: \"ada@example.com\""
                + " account_enabled: false"));
        assertThat(entra).extracting(l -> l.getFrom().getIdentity() + " -> " + l.getTo().getIdentityType()
                + ":" + l.getTo().getIdentity()).containsExactly(
                "ada@contoso.onmicrosoft.com -> email:ada@example.com",
                "ada@contoso.onmicrosoft.com -> email:ada@example.com",
                "someone@example.com -> email:someone@example.com",
                "cy@contoso.com -> email:cy@contoso.com",
                "cy@contoso.com -> email:Cy.Alias@Example.com");
        mapped.addAll(entra);
        mapped.add(IdentityLink.newBuilder()
                .setFrom(principal("email:cy.alias@example.com"))
                .setTo(principal("atlassian-account:acc-2"))
                .build());

        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        DocumentSecurity security = map(SPACE, CHILD);
        RepositoryCaller ada = expansion.expand(caller("user-principal-name:ada@contoso.onmicrosoft.com"));
        assertThat(ada.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("email:ada@example.com", "atlassian-account:acc-1", "atlassian-group:g-staff");
        assertThat(allows(ada, security)).isTrue();

        RepositoryCaller cy = expansion.expand(caller("user-principal-name:cy@contoso.com"));
        assertThat(cy.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .as("verified SMTP proxy alias joins, but unverified other_mails never escalates to acc-1")
                .contains("atlassian-account:acc-2")
                .doesNotContain("email:Ada@Example.com", "atlassian-account:acc-1");
        assertThat(allows(cy, security)).isFalse();

        RepositoryCaller disabled = expansion.expand(caller("user-principal-name:disabled@contoso.com"));
        assertThat(allows(disabled, security)).as("disabled Entra user emits no links").isFalse();
    }

    private static List<IdentityLink> entraLinks(String text) throws Exception {
        return mapIdentityLinks(registry, entraLinks, Map.of("user", parseText(graphUser, text)), "entra-identity");
    }

    private static DocumentSecurity map(String space, String item) throws Exception {
        Map<String, DynamicMessage> values = new LinkedHashMap<>();
        values.put("space", parseText(entity, space));
        values.put("item", parseText(entity, item));
        return mapSecurity(registry, edge, values, "confluence-acl");
    }
}
