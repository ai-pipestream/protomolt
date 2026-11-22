package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TextFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Confluence ACL mapping config applied as a pipeline edge: the records
 * grpc-confluence Sync emits for a child page under a view-restricted parent,
 * mapped into a DocumentSecurity and evaluated by the repository policy.
 */
class ConfluenceSecurityMappingTest {

    private static final String ACCOUNT = "ACC";
    private static DescriptorRegistry registry;
    private static TypedEdge edge;
    private static TypedEdge links;
    private static Descriptor entity;

    @BeforeAll
    static void load() throws Exception {
        registry = DescriptorRegistry.create(false);
        new GoogleDescriptorLoader("acl/confluence-v1.binpb").loadDescriptors().forEach(registry::registerFile);
        registry.registerFile(DocumentSecurity.getDescriptor().getFile());
        registry.registerFile(IdentityLink.getDescriptor().getFile());
        TypedEdge.Builder builder = TypedEdge.newBuilder();
        TextFormat.merge(resource("acl/confluence-document-security.textproto"), builder);
        edge = builder.build();
        TypedEdge.Builder linkBuilder = TypedEdge.newBuilder();
        TextFormat.merge(resource("acl/confluence-identity-links.textproto"), linkBuilder);
        links = linkBuilder.build();
        entity = registry.findDescriptorByFullName("ai.pipestream.confluence.v1.ConfluenceEntity");
    }

    /** The space: group g-staff and user acc-admin may view it; a role and a delete grant do not count. */
    private static final String SPACE = """
            entity_id: "space:100/access"
            security_completeness: SECURITY_COMPLETENESS_COMPLETE
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
            security_completeness: SECURITY_COMPLETENESS_COMPLETE
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
    void anIncompleteRecordMapsToNoRuleAndDenies() throws Exception {
        DocumentSecurity incomplete = map(SPACE, CHILD.replace(
                "security_completeness: SECURITY_COMPLETENESS_COMPLETE",
                "security_completeness: SECURITY_COMPLETENESS_UNAVAILABLE"));

        assertThat(incomplete.getPermissionsList()).isEmpty();
        assertThat(reads(incomplete, "atlassian-account:acc-1", "atlassian-group:g-staff")).isFalse();
        assertThat(map(SPACE.replace("SECURITY_COMPLETENESS_COMPLETE", "SECURITY_COMPLETENESS_UNSPECIFIED"),
                CHILD).getPermissionsList()).isEmpty();
    }

    @Test
    void anOpenItemNeedsOnlyTheSpaceGrant() throws Exception {
        String open = CHILD.replace("CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED", "CONTENT_RESTRICTION_STATE_OPEN");
        DocumentSecurity security = map(SPACE, open);

        assertThat(security.getRequiredRuleSetsList()).isEmpty();
        assertThat(reads(security, "atlassian-account:acc-2", "atlassian-group:g-staff")).isTrue();
        assertThat(reads(security, "atlassian-account:acc-2")).isFalse();
    }

    /** Directory records as Sync emits them: two users (one hides its email) and their memberships. */
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
    void anEntraCallerJoinsByEmailToTheAtlassianAccountAndItsGroups() throws Exception {
        List<IdentityLink> mapped = new java.util.ArrayList<>();
        for (String record : DIRECTORY) {
            Map<String, DynamicMessage> values = Map.of("entity", parse(record));
            IdentityLink link = IdentityLink.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(links.getProduceType()), values, links.getRulesList(),
                    links.getCelRulesList(), registry, "confluence-identity").toByteString());
            if (link.hasFrom() && link.hasTo()) {
                mapped.add(link);
            }
        }
        assertThat(mapped).extracting(l -> l.getFrom().getIdentityType() + ":" + l.getFrom().getIdentity()
                + " -> " + l.getTo().getIdentityType() + ":" + l.getTo().getIdentity()).containsExactly(
                "user-principal-name:Ada@Example.com -> atlassian-account:acc-1",
                "atlassian-account:acc-1 -> atlassian-group:g-staff",
                "atlassian-account:acc-2 -> atlassian-group:g-staff");

        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        DocumentSecurity security = map(SPACE, CHILD);
        RepositoryCaller ada = expansion.expand(new RepositoryCaller("ada", false, Set.of(ACCOUNT),
                Set.of(Principal.newBuilder().setIdentityType("user-principal-name")
                        .setIdentity("ada@example.com").build())));
        assertThat(ada.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("atlassian-account:acc-1", "atlassian-group:g-staff");
        assertThat(DocumentAccessPolicy.allows(ada, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
        RepositoryCaller other = expansion.expand(new RepositoryCaller("other", false, Set.of(ACCOUNT),
                Set.of(Principal.newBuilder().setIdentityType("user-principal-name")
                        .setIdentity("someone@example.com").build())));
        assertThat(DocumentAccessPolicy.allows(other, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                .as("no Atlassian account has this email").isFalse();
    }

    private static DocumentSecurity map(String space, String item) throws Exception {
        Map<String, DynamicMessage> values = new LinkedHashMap<>();
        values.put("space", parse(space));
        values.put("item", parse(item));
        DynamicMessage mapped = EdgeMapping.map(registry.findDescriptorByFullName(edge.getProduceType()), values,
                edge.getRulesList(), edge.getCelRulesList(), registry, "confluence-acl");
        return DocumentSecurity.parseFrom(mapped.toByteString());
    }

    private static boolean reads(DocumentSecurity security, String... identities) {
        Set<Principal> principals = java.util.Arrays.stream(identities).map(identity -> {
            int colon = identity.indexOf(':');
            return Principal.newBuilder().setIdentityType(identity.substring(0, colon))
                    .setIdentity(identity.substring(colon + 1)).build();
        }).collect(Collectors.toSet());
        RepositoryCaller caller = new RepositoryCaller("reader", false, Set.of(ACCOUNT), principals);
        return DocumentAccessPolicy.allows(caller, ACCOUNT, security, List.of(), Access.ACCESS_READ);
    }

    private static DynamicMessage parse(String text) throws TextFormat.ParseException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(entity);
        TextFormat.merge(text, builder);
        return builder.build();
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = ConfluenceSecurityMappingTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("missing resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
