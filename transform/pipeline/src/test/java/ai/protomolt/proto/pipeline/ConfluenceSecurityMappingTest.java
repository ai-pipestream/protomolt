package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
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
    private static TypedEdge entraLinks;
    private static Descriptor entity;

    @BeforeAll
    static void load() throws Exception {
        registry = DescriptorRegistry.create(false);
        new GoogleDescriptorLoader("acl/confluence-v1.binpb").loadDescriptors().forEach(registry::registerFile);
        new GoogleDescriptorLoader("acl/microsoft-v1.binpb").loadDescriptors().forEach(registry::registerFile);
        registry.registerFile(DocumentSecurity.getDescriptor().getFile());
        registry.registerFile(IdentityLink.getDescriptor().getFile());
        TypedEdge.Builder builder = TypedEdge.newBuilder();
        TextFormat.merge(resource("acl/confluence-document-security.textproto"), builder);
        edge = builder.build();
        TypedEdge.Builder linkBuilder = TypedEdge.newBuilder();
        TextFormat.merge(resource("acl/confluence-identity-links.textproto"), linkBuilder);
        links = linkBuilder.build();
        TypedEdge.Builder entraBuilder = TypedEdge.newBuilder();
        TextFormat.merge(resource("acl/entra-identity-links.textproto"), entraBuilder);
        entraLinks = entraBuilder.build();
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
                "email:Ada@Example.com -> atlassian-account:acc-1",
                "atlassian-account:acc-1 -> atlassian-group:g-staff",
                "atlassian-account:acc-2 -> atlassian-group:g-staff");

        // Entra users: Ada's mail differs from her UPN; Bo has no mail, so his UPN is his address.
        mapped.add(entraLink("user_principal_name: \"ada@contoso.onmicrosoft.com\" mail: \"ada@example.com\""));
        mapped.add(entraLink("user_principal_name: \"someone@example.com\""));
        assertThat(mapped.subList(3, 5)).extracting(l -> l.getFrom().getIdentity() + " -> " + l.getTo().getIdentityType()
                + ":" + l.getTo().getIdentity()).containsExactly(
                "ada@contoso.onmicrosoft.com -> email:ada@example.com",
                "someone@example.com -> email:someone@example.com");

        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        DocumentSecurity security = map(SPACE, CHILD);
        RepositoryCaller ada = expansion.expand(caller("user-principal-name:ada@contoso.onmicrosoft.com"));
        assertThat(ada.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("email:ada@example.com", "atlassian-account:acc-1", "atlassian-group:g-staff");
        assertThat(DocumentAccessPolicy.allows(ada, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
        RepositoryCaller other = expansion.expand(caller("user-principal-name:someone@example.com"));
        assertThat(DocumentAccessPolicy.allows(other, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                .as("no Atlassian account has this email").isFalse();
    }

    private static IdentityLink entraLink(String graphUser) throws Exception {
        DynamicMessage.Builder user = DynamicMessage.newBuilder(
                registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser"));
        TextFormat.merge(graphUser, user);
        return IdentityLink.parseFrom(EdgeMapping.map(registry.findDescriptorByFullName(entraLinks.getProduceType()),
                Map.of("user", user.build()), entraLinks.getRulesList(), entraLinks.getCelRulesList(), registry,
                "entra-identity").toByteString());
    }

    /**
     * The records grpc-confluence Sync emitted in a live run on the test site
     * (a child page under a parent view-restricted to the caller, with one of
     * the caller's groups granted READ on the parent), as its recording replays
     * them, scrubbed.
     */
    @Test
    void theRecordedLiveSyncRecordsMapToTheSiteSecurity() throws Exception {
        List<DynamicMessage> records = new java.util.ArrayList<>();
        try (InputStream in = ConfluenceSecurityMappingTest.class.getClassLoader()
                .getResourceAsStream("acl/confluence-sync-records.binpb")) {
            DynamicMessage.Builder record = DynamicMessage.newBuilder(entity);
            while (record.mergeDelimitedFrom(in)) {
                records.add(record.build());
                record = DynamicMessage.newBuilder(entity);
            }
        }
        DynamicMessage space = records.stream().filter(r -> field(r, "entity_id").toString().startsWith("space:"))
                .findFirst().orElseThrow();
        DynamicMessage child = records.stream().filter(ConfluenceSecurityMappingTest::isRestrictedChild)
                .findFirst().orElseThrow();

        Map<String, DynamicMessage> values = new LinkedHashMap<>();
        values.put("space", space);
        values.put("item", child);
        DocumentSecurity security = DocumentSecurity.parseFrom(EdgeMapping.map(
                registry.findDescriptorByFullName(edge.getProduceType()), values, edge.getRulesList(),
                edge.getCelRulesList(), registry, "confluence-acl").toByteString());
        assertThat(security.getPermissionsList()).extracting(r -> r.getIdentityType() + ":" + r.getIdentity())
                .contains("atlassian-account:account-1", "atlassian-access-class:ALL_LICENSED_USERS");
        assertThat(security.getRequiredRuleSetsList()).singleElement().satisfies(set ->
                assertThat(set.getRulesList()).extracting(AccessRule::getIdentityType)
                        .containsExactlyInAnyOrder("atlassian-account", "atlassian-group"));
        String group = security.getRequiredRuleSets(0).getRulesList().stream()
                .filter(r -> r.getIdentityType().equals("atlassian-group")).findFirst().orElseThrow().getIdentity();

        List<IdentityLink> mapped = new java.util.ArrayList<>();
        for (DynamicMessage record : records) {
            IdentityLink link = IdentityLink.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(links.getProduceType()), Map.of("entity", record),
                    links.getRulesList(), links.getCelRulesList(), registry, "confluence-identity").toByteString());
            if (link.hasFrom() && link.hasTo()) {
                mapped.add(link);
            }
        }
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        // The caller's email is the one the site showed; it joins to the account and the group.
        RepositoryCaller caller = expansion.expand(caller("email:user-1@example.invalid"));
        assertThat(caller.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("atlassian-account:account-1", "atlassian-group:" + group);
        assertThat(DocumentAccessPolicy.allows(caller, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
        // account-2 may view the space but is not on the restricted parent.
        assertThat(reads(security, "atlassian-account:account-2")).isFalse();
        // account-4 is in the group on the parent, and views the space only as a licensed user.
        RepositoryCaller member = expansion.expand(caller("atlassian-account:account-4"));
        assertThat(DocumentAccessPolicy.allows(member, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                .as("no access class among its identities").isFalse();
        RepositoryCaller licensed = expansion.expand(caller("atlassian-account:account-4",
                "atlassian-access-class:ALL_LICENSED_USERS"));
        assertThat(DocumentAccessPolicy.allows(licensed, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
    }

    /** An access chain whose second owner is view-restricted with a group on it. */
    private static boolean isRestrictedChild(DynamicMessage record) {
        if (!record.hasField(entity.findFieldByName("access_chain"))) {
            return false;
        }
        Message chain = (Message) field(record, "access_chain");
        List<?> owners = (List<?>) chain.getField(chain.getDescriptorForType().findFieldByName("owners"));
        if (owners.size() < 2) {
            return false;
        }
        Message parent = (Message) owners.get(1);
        Message state = (Message) parent.getField(parent.getDescriptorForType().findFieldByName("restriction_state"));
        boolean viewRestricted = state.getField(state.getDescriptorForType().findFieldByName("state")).toString()
                .equals("CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED");
        boolean group = ((List<?>) parent.getField(parent.getDescriptorForType().findFieldByName("permissions")))
                .stream().map(Message.class::cast).anyMatch(permission -> {
                    Message principal = (Message) permission.getField(
                            permission.getDescriptorForType().findFieldByName("principal"));
                    return principal.getField(principal.getDescriptorForType().findFieldByName("principal_type"))
                            .toString().equals("CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP");
                });
        return viewRestricted && group;
    }

    private static Object field(DynamicMessage message, String name) {
        return message.getField(message.getDescriptorForType().findFieldByName(name));
    }

    private static RepositoryCaller caller(String... identities) {
        Set<Principal> principals = java.util.Arrays.stream(identities).map(identity -> {
            int colon = identity.indexOf(':');
            return Principal.newBuilder().setIdentityType(identity.substring(0, colon))
                    .setIdentity(identity.substring(colon + 1)).build();
        }).collect(Collectors.toSet());
        return new RepositoryCaller("reader", false, Set.of(ACCOUNT), principals);
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
