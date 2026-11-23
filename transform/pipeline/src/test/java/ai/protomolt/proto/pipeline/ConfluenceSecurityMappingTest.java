package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.repo.spi.CallerIdentityExpansion;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.DocumentSecurityTerms;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.IdentityLinks;
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
    private static TypedEdge classLinks;
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
        TypedEdge.Builder classBuilder = TypedEdge.newBuilder();
        TextFormat.merge(resource("acl/confluence-access-class-links.textproto"), classBuilder);
        classLinks = classBuilder.build();
        entity = registry.findDescriptorByFullName("ai.pipestream.confluence.v1.ConfluenceEntity");
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
                "security_completeness: ACL_COMPLETENESS_COMPLETE",
                "security_completeness: ACL_COMPLETENESS_UNAVAILABLE"));

        assertThat(incomplete.getPermissionsList()).isEmpty();
        assertThat(reads(incomplete, "atlassian-account:acc-1", "atlassian-group:g-staff")).isFalse();
        assertThat(map(SPACE.replace("ACL_COMPLETENESS_COMPLETE", "ACL_COMPLETENESS_UNSPECIFIED"),
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

        // Entra users: Ada's mail differs from her UPN; Bo has no mail, so his UPN is his address;
        // Cy signed up to Atlassian with an alias, listed as an other mail and a proxy address.
        List<IdentityLink> entra = new java.util.ArrayList<>();
        entra.addAll(entraLinks("user_principal_name: \"ada@contoso.onmicrosoft.com\" mail: \"ada@example.com\""
                + " proxy_addresses: \"SMTP:ada@example.com\" proxy_addresses: \"x500:/o=ExchangeLabs\""));
        entra.addAll(entraLinks("user_principal_name: \"someone@example.com\""));
        entra.addAll(entraLinks("user_principal_name: \"cy@contoso.com\" mail: \"cy@contoso.com\""
                + " other_mails: \"cy.signup@example.org\" proxy_addresses: \"smtp:Cy.Alias@Example.com\""));
        assertThat(entra).extracting(l -> l.getFrom().getIdentity() + " -> " + l.getTo().getIdentityType()
                + ":" + l.getTo().getIdentity()).containsExactly(
                "ada@contoso.onmicrosoft.com -> email:ada@example.com",
                "ada@contoso.onmicrosoft.com -> email:ada@example.com",
                "someone@example.com -> email:someone@example.com",
                "cy@contoso.com -> email:cy@contoso.com",
                "cy@contoso.com -> email:cy.signup@example.org",
                "cy@contoso.com -> email:Cy.Alias@Example.com");
        mapped.addAll(entra);
        mapped.add(IdentityLink.newBuilder()
                .setFrom(Principal.newBuilder().setIdentityType("email").setIdentity("cy.alias@example.com"))
                .setTo(Principal.newBuilder().setIdentityType("atlassian-account").setIdentity("acc-1")).build());

        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        DocumentSecurity security = map(SPACE, CHILD);
        RepositoryCaller ada = expansion.expand(caller("user-principal-name:ada@contoso.onmicrosoft.com"));
        assertThat(ada.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("email:ada@example.com", "atlassian-account:acc-1", "atlassian-group:g-staff");
        assertThat(DocumentAccessPolicy.allows(ada, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
        RepositoryCaller cy = expansion.expand(caller("user-principal-name:cy@contoso.com"));
        assertThat(cy.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .as("an alias joins too").contains("atlassian-account:acc-1");
        RepositoryCaller other = expansion.expand(caller("user-principal-name:someone@example.com"));
        assertThat(DocumentAccessPolicy.allows(other, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                .as("no Atlassian account has this email").isFalse();
    }

    private static List<IdentityLink> entraLinks(String graphUser) throws Exception {
        DynamicMessage.Builder user = DynamicMessage.newBuilder(
                registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser"));
        TextFormat.merge(graphUser, user);
        return IdentityLinks.parseFrom(EdgeMapping.map(registry.findDescriptorByFullName(entraLinks.getProduceType()),
                Map.of("user", user.build()), entraLinks.getRulesList(), entraLinks.getCelRulesList(), registry,
                "entra-identity").toByteString()).getLinksList();
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
                        .contains("atlassian-account", "atlassian-group"));
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
        RepositoryCaller caller = expansion.expand(caller("email:join-proof@example.invalid"));
        assertThat(caller.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("atlassian-account:account-1", "atlassian-group:" + group);
        assertThat(DocumentAccessPolicy.allows(caller, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
        // A licensed user the space lets in through ALL_LICENSED_USERS but who is on neither the
        // parent nor in its group is denied: the parent's restriction still gates it.
        RepositoryCaller outsider = expansion.expand(caller("atlassian-account:outsider",
                "atlassian-access-class:ALL_LICENSED_USERS"));
        assertThat(DocumentAccessPolicy.allows(outsider, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isFalse();
        // A member of the group on the parent whose only space grant is the access class.
        String other = records.stream().filter(r -> field(r, "entity_id").toString()
                        .startsWith("group:" + group + "/member/"))
                .map(r -> field(r, "entity_id").toString().substring(("group:" + group + "/member/").length()))
                .filter(id -> !id.equals("account-1")).findFirst().orElseThrow();
        RepositoryCaller member = expansion.expand(caller("atlassian-account:" + other));
        assertThat(DocumentAccessPolicy.allows(member, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                .as("no access class among its identities").isFalse();
        RepositoryCaller licensed = expansion.expand(caller("atlassian-account:" + other,
                "atlassian-access-class:ALL_LICENSED_USERS"));
        assertThat(DocumentAccessPolicy.allows(licensed, ACCOUNT, security, List.of(), Access.ACCESS_READ)).isTrue();
    }

    /**
     * End to end on recorded live data: the tenant's one Entra user, as grpc-microsoft's mapper
     * reads it from a live ListUsers response, signs in with its user principal name; its other
     * mail is the Confluence login, so it expands to the Confluence account and groups, and it may
     * read every page of the recorded live Sync, restricted ones included. Without the Entra links
     * the same caller reads nothing. {@code acl/entra-users.binpb} is grpc-microsoft's
     * {@code MicrosoftMapper.toUser} over its recording {@code list-users/001-GET-users.json}
     * (commit 494f76f, live run 45100103838), as delimited {@code GraphUser} messages.
     */
    @Test
    void theRealEntraUserReadsTheRealConfluencePagesThroughTheEmailJoin() throws Exception {
        Descriptor graphUser = registry.findDescriptorByFullName("ai.pipestream.microsoft.v1.GraphUser");
        List<IdentityLink> entra = new java.util.ArrayList<>();
        String upn = null;
        try (InputStream in = ConfluenceSecurityMappingTest.class.getClassLoader()
                .getResourceAsStream("acl/entra-users.binpb")) {
            DynamicMessage.Builder user = DynamicMessage.newBuilder(graphUser);
            while (user.mergeDelimitedFrom(in)) {
                DynamicMessage record = user.build();
                upn = field(record, "user_principal_name").toString();
                entra.addAll(IdentityLinks.parseFrom(EdgeMapping.map(
                        registry.findDescriptorByFullName(entraLinks.getProduceType()), Map.of("user", record),
                        entraLinks.getRulesList(), entraLinks.getCelRulesList(), registry, "entra-identity")
                        .toByteString()).getLinksList());
                user = DynamicMessage.newBuilder(graphUser);
            }
        }
        assertThat(upn).isNotBlank();
        List<DynamicMessage> records = recordedRecords();
        List<IdentityLink> confluence = new java.util.ArrayList<>();
        for (DynamicMessage record : records) {
            IdentityLink link = IdentityLink.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(links.getProduceType()), Map.of("entity", record),
                    links.getRulesList(), links.getCelRulesList(), registry, "confluence-identity").toByteString());
            if (link.hasFrom() && link.hasTo()) {
                confluence.add(link);
            }
        }
        List<IdentityLink> all = new java.util.ArrayList<>(entra);
        all.addAll(confluence);
        RepositoryCaller joined = new CallerIdentityExpansion(all).expand(caller("user-principal-name:" + upn));
        RepositoryCaller unjoined = new CallerIdentityExpansion(confluence).expand(caller("user-principal-name:" + upn));
        assertThat(joined.identities()).extracting(p -> p.getIdentityType() + ":" + p.getIdentity())
                .contains("email:join-proof@example.invalid", "atlassian-account:account-1");

        DynamicMessage space = records.stream().filter(r -> field(r, "entity_id").toString().startsWith("space:"))
                .findFirst().orElseThrow();
        int pages = 0;
        int gated = 0;
        for (DynamicMessage item : records) {
            if (!item.hasField(entity.findFieldByName("access_chain"))) {
                continue;
            }
            Map<String, DynamicMessage> values = new LinkedHashMap<>();
            values.put("space", space);
            values.put("item", item);
            DocumentSecurity security = DocumentSecurity.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(edge.getProduceType()), values, edge.getRulesList(),
                    edge.getCelRulesList(), registry, "confluence-acl").toByteString());
            String id = field(item, "entity_id").toString();
            pages++;
            gated += security.getRequiredRuleSetsCount() > 0 ? 1 : 0;
            assertThat(DocumentAccessPolicy.allows(joined, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                    .as("the Entra user on %s", id).isTrue();
            assertThat(DocumentAccessPolicy.allows(unjoined, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                    .as("the Entra user without the join on %s", id).isFalse();
        }
        assertThat(pages).isPositive();
        assertThat(gated).as("pages under a view-restricted parent").isPositive();
    }

    /**
     * Writes the OpenSearch fixture from the recorded live records: one NDJSON line per item
     * with its security terms, and for each caller its expanded terms and the policy's READ
     * decision on every item. The terms must answer what the policy answers.
     */
    @Test
    void writesTheSearchFixtureFromTheRecordedLiveRecords() throws Exception {
        List<DynamicMessage> records = new java.util.ArrayList<>(recordedRecords());
        records.addAll(records("acl/confluence-private-sync-records.binpb"));
        Map<String, DynamicMessage> spaces = new LinkedHashMap<>();
        records.stream().filter(r -> r.hasField(entity.findFieldByName("space_access")))
                .forEach(r -> spaces.put(field(r, "entity_id").toString(), r));
        List<IdentityLink> mapped = new java.util.ArrayList<>();
        for (DynamicMessage record : records) {
            IdentityLink link = IdentityLink.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(links.getProduceType()), Map.of("entity", record),
                    links.getRulesList(), links.getCelRulesList(), registry, "confluence-identity").toByteString());
            if (link.hasFrom() && link.hasTo()) {
                mapped.add(link);
            }
        }
        List<IdentityLink> unclassed = List.copyOf(mapped);
        for (DynamicMessage record : records) {
            mapped.addAll(IdentityLinks.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(classLinks.getProduceType()), Map.of("entity", record),
                    classLinks.getRulesList(), classLinks.getCelRulesList(), registry, "confluence-classes")
                    .toByteString()).getLinksList());
        }
        CallerIdentityExpansion expansion = new CallerIdentityExpansion(mapped);
        Map<String, RepositoryCaller> callers = new LinkedHashMap<>();
        callers.put("email:join-proof@example.invalid", expansion.expand(caller("email:join-proof@example.invalid")));
        records.stream().filter(r -> r.hasField(entity.findFieldByName("user")))
                .map(r -> "atlassian-account:" + field(r, "entity_id")).filter(id -> !id.endsWith(":account-1"))
                .forEach(id -> callers.put(id, expansion.expand(caller(id))));
        String member = callers.keySet().stream().filter(id -> !id.startsWith("email:")).reduce((a, b) -> b)
                .orElseThrow();
        callers.put(member + " without access classes", new CallerIdentityExpansion(unclassed).expand(caller(member)));
        callers.put("atlassian-account:outsider licensed", expansion.expand(caller("atlassian-account:outsider",
                "atlassian-access-class:ALL_LICENSED_USERS")));

        StringBuilder items = new StringBuilder();
        Map<String, Map<String, Boolean>> decisions = new LinkedHashMap<>();
        callers.keySet().forEach(name -> decisions.put(name, new LinkedHashMap<>()));
        int restricted = 0;
        for (DynamicMessage item : records) {
            if (!item.hasField(entity.findFieldByName("access_chain"))) {
                continue;
            }
            Map<String, DynamicMessage> values = new LinkedHashMap<>();
            Message chain = (Message) field(item, "access_chain");
            values.put("space", spaces.get("space:" + chain.getField(
                    chain.getDescriptorForType().findFieldByName("space_id")) + "/access"));
            values.put("item", item);
            DocumentSecurity security = DocumentSecurity.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(edge.getProduceType()), values, edge.getRulesList(),
                    edge.getCelRulesList(), registry, "confluence-acl").toByteString());
            DocumentSecurityTerms terms = DocumentSecurityTerms.of(security, List.of());
            restricted += terms.gates().isEmpty() ? 0 : 1;
            String id = field(item, "entity_id").toString();
            items.append("{\"_id\":").append(json(id))
                    .append(",\"acl_allow\":").append(json(terms.allow()))
                    .append(",\"acl_deny\":").append(json(terms.deny()))
                    .append(",\"acl_gates\":[");
            for (int i = 0; i < terms.gates().size(); i++) {
                DocumentSecurityTerms.Gate gate = terms.gates().get(i);
                items.append(i == 0 ? "" : ",").append("{\"source\":").append(json(gate.source()))
                        .append(",\"allow\":").append(json(gate.allow())).append('}');
            }
            items.append("],\"acl_complete\":true}\n");
            for (var entry : callers.entrySet()) {
                boolean allowed = DocumentAccessPolicy.allows(entry.getValue(), ACCOUNT, security, List.of(),
                        Access.ACCESS_READ);
                assertThat(terms.reads(DocumentSecurityTerms.callerTerms(entry.getValue().identities())))
                        .as("%s on %s", entry.getKey(), id).isEqualTo(allowed);
                decisions.get(entry.getKey()).put(id, allowed);
            }
        }
        assertThat(restricted).as("items under a view-restricted owner").isPositive();
        assertThat(decisions.get("email:join-proof@example.invalid")).doesNotContainValue(false);
        assertThat(callers.keySet().stream().filter(name -> name.startsWith("atlassian-account:account-"))
                .filter(name -> !name.contains(" ")).map(decisions::get))
                .as("a real user denied a page of the private space").anySatisfy(d ->
                        assertThat(d).containsValue(true).containsValue(false));
        assertThat(decisions.get("atlassian-account:outsider licensed")).containsValue(true).containsValue(false);
        assertThat(decisions.get(member + " without access classes")).doesNotContainValue(true);

        StringBuilder callerJson = new StringBuilder("[");
        int n = 0;
        for (var entry : callers.entrySet()) {
            callerJson.append(n++ == 0 ? "\n" : ",\n").append("{\"caller\":").append(json(entry.getKey()))
                    .append(",\"terms\":").append(json(List.copyOf(
                            DocumentSecurityTerms.callerTerms(entry.getValue().identities()))))
                    .append(",\"reads\":{");
            int m = 0;
            for (var decision : decisions.get(entry.getKey()).entrySet()) {
                callerJson.append(m++ == 0 ? "" : ",").append(json(decision.getKey())).append(':')
                        .append(decision.getValue());
            }
            callerJson.append("}}");
        }
        callerJson.append("\n]\n");
        java.nio.file.Path out = java.nio.file.Path.of("build", "acl-opensearch");
        java.nio.file.Files.createDirectories(out);
        java.nio.file.Files.writeString(out.resolve("confluence-items.ndjson"), items);
        java.nio.file.Files.writeString(out.resolve("confluence-callers.json"), callerJson);
    }

    /**
     * Confluence's own answers decide: in the live run every user granted on the restricted
     * parent was asked about with CheckContentPermission ({@code acl/confluence-permission-checks.binpb},
     * request and response pairs from the same run as the Sync records). protomolt's decision
     * on the same records, with each user's access classes from its operations, equals every answer.
     * Without the access-class links a user whose only space grant is ALL_LICENSED_USERS is denied
     * what Confluence lets it read. The run's private space ({@code acl/confluence-private-sync-records.binpb})
     * has a restricted page granted to a user who cannot view that space: Confluence denies it, and so
     * does protomolt.
     */
    @Test
    void protomoltDecidesWhatConfluenceAnsweredLive() throws Exception {
        List<DynamicMessage> records = new java.util.ArrayList<>(recordedRecords());
        records.addAll(records("acl/confluence-private-sync-records.binpb"));
        List<IdentityLink> withClasses = new java.util.ArrayList<>(recordedLinks(records));
        List<IdentityLink> withoutClasses = List.copyOf(withClasses);
        for (DynamicMessage record : records) {
            withClasses.addAll(IdentityLinks.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(classLinks.getProduceType()), Map.of("entity", record),
                    classLinks.getRulesList(), classLinks.getCelRulesList(), registry, "confluence-classes")
                    .toByteString()).getLinksList());
        }
        assertThat(withClasses).extracting(l -> l.getTo().getIdentity()).contains("ALL_LICENSED_USERS");
        Map<String, DynamicMessage> spaces = new LinkedHashMap<>();
        records.stream().filter(r -> r.hasField(entity.findFieldByName("space_access")))
                .forEach(r -> spaces.put(field(r, "entity_id").toString(), r));
        assertThat(spaces).hasSize(2);
        Map<String, DynamicMessage> items = new LinkedHashMap<>();
        records.stream().filter(r -> r.hasField(entity.findFieldByName("access_chain")))
                .forEach(r -> items.put(field(r, "entity_id").toString(), r));

        Descriptor request = registry.findDescriptorByFullName(
                "ai.pipestream.confluence.v1.CheckContentPermissionRequest");
        Descriptor response = registry.findDescriptorByFullName(
                "ai.pipestream.confluence.v1.CheckContentPermissionResponse");
        int compared = 0;
        int classOnly = 0;
        int grantedButDenied = 0;
        try (InputStream in = ConfluenceSecurityMappingTest.class.getClassLoader()
                .getResourceAsStream("acl/confluence-permission-checks.binpb")) {
            while (true) {
                DynamicMessage.Builder asked = DynamicMessage.newBuilder(request);
                if (!asked.mergeDelimitedFrom(in)) break;
                DynamicMessage.Builder answered = DynamicMessage.newBuilder(response);
                assertThat(answered.mergeDelimitedFrom(in)).isTrue();
                DynamicMessage check = asked.build();
                boolean confluence = (Boolean) field(answered.build(), "has_permission");
                String content = field(check, "content_id").toString();
                Message subject = (Message) field(check, "subject");
                String account = subject.getField(subject.getDescriptorForType().findFieldByName("principal_id"))
                        .toString();
                DynamicMessage item = items.get("page:" + content + "/access");
                assertThat(item).as("the access chain of page %s", content).isNotNull();
                Map<String, DynamicMessage> values = new LinkedHashMap<>();
                Message chain = (Message) field(item, "access_chain");
                String spaceId = chain.getField(chain.getDescriptorForType().findFieldByName("space_id")).toString();
                values.put("space", spaces.get("space:" + spaceId + "/access"));
                values.put("item", item);
                boolean onPage = ((List<?>) chain.getField(chain.getDescriptorForType().findFieldByName("owners")))
                        .stream().map(Message.class::cast).anyMatch(owner -> ((List<?>) owner.getField(
                                owner.getDescriptorForType().findFieldByName("permissions"))).stream()
                                .map(Message.class::cast).anyMatch(permission -> {
                                    Message principal = (Message) permission.getField(
                                            permission.getDescriptorForType().findFieldByName("principal"));
                                    return principal.getField(principal.getDescriptorForType()
                                            .findFieldByName("principal_id")).toString().equals(account);
                                }));
                grantedButDenied += onPage && !confluence ? 1 : 0;
                DocumentSecurity security = DocumentSecurity.parseFrom(EdgeMapping.map(
                        registry.findDescriptorByFullName(edge.getProduceType()), values, edge.getRulesList(),
                        edge.getCelRulesList(), registry, "confluence-acl").toByteString());
                RepositoryCaller caller = new CallerIdentityExpansion(withClasses)
                        .expand(caller("atlassian-account:" + account));
                assertThat(DocumentAccessPolicy.allows(caller, ACCOUNT, security, List.of(), Access.ACCESS_READ))
                        .as("%s on page %s, as Confluence answered", account, content).isEqualTo(confluence);
                boolean unclassed = DocumentAccessPolicy.allows(new CallerIdentityExpansion(withoutClasses)
                        .expand(caller("atlassian-account:" + account)), ACCOUNT, security, List.of(),
                        Access.ACCESS_READ);
                classOnly += confluence && !unclassed ? 1 : 0;
                compared++;
            }
        }
        assertThat(compared).isGreaterThanOrEqualTo(10);
        assertThat(classOnly).as("answers that need the licensed-users class").isPositive();
        assertThat(grantedButDenied).as("users on a page of a space they cannot view, denied by Confluence")
                .isPositive();
    }

    private static List<IdentityLink> recordedLinks(List<DynamicMessage> records) throws Exception {
        List<IdentityLink> mapped = new java.util.ArrayList<>();
        for (DynamicMessage record : records) {
            IdentityLink link = IdentityLink.parseFrom(EdgeMapping.map(
                    registry.findDescriptorByFullName(links.getProduceType()), Map.of("entity", record),
                    links.getRulesList(), links.getCelRulesList(), registry, "confluence-identity").toByteString());
            if (link.hasFrom() && link.hasTo()) {
                mapped.add(link);
            }
        }
        return mapped;
    }

    private static List<DynamicMessage> recordedRecords() throws IOException {
        return records("acl/confluence-sync-records.binpb");
    }

    private static List<DynamicMessage> records(String resource) throws IOException {
        List<DynamicMessage> records = new java.util.ArrayList<>();
        try (InputStream in = ConfluenceSecurityMappingTest.class.getClassLoader()
                .getResourceAsStream(resource)) {
            DynamicMessage.Builder record = DynamicMessage.newBuilder(entity);
            while (record.mergeDelimitedFrom(in)) {
                records.add(record.build());
                record = DynamicMessage.newBuilder(entity);
            }
        }
        return records;
    }

    private static String json(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.append('"').toString();
    }

    private static String json(List<String> values) {
        return values.stream().map(ConfluenceSecurityMappingTest::json)
                .collect(Collectors.joining(",", "[", "]"));
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
