package ai.pipestream.proto.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Pins the catalog's authorization behavior: scoped refusal before dispatch, filtered listing. */
class ActionCatalogScopeTest {

    private static final Caller READER = Caller.scoped("reader", Set.of(Scopes.SCHEMA_READ));
    private static final Caller INVOKER =
            Caller.scoped("invoker", Set.of(Scopes.SERVICE_INVOKE));

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private static ProtoAction stub(String name, String scope, AtomicBoolean ran) {
        return new ProtoAction() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String description() {
                return "test stub";
            }

            @Override
            public String requiredScope() {
                return scope;
            }

            @Override
            public ObjectNode inputSchema() {
                return JsonNodeFactory.instance.objectNode();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                ran.set(true);
                return JsonNodeFactory.instance.objectNode().put("ok", true);
            }
        };
    }

    @Test
    void aMissingScopeRefusesByNameBeforeTheActionRuns() {
        AtomicBoolean ran = new AtomicBoolean();
        catalog.register(stub("guarded", Scopes.SCHEMA_WRITE, ran));

        ActionException denied = catchThrowableOfType(ActionException.class, () ->
                catalog.execute("guarded", JsonNodeFactory.instance.objectNode(), READER));

        assertThat(denied.code()).isEqualTo("permission-denied");
        assertThat(denied.getMessage())
                .contains("reader").contains(Scopes.SCHEMA_WRITE).contains("guarded");
        assertThat(ran).isFalse();
        assertThat(denied.details().orElseThrow().path("requiredScope").asText())
                .isEqualTo(Scopes.SCHEMA_WRITE);
    }

    @Test
    void aHeldScopeDispatches() throws Exception {
        AtomicBoolean ran = new AtomicBoolean();
        catalog.register(stub("guarded", Scopes.SCHEMA_WRITE, ran));
        Caller writer = Caller.scoped("writer", Set.of(Scopes.SCHEMA_WRITE));

        ObjectNode result =
                catalog.execute("guarded", JsonNodeFactory.instance.objectNode(), writer);

        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(ran).isTrue();
    }

    @Test
    void theRefusalPrecedesEnvelopeValidation() {
        ActionException denied = catchThrowableOfType(ActionException.class, () ->
                catalog.execute("compile", null, INVOKER));
        assertThat(denied.code()).isEqualTo("permission-denied");

        ActionException dispatched = catchThrowableOfType(ActionException.class, () ->
                catalog.execute("compile", null, READER));
        assertThat(dispatched.code()).isEqualTo("invalid-input");
    }

    @Test
    void anActionWithoutADeclaredScopeRefusesEveryScopedCaller() {
        AtomicBoolean ran = new AtomicBoolean();
        catalog.register(stub("undeclared", "", ran));
        Caller everything = Caller.scoped("everything", Scopes.VOCABULARY);

        ActionException denied = catchThrowableOfType(ActionException.class, () ->
                catalog.execute("undeclared", JsonNodeFactory.instance.objectNode(), everything));

        assertThat(denied.code()).isEqualTo("permission-denied");
        assertThat(denied.getMessage()).contains("declares no required scope");
        assertThat(ran).isFalse();
    }

    @Test
    void theOperatorPathIsUnchanged() throws Exception {
        AtomicBoolean ran = new AtomicBoolean();
        catalog.register(stub("undeclared", "", ran));

        catalog.execute("undeclared", JsonNodeFactory.instance.objectNode());

        assertThat(ran).isTrue();
    }

    @Test
    void listingFiltersToTheCallersScopesAndHidesUndeclaredActions() {
        catalog.register(stub("undeclared", "", new AtomicBoolean()));
        Caller everything = Caller.scoped("everything", Scopes.VOCABULARY);

        assertThat(catalog.list(READER)).hasSize(17);
        assertThat(catalog.list(INVOKER)).isEmpty();
        assertThat(catalog.list(everything)).hasSize(17);
        assertThat(catalog.list()).hasSize(18);
        assertThat(catalog.list(Caller.operator())).hasSize(18);
    }

    @Test
    void streamingDispatchRefusesTheSameWay() {
        List<ObjectNode> emitted = new ArrayList<>();
        ActionException denied = catchThrowableOfType(ActionException.class, () ->
                catalog.executeStreaming("list-types",
                        JsonNodeFactory.instance.objectNode(), INVOKER, emitted::add));
        assertThat(denied.code()).isEqualTo("permission-denied");
        assertThat(emitted).isEmpty();
    }
}
