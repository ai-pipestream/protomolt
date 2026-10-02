package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;

/**
 * Schema compilation and linking behind the action API. Exactly one implementation must
 * be installed when schema resolution is used. Constructors must not acquire resources.
 * Implementations must be stateless and thread-safe; failures must propagate.
 */
public interface SchemaResolverProvider {
    SchemaResolver.ResolvedSchema resolve(ObjectNode input, String field, ActionContext context)
            throws ActionException;
    SchemaResolver.ResolvedSchema resolveSource(Message schema, String pointer, ActionContext context)
            throws ActionException;
}
