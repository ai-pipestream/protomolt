package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Base64;
import java.util.function.Function;

/**
 * {@code reflect}: ask a live gRPC server for its own schema over the server-reflection protocol,
 * so an agent can operate a service given nothing but its address. Returns the advertised service
 * names and a base64 descriptor set that feeds straight into {@code list-types}, {@code grpc-invoke},
 * and {@code generate-stubs}.
 *
 * <p>This is the verb that removes the last precondition from "any gRPC service is an MCP
 * integration": no schema needs to be registered or pasted first. Servers that do not enable
 * reflection return {@code ok: false} with the reason, rather than an error.</p>
 */
public final class ReflectAction implements ProtoAction {

    private static final int DEFAULT_DEADLINE_MS = 15_000;

    private final ChannelFactory channelFactory;

    public ReflectAction() {
        this(ChannelFactory.standard());
    }

    /** Visible for tests and custom transports: maps a target string to a channel. */
    public ReflectAction(Function<String, ManagedChannel> channelFactory) {
        this((target, tls) -> channelFactory.apply(target));
    }

    /** Full transport control: the factory sees the verb's {@code tls} input. */
    public ReflectAction(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public String name() {
        return "reflect";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Fetches a live gRPC server's own schema over the server-reflection protocol, "
                + "given only its address. Returns the service names and a base64 descriptor set "
                + "usable directly as the 'schema' input to list-types, grpc-invoke, and "
                + "generate-stubs. Servers without reflection enabled return ok:false.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("ReflectRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("ReflectResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // target is required by the message and the deadline is bounded there, so a blank
        // target or a non-positive deadline is refused before the verb runs. Zero means the
        // caller said nothing, which the message documents as the default.
        String target = Fields.string(input, "target");
        int asked = Fields.integer(input, "deadlineMs");
        long deadlineMs = asked == 0 ? DEFAULT_DEADLINE_MS : asked;

        Reply result = Reply.of(responseType());
        boolean tls = Fields.flag(input, "tls");
        try {
            channelFactory.validateTarget(target, tls);
            channelFactory.validateDeadline(deadlineMs);
        } catch (IllegalArgumentException e) {
            throw invalidInput(e.getMessage(), e.getMessage().contains("deadline")
                    ? "/deadlineMs" : "/target");
        }
        ManagedChannel channel;
        try {
            channel = channelFactory.open(target, tls);
        } catch (IllegalArgumentException e) {
            throw invalidInput(e.getMessage(), "/target");
        }
        try {
            ReflectionClient.Result discovered = channelFactory.policy() == null
                    ? ReflectionClient.discover(channel, deadlineMs)
                    : ReflectionClient.discover(channel, deadlineMs, channelFactory.policy());
            result.set("ok", true)
                    .addAll("services", discovered.services())
                    .set("descriptorSetBase64", Base64.getEncoder()
                            .encodeToString(discovered.descriptorSet().toByteArray()))
                    .set("fileCount", discovered.descriptorSet().getFileCount());
        } catch (ReflectionException e) {
            result.set("ok", false).set("error", e.getMessage());
        } finally {
            channel.shutdownNow();
        }
        return result.build();
    }

    private static ActionException invalidInput(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message + " (at '" + pointer + "')", details);
    }
}
