/**
 * Dynamic gRPC invocation driven entirely by descriptors, with no generated stubs.
 *
 * <p>{@link DynamicGrpcCalls} builds the wire method descriptor from a protobuf
 * {@link com.google.protobuf.Descriptors.MethodDescriptor} with
 * {@link com.google.protobuf.DynamicMessage} marshallers, so any method described by a
 * descriptor set is callable: unary, client-streaming, and server-streaming. For an
 * open-ended server stream, {@link DynamicGrpcStream} gives a flow-controlled handle whose
 * poll-shaped {@code take} contract suits a worker loop or a connector source task, requesting
 * messages from the server only as the consumer drains them.
 *
 * <p>{@link ReflectionClient} covers the other direction: it drives the server-reflection bidi
 * stream to list a server's services and walk the descriptor graph into a single
 * {@link com.google.protobuf.DescriptorProtos.FileDescriptorSet}, so a service can be operated
 * given nothing but its address. Reflection faults are reported as
 * {@link ReflectionException}. Channel construction is the extension point: a
 * {@link ChannelFactory} decides how the connection is opened, the default honoring the verbs'
 * TLS input.
 *
 * <p>{@link GrpcInvokeAction} and {@link ReflectAction} expose this machinery as the
 * {@code grpc-invoke} and {@code reflect} verbs of
 * {@link ai.pipestream.proto.actions.ActionCatalog}; the classes underneath are usable on their
 * own as a library. See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/actions.md">actions
 * guide</a>.
 */
package ai.pipestream.proto.grpc.invoke;
