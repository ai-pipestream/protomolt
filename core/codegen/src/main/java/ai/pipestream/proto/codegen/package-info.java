/**
 * Code generation from descriptors, running protoc's own generators as WebAssembly.
 *
 * <p>{@link WasmProtoc} is the engine: a {@code CodeGeneratorRequest} goes in and a
 * {@code CodeGeneratorResponse} comes back, exactly the protoc plugin protocol, with no
 * native toolchain installed anywhere. The bundled module carries protoc's Java, Kotlin,
 * Python, C++, C#, Ruby, PHP, and Objective-C generators together with the grpc-java plugin,
 * and is compiled to JVM bytecode once per process on first use.</p>
 *
 * <p>{@link GenerateStubsAction} exposes it as the {@code generate-stubs} verb, an
 * {@link ai.pipestream.proto.actions.ProtoAction} that takes a resolved schema and returns
 * generated source files. A generator-reported failure is returned as a result carrying
 * protoc's message rather than thrown.</p>
 */
package ai.pipestream.proto.codegen;
